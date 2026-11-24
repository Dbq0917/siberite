package queue

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	// ErrIsEmpty is returned when queue is empty
	ErrIsEmpty = errors.New("queue: is empty")

	// ErrIDOutOfBounds is returned when queue is is out of bounds
	ErrIDOutOfBounds = errors.New("queue: ID is out of bounds")

	// ErrInvalidName is returned when queue name is not valid
	ErrInvalidName = errors.New("queue: has invalid characters")

	// ErrNameTooLong means that queue name is longer then allowed limit
	ErrNameTooLong = errors.New("queue: name is too long")

	// ErrInvalidHeadValue is returned when there is an attempt
	// to assign invalid queue head value
	ErrInvalidHeadValue = errors.New("queue: head can not be less then zero")

	// ErrSharedFlush means that there was an attempt to flush shared queue
	ErrSharedFlush = errors.New("queue: can't flush shared queue")

	// ErrRetentionLimit means unread data reached the configured retention limit
	ErrRetentionLimit = errors.New("queue: retention limit reached")

	// ErrQueueClosed is returned when queue is not open
	ErrQueueClosed = errors.New("queue: is closed")
)

const levelDBOpenFilesCacheCapacity = 64

var validQueueNameRegex = regexp.MustCompile(`[^a-zA-Z0-9_\-\:\#\$\%\,\\\~\@\*\&\!]+`)

// Consumer represents a queue consumer
type Consumer interface {
	GetNext() ([]byte, error)
	PutBack([]byte) error
	Peek() ([]byte, error)
	Flush() error
	Length() uint64
	IsEmpty() bool
	Stats() *Stats
}

// make sure Queue implements Consumer interface
var _ Consumer = (*Queue)(nil)

// Queue represents a persistent FIFO structure
// that stores the data in leveldb
type Queue struct {
	sync.RWMutex
	Name                string
	DataDir             string
	stats               *Stats
	db                  *leveldb.DB
	opts                *Options
	head                uint64
	tail                uint64
	segments            []segment
	currentBytes        int64
	segmentSize         int64
	maxRetainedSegments int
	readers             map[*Reader]struct{}
	isOpened            bool
	isShared            bool
}

// Options represents queue options
type Options struct {
	KeyPrefix           []byte
	SegmentSize         int64 // zero uses the default
	MaxRetainedSegments int   // zero uses the default
}

// Reader pins a read position while queue segments are being reclaimed.
type Reader struct {
	q        *Queue
	position uint64
	closed   uint32
}

// Item represents a queue item
type Item struct {
	ID    uint64
	Key   []byte
	Value []byte
}

// Open creates a queue and opens underlying leveldb database
func Open(name string, dataDir string, opts *Options) (*Queue, error) {
	q := &Queue{
		Name:     name,
		DataDir:  dataDir,
		stats:    &Stats{0},
		db:       &leveldb.DB{},
		opts:     opts,
		head:     0,
		tail:     0,
		readers:  make(map[*Reader]struct{}),
		isOpened: false,
		isShared: false,
	}
	return q, q.open()
}

// OpenShared creates and initializes a queue from opened leveldb database
func OpenShared(name string, keyPrefix string, db *leveldb.DB) (*Queue, error) {

	q := &Queue{
		Name:     name,
		DataDir:  "",
		stats:    &Stats{0},
		db:       db,
		opts:     &Options{KeyPrefix: []byte(keyPrefix)},
		head:     0,
		tail:     0,
		readers:  make(map[*Reader]struct{}),
		isOpened: false,
		isShared: true,
	}
	return q, q.open()
}

// Close leveldb database
func (q *Queue) Close() {
	if q.isOpened && !q.isShared {
		q.db.Close()
	}
	q.isOpened = false
}

// Drop closes and deletes leveldb database
func (q *Queue) Drop() {
	if q.isShared {
		return
	}
	q.Close()
	os.RemoveAll(q.Path())
}

// Flush flushes all queue data
func (q *Queue) Flush() error {
	if q.isShared {
		return ErrSharedFlush
	}
	q.Lock()
	defer q.Unlock()
	q.Drop()
	return q.open()
}

// Head returns current head offset of the queue
func (q *Queue) Head() uint64 {
	q.RLock()
	defer q.RUnlock()
	return q.head
}

// Tail returns current tail offset of the queue
func (q *Queue) Tail() uint64 {
	q.RLock()
	defer q.RUnlock()
	return q.tail
}

// Length returns current length of the queue
func (q *Queue) Length() uint64 {
	q.RLock()
	defer q.RUnlock()
	return q.length()
}

// NewReader registers a position that segment reclamation must not pass.
func (q *Queue) NewReader(position uint64) (*Reader, error) {
	q.Lock()
	defer q.Unlock()

	if !q.isOpened {
		return nil, ErrQueueClosed
	}
	oldest := q.oldestRetainedID()
	if position < oldest {
		if oldest == 0 {
			position = 0
		} else {
			position = oldest - 1
		}
	}
	if position > q.tail {
		position = q.tail
	}

	reader := &Reader{q: q, position: position}
	q.readers[reader] = struct{}{}
	return reader, nil
}

// Position returns the last read item ID.
func (r *Reader) Position() uint64 {
	return atomic.LoadUint64(&r.position)
}

// SetPosition updates the last read item ID without releasing the reader.
func (r *Reader) SetPosition(position uint64) {
	atomic.StoreUint64(&r.position, position)
}

// Close releases the reader and lets reclamation continue.
func (r *Reader) Close() error {
	return r.release(true)
}

// Detach releases the reader without reclaiming its segments.
// It is used when the process is shutting down and the durable cursor
// will register again when the queue is reopened.
func (r *Reader) Detach() {
	_ = r.release(false)
}

func (r *Reader) release(reclaim bool) error {
	if !atomic.CompareAndSwapUint32(&r.closed, 0, 1) {
		return nil
	}
	r.q.Lock()
	defer r.q.Unlock()
	delete(r.q.readers, r)
	if !reclaim {
		return nil
	}
	return r.q.reclaimLocked()
}

// IsEmpty returns false if queue is empty
func (q *Queue) IsEmpty() bool {
	return q.Length() < 1
}

// Enqueue adds new value to the queue
func (q *Queue) Enqueue(value []byte) error {
	q.Lock()
	defer q.Unlock()

	if !q.isOpened {
		return ErrQueueClosed
	}
	if err := q.reclaimLocked(); err != nil {
		return err
	}

	nextID := q.tail + 1
	segments := q.segments
	currentBytes := q.currentBytes
	newSegment := false

	if len(segments) == 0 {
		segments = []segment{{start: nextID}}
		currentBytes = 0
		newSegment = true
	} else if q.segmentSize > 0 &&
		currentBytes > 0 &&
		currentBytes+int64(len(value)) > q.segmentSize {
		segments = append(append([]segment{}, segments...), segment{start: nextID})
		currentBytes = 0
		newSegment = true
	}

	if q.maxRetainedSegments > 0 && len(segments) > q.maxRetainedSegments {
		return ErrRetentionLimit
	}

	batch := new(leveldb.Batch)
	batch.Put(q.dbKey(nextID), value)
	q.putState(batch, queueState{head: q.head, tail: nextID})
	if newSegment {
		q.putSegments(batch, segments)
	}
	if err := q.db.Write(batch, nil); err != nil {
		return err
	}

	q.tail = nextID
	q.segments = segments
	q.currentBytes = currentBytes + int64(len(value))
	return nil
}

// GetNext returns next value from queue
func (q *Queue) GetNext() ([]byte, error) {
	q.Lock()
	defer q.Unlock()

	item, err := q.readItemByID(q.head + 1)
	if err != nil {
		return item.Value, err
	}

	oldHead := q.head
	oldSegments := q.segments
	q.head++

	batch := new(leveldb.Batch)
	segments, changed, deleted := q.planReclaimLocked(batch)
	q.putState(batch, queueState{head: q.head, tail: q.tail})
	if changed {
		q.putSegments(batch, segments)
	}
	if err := q.db.Write(batch, nil); err != nil {
		q.head = oldHead
		q.segments = oldSegments
		return item.Value, err
	}

	q.segments = segments
	if len(segments) == 0 {
		q.currentBytes = 0
	}
	q.compactRanges(deleted)
	return item.Value, err
}

// PutBack returns value to the queue
func (q *Queue) PutBack(value []byte) error {
	q.Lock()
	defer q.Unlock()
	if q.head < 1 {
		return ErrInvalidHeadValue
	}

	id := q.head
	segments := append([]segment{}, q.segments...)
	changed := false
	if !segmentsContain(segments, id, q.tail) {
		segments = append(segments, segment{start: id})
		sort.Slice(segments, func(i, j int) bool {
			return segments[i].start < segments[j].start
		})
		changed = true
	}

	batch := new(leveldb.Batch)
	batch.Put(q.dbKey(id), value)
	q.putState(batch, queueState{head: id - 1, tail: q.tail})
	if changed {
		q.putSegments(batch, segments)
	}
	if err := q.db.Write(batch, nil); err != nil {
		return err
	}

	q.head = id - 1
	q.segments = segments
	if len(segments) > 0 && id >= segments[len(segments)-1].start {
		q.currentBytes += int64(len(value))
	}
	return nil
}

// Peek returns next value without removing it from the queue
func (q *Queue) Peek() ([]byte, error) {
	q.RLock()
	defer q.RUnlock()
	item, err := q.readItemByID(q.head + 1)
	return item.Value, err
}

// ReadItemByID returns a value by it's id
func (q *Queue) ReadItemByID(id uint64) (*Item, error) {
	q.RLock()
	defer q.RUnlock()
	return q.readItemByID(id)
}

func (q *Queue) readItemByID(id uint64) (*Item, error) {
	if id < q.oldestRetainedID() || id > q.tail {
		if q.length() < 1 {
			return &Item{}, ErrIsEmpty
		}
		return &Item{}, ErrIDOutOfBounds
	}

	var err error
	item := &Item{ID: id, Key: q.dbKey(id)}
	item.Value, err = q.db.Get(item.Key, nil)
	return item, err
}

// ReadItemByOffset returns an item by offset from the queue head, starting from 0.
func (q *Queue) ReadItemByOffset(offset uint64) (*Item, error) {
	q.RLock()
	defer q.RUnlock()
	return q.readItemByID(q.head + 1 + offset)
}

// DeleteAll deletes all items from the queue.
// This is expensive operation. If you want to drop all elements,
// it's better to close the queue and leveldb folder
func (q *Queue) DeleteAll() error {
	q.Lock()
	defer q.Unlock()

	iter := q.db.NewIterator(util.BytesPrefix(q.opts.KeyPrefix), nil)
	defer iter.Release()

	batch := new(leveldb.Batch)

	for iter.Next() {
		if !q.isItemKey(iter.Key()) {
			continue
		}
		batch.Delete(iter.Key())
	}
	if err := iter.Error(); err != nil {
		return err
	}
	q.putState(batch, queueState{})
	batch.Delete(q.metaKey(queueSegmentsName))
	if err := q.db.Write(batch, nil); err != nil {
		return err
	}
	q.head = 0
	q.tail = 0
	q.segments = nil
	q.currentBytes = 0
	for reader := range q.readers {
		reader.SetPosition(0)
	}
	return nil
}

// Stats returns stats struct
func (q *Queue) Stats() *Stats {
	return q.stats
}

// Path returns leveldb database file path
func (q *Queue) Path() string {
	return filepath.Join(q.DataDir, q.Name)
}

func (q *Queue) open() error {
	if validQueueNameRegex.MatchString(q.Name) || len(q.Name) < 1 {
		return ErrInvalidName
	}

	if len(q.Name) > 100 {
		return ErrNameTooLong
	}

	if q.opts == nil {
		q.opts = &Options{}
	}
	q.segmentSize = q.opts.SegmentSize
	if q.segmentSize <= 0 {
		q.segmentSize = defaultSegmentSize
	}
	q.maxRetainedSegments = q.opts.MaxRetainedSegments
	if q.maxRetainedSegments <= 0 {
		q.maxRetainedSegments = defaultMaxRetainedSegments
	}
	if q.readers == nil {
		q.readers = make(map[*Reader]struct{})
	}

	if !q.isShared {
		var err error
		q.db, err = leveldb.OpenFile(
			q.Path(),
			&opt.Options{OpenFilesCacheCapacity: levelDBOpenFilesCacheCapacity},
		)
		if err != nil {
			return err
		}
	}
	q.isOpened = true
	return q.initialize()
}

func (q *Queue) dbKey(id uint64) []byte {
	if len(q.opts.KeyPrefix) == 0 {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, id)
		return key
	}
	key := make([]byte, len(q.opts.KeyPrefix)+8)
	copy(key[0:len(q.opts.KeyPrefix)], q.opts.KeyPrefix)
	binary.BigEndian.PutUint64(key[len(q.opts.KeyPrefix):], id)
	return key
}

func (q *Queue) dbKeyToID(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[len(q.opts.KeyPrefix):])
}

func (q *Queue) length() uint64 {
	return q.tail - q.head
}

func (q *Queue) initialize() error {
	iter := q.db.NewIterator(util.BytesPrefix(q.opts.KeyPrefix), nil)
	defer iter.Release()

	var (
		count uint64
		first uint64
		last  uint64
	)
	for iter.Next() {
		if !q.isItemKey(iter.Key()) {
			continue
		}
		id := q.dbKeyToID(iter.Key())
		if count == 0 || id < first {
			first = id
		}
		if count == 0 || id > last {
			last = id
		}
		count++
	}
	if err := iter.Error(); err != nil {
		return err
	}

	state, hasState, err := q.loadState()
	if err != nil {
		return err
	}
	if hasState {
		q.head = state.head
		q.tail = state.tail
	} else if count > 0 {
		q.head = first - 1
		q.tail = last
	}

	if count == 0 {
		if !hasState {
			q.head = 0
			q.tail = 0
		}
		q.segments = nil
		q.currentBytes = 0
		return nil
	}
	if q.tail < last {
		q.tail = last
	}
	if q.head > q.tail {
		q.head = q.tail
	}

	segments, hasSegments, err := q.loadSegments()
	if err != nil {
		return err
	}
	if !hasSegments {
		segments = []segment{{start: first}}
	}
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].start < segments[j].start
	})
	q.segments = normalizeSegments(segments, first, q.tail)
	if !hasSegments {
		q.currentBytes = q.segmentSize
		return nil
	}
	q.currentBytes, err = q.lastSegmentBytes()
	return err
}

func segmentsContain(segments []segment, id, tail uint64) bool {
	for i, seg := range segments {
		start := seg.start
		end := tail
		if i+1 < len(segments) {
			end = segments[i+1].start - 1
		}
		if id >= start && id <= end {
			return true
		}
	}
	return false
}
