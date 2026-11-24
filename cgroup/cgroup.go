package cgroup

import (
	"encoding/binary"
	"errors"
	"regexp"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"

	"github.com/bogdanovich/siberite/queue"
)

const (
	cgCursorPrefix      = "_c:"
	cgFailedReadsPrefix = "_r:"
)

var (
	// make sure ConsumerGroup implements queue.Consumer interface
	_ queue.Consumer = (*ConsumerGroup)(nil)

	alphaNumericRegexp = regexp.MustCompile(`[^a-zA-Z0-9_]+`)
)

// ErrInvalidName is returned when consumer group name is not alphanumeric
var ErrInvalidName = errors.New("cgroup: name is not alphanumeric")

// ConsumerGroup represents a consumer group that reads from a
// source queue, stores its own cursor position and saves failed
// reliable reads in order to serve them to other consumers later
type ConsumerGroup struct {
	sync.RWMutex
	Name        string
	stats       *queue.Stats
	source      *queue.Queue
	storage     *leveldb.DB
	cursor      uint64
	failedReads *queue.Queue
	cursorKey   []byte
	readLease   *queue.Reader
}

// NewConsumerGroup initializes a consumer group
func NewConsumerGroup(name string, source *queue.Queue,
	storage *leveldb.DB) (*ConsumerGroup, error) {
	cg := &ConsumerGroup{
		Name:    name,
		stats:   &queue.Stats{},
		source:  source,
		storage: storage,
	}
	cg.cursorKey = []byte(cgCursorPrefix + cg.Name)
	return cg, cg.initialize()
}

// GetNext returns next value for that particular consumer group
func (cg *ConsumerGroup) GetNext() ([]byte, error) {
	cg.Lock()
	defer cg.Unlock()

	// serve from failedReads first
	if !cg.failedReads.IsEmpty() {
		return cg.failedReads.GetNext()
	}

	item, err := cg.readNextItemFromSource()
	if err != nil {
		return nil, err
	}
	if err := cg.updateCursor(item.ID); err != nil {
		return nil, err
	}
	return item.Value, err
}

// Peek returns next value without updating the cursor
func (cg *ConsumerGroup) Peek() ([]byte, error) {
	cg.Lock()
	defer cg.Unlock()

	// serve from failedReads first
	if !cg.failedReads.IsEmpty() {
		return cg.failedReads.Peek()
	}

	item, err := cg.readNextItemFromSource()
	if err != nil {
		return nil, err
	}
	return item.Value, err
}

// PutBack returns failed item back so it can be served to next consumer
func (cg *ConsumerGroup) PutBack(value []byte) error {
	return cg.failedReads.Enqueue(value)
}

// Length returns remaining number of items for consumer group
func (cg *ConsumerGroup) Length() uint64 {
	cg.RLock()
	defer cg.RUnlock()
	length := cg.failedReads.Length()
	if tail := cg.source.Tail(); tail > cg.cursor {
		length += tail - cg.cursor
	}
	return length
}

// IsEmpty returns false if thereis no more items for this consumer group
func (cg *ConsumerGroup) IsEmpty() bool {
	return cg.Length() < 1
}

// Source returns source queue Consumer interface
func (cg *ConsumerGroup) Source() queue.Consumer {
	return cg.source
}

// Stats returns stats struct
func (cg *ConsumerGroup) Stats() *queue.Stats {
	return cg.stats
}

func (cg *ConsumerGroup) readNextItemFromSource() (*queue.Item, error) {
	item, err := cg.source.ReadItemByID(cg.cursor + 1)
	if err == nil {
		return item, nil
	}

	// Only recover when the cursor points before the retained window.
	if errors.Is(err, queue.ErrIDOutOfBounds) && cg.cursor < cg.source.Head() {
		if err := cg.updateCursor(cg.source.Head()); err != nil {
			return &queue.Item{}, err
		}
		return cg.source.ReadItemByID(cg.cursor + 1)
	}
	return item, err
}

// Flush resets consumer group
func (cg *ConsumerGroup) Flush() error {
	cg.Lock()
	defer cg.Unlock()
	err := cg.failedReads.DeleteAll()
	if err == nil {
		return cg.updateCursor(cg.source.Head())
	}
	return err
}

// Delete deletes all the data associated with consumer group
func (cg *ConsumerGroup) Delete() error {
	cg.Lock()
	defer cg.Unlock()
	err := cg.failedReads.DeleteAll()
	if err == nil {
		cg.cursor = 0
		err = cg.storage.Delete(cg.cursorKey, nil)
	}
	if err == nil && cg.readLease != nil {
		err = cg.readLease.Close()
		cg.readLease = nil
	}
	return err
}

func (cg *ConsumerGroup) close() {
	cg.Lock()
	defer cg.Unlock()
	if cg.readLease != nil {
		cg.readLease.Detach()
		cg.readLease = nil
	}
}

func (cg *ConsumerGroup) initialize() error {
	if alphaNumericRegexp.MatchString(cg.Name) {
		return ErrInvalidName
	}

	err := cg.loadCursor()
	if err != nil {
		return err
	}

	cg.failedReads, err = queue.OpenShared(cg.Name,
		cgFailedReadsPrefix+cg.Name+":", cg.storage)
	if err != nil {
		return err
	}

	if cg.cursor > cg.source.Tail() {
		if err := cg.updateCursor(cg.source.Tail()); err != nil {
			return err
		}
	}
	cg.readLease, err = cg.source.NewReader(cg.cursor)
	return err
}

func (cg *ConsumerGroup) loadCursor() error {
	value, err := cg.storage.Get(cg.cursorKey, nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return cg.updateCursor(cg.source.Head())
		}
		return err
	}
	cg.cursor = binary.BigEndian.Uint64(value)
	return nil
}

func (cg *ConsumerGroup) updateCursor(cursor uint64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, cursor)
	if err := cg.storage.Put(cg.cursorKey, value, nil); err != nil {
		return err
	}
	cg.cursor = cursor
	if cg.readLease != nil {
		cg.readLease.SetPosition(cursor)
	}
	return nil
}
