package queue

import (
	"encoding/binary"
	"errors"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	defaultSegmentSize         = int64(64 << 20)
	defaultMaxRetainedSegments = 256

	queueMetaPrefix   = "\x00siberite:queue:"
	queueStateName    = "state"
	queueSegmentsName = "segments"
)

type segment struct {
	start uint64
}

type queueState struct {
	head uint64
	tail uint64
}

type keyRange struct {
	start uint64
	end   uint64
}

func (q *Queue) metaKey(name string) []byte {
	key := make([]byte, 0, len(q.opts.KeyPrefix)+len(queueMetaPrefix)+len(name))
	key = append(key, q.opts.KeyPrefix...)
	key = append(key, queueMetaPrefix...)
	key = append(key, name...)
	return key
}

func (q *Queue) isItemKey(key []byte) bool {
	return len(key) == len(q.opts.KeyPrefix)+8
}

func encodeQueueState(state queueState) []byte {
	value := make([]byte, 16)
	binary.BigEndian.PutUint64(value[0:8], state.head)
	binary.BigEndian.PutUint64(value[8:16], state.tail)
	return value
}

func decodeQueueState(value []byte) (queueState, error) {
	if len(value) != 16 {
		return queueState{}, errors.New("queue: invalid state metadata")
	}
	return queueState{
		head: binary.BigEndian.Uint64(value[0:8]),
		tail: binary.BigEndian.Uint64(value[8:16]),
	}, nil
}

func encodeSegments(segments []segment) []byte {
	value := make([]byte, 8+len(segments)*8)
	binary.BigEndian.PutUint64(value[0:8], uint64(len(segments)))
	for i, seg := range segments {
		binary.BigEndian.PutUint64(value[8+i*8:16+i*8], seg.start)
	}
	return value
}

func decodeSegments(value []byte) ([]segment, error) {
	if len(value) < 8 {
		return nil, errors.New("queue: invalid segment metadata")
	}
	count := binary.BigEndian.Uint64(value[0:8])
	if count > uint64((len(value)-8)/8) {
		return nil, errors.New("queue: invalid segment metadata")
	}
	segments := make([]segment, int(count))
	for i := range segments {
		segments[i].start = binary.BigEndian.Uint64(value[8+i*8 : 16+i*8])
	}
	return segments, nil
}

func (q *Queue) loadState() (queueState, bool, error) {
	value, err := q.db.Get(q.metaKey(queueStateName), nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return queueState{}, false, nil
	}
	if err != nil {
		return queueState{}, false, err
	}
	state, err := decodeQueueState(value)
	return state, true, err
}

func (q *Queue) loadSegments() ([]segment, bool, error) {
	value, err := q.db.Get(q.metaKey(queueSegmentsName), nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	segments, err := decodeSegments(value)
	return segments, true, err
}

func (q *Queue) putState(batch *leveldb.Batch, state queueState) {
	batch.Put(q.metaKey(queueStateName), encodeQueueState(state))
}

func (q *Queue) putSegments(batch *leveldb.Batch, segments []segment) {
	batch.Put(q.metaKey(queueSegmentsName), encodeSegments(segments))
}

func (q *Queue) oldestRetainedID() uint64 {
	if len(q.segments) == 0 {
		if q.tail > q.head {
			return q.head + 1
		}
		return q.tail + 1
	}
	return q.segments[0].start
}

func (q *Queue) safeReclaimID() uint64 {
	safe := q.head
	for reader := range q.readers {
		position := reader.Position()
		if position < safe {
			safe = position
		}
	}
	return safe
}

func (q *Queue) planReclaimLocked(batch *leveldb.Batch) ([]segment, bool, []keyRange) {
	if len(q.segments) == 0 {
		return q.segments, false, nil
	}

	safe := q.safeReclaimID()
	segments := q.segments
	changed := false
	deleted := make([]keyRange, 0, len(segments))

	for len(segments) > 1 {
		end := segments[1].start - 1
		if end > safe {
			break
		}
		q.deleteRange(batch, segments[0].start, end)
		deleted = append(deleted, keyRange{start: segments[0].start, end: end})
		segments = segments[1:]
		changed = true
	}

	if len(segments) == 1 && q.length() == 0 && safe >= q.tail {
		q.deleteRange(batch, segments[0].start, q.tail)
		deleted = append(deleted, keyRange{start: segments[0].start, end: q.tail})
		segments = nil
		changed = true
	}
	return segments, changed, deleted
}

func (q *Queue) reclaimLocked() error {
	batch := new(leveldb.Batch)
	segments, changed, deleted := q.planReclaimLocked(batch)
	if !changed {
		return nil
	}
	q.putSegments(batch, segments)
	if err := q.db.Write(batch, nil); err != nil {
		return err
	}
	q.segments = segments
	if len(segments) == 0 {
		q.currentBytes = 0
	}
	q.compactRanges(deleted)
	return nil
}

func (q *Queue) compactRanges(ranges []keyRange) {
	for _, r := range ranges {
		if r.end == ^uint64(0) {
			continue
		}
		_ = q.db.CompactRange(util.Range{
			Start: q.dbKey(r.start),
			Limit: q.dbKey(r.end + 1),
		})
	}
}

func (q *Queue) deleteRange(batch *leveldb.Batch, start, end uint64) {
	for id := start; id <= end; id++ {
		batch.Delete(q.dbKey(id))
		if id == ^uint64(0) {
			break
		}
	}
}

func (q *Queue) lastSegmentBytes() (int64, error) {
	if len(q.segments) == 0 {
		return 0, nil
	}
	start := q.segments[len(q.segments)-1].start
	if start > q.tail {
		return 0, nil
	}
	iter := q.db.NewIterator(&util.Range{
		Start: q.dbKey(start),
		Limit: q.dbKey(q.tail + 1),
	}, nil)
	defer iter.Release()

	var size int64
	for iter.Next() {
		if !q.isItemKey(iter.Key()) {
			continue
		}
		size += int64(len(iter.Value()))
	}
	return size, iter.Error()
}

func normalizeSegments(segments []segment, first, last uint64) []segment {
	if first > last {
		return nil
	}
	result := make([]segment, 0, len(segments)+1)
	for _, seg := range segments {
		if seg.start == 0 || seg.start > last {
			continue
		}
		if len(result) > 0 && result[len(result)-1].start == seg.start {
			continue
		}
		result = append(result, seg)
	}
	if len(result) == 0 || result[0].start > first {
		result = append([]segment{{start: first}}, result...)
	} else {
		result[0].start = first
	}
	return result
}
