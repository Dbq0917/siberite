package queue

import (
	"encoding/binary"
	"errors"

	"github.com/syndtr/goleveldb/leveldb"
)

const (
	queueMetaPrefix = "\x00siberite:queue:"
	queueStateName  = "state"
)

type queueState struct {
	head uint64
	tail uint64
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

func (q *Queue) putState(batch *leveldb.Batch, state queueState) {
	batch.Put(q.metaKey(queueStateName), encodeQueueState(state))
}
