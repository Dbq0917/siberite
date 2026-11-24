package queue

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func openSegmentQueue(t *testing.T, opts *Options) *Queue {
	t.Helper()
	q, err := Open("segments", t.TempDir(), opts)
	assert.NoError(t, err)
	t.Cleanup(q.Drop)
	return q
}

func itemKeyCount(t *testing.T, q *Queue) int {
	t.Helper()
	iter := q.db.NewIterator(util.BytesPrefix(q.opts.KeyPrefix), nil)
	defer iter.Release()

	count := 0
	for iter.Next() {
		if q.isItemKey(iter.Key()) {
			count++
		}
	}
	assert.NoError(t, iter.Error())
	return count
}

func TestSegmentReclaimWaitsForReader(t *testing.T) {
	q := openSegmentQueue(t, &Options{
		SegmentSize:         10,
		MaxRetainedSegments: 100,
	})
	reader, err := q.NewReader(0)
	assert.NoError(t, err)
	defer reader.Close()

	for i := 0; i < 10; i++ {
		assert.NoError(t, q.Enqueue([]byte(fmt.Sprintf("%02d", i))))
	}

	for i := 0; i < 10; i++ {
		_, err := q.GetNext()
		assert.NoError(t, err)
	}
	assert.Equal(t, 10, itemKeyCount(t, q), "unread segment was reclaimed")

	reader.SetPosition(10)
	assert.NoError(t, q.Enqueue([]byte("10")))
	assert.Equal(t, 1, itemKeyCount(t, q), "read segments were not reclaimed")
}

func TestSegmentRetentionLimitAppliesBackpressure(t *testing.T) {
	q := openSegmentQueue(t, &Options{
		SegmentSize:         4,
		MaxRetainedSegments: 2,
	})
	reader, err := q.NewReader(0)
	assert.NoError(t, err)
	defer reader.Close()

	for i := 0; i < 4; i++ {
		assert.NoError(t, q.Enqueue([]byte("xx")))
	}
	for i := 0; i < 4; i++ {
		_, err := q.GetNext()
		assert.NoError(t, err)
	}

	assert.EqualError(t, q.Enqueue([]byte("xx")), ErrRetentionLimit.Error())
	assert.EqualValues(t, 4, q.Tail())
	assert.Equal(t, 4, itemKeyCount(t, q))

	reader.SetPosition(4)
	assert.NoError(t, q.Enqueue([]byte("xx")))
	assert.EqualValues(t, 5, q.Tail())
}

func TestSegmentStateSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	opts := &Options{SegmentSize: 4, MaxRetainedSegments: 10}
	q, err := Open("segments", dir, opts)
	assert.NoError(t, err)

	_, err = q.NewReader(0)
	assert.NoError(t, err)
	for i := 0; i < 6; i++ {
		assert.NoError(t, q.Enqueue([]byte(fmt.Sprintf("%d", i))))
	}
	for i := 0; i < 6; i++ {
		_, err := q.GetNext()
		assert.NoError(t, err)
	}
	q.Close()

	q, err = Open("segments", dir, opts)
	assert.NoError(t, err)
	defer q.Drop()
	assert.EqualValues(t, 6, q.Head())
	assert.EqualValues(t, 6, q.Tail())
	assert.Equal(t, 6, itemKeyCount(t, q))

	for i := 1; i <= 6; i++ {
		item, err := q.ReadItemByID(uint64(i))
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("%d", i-1), string(item.Value))
	}
}
