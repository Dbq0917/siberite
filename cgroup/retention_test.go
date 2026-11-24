package cgroup

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/bogdanovich/siberite/queue"
)

func TestConsumerGroupDoesNotSkipWhenSourceCleansAhead(t *testing.T) {
	q, err := CGQueueOpenWithOptions("retention", t.TempDir(), &queue.Options{
		SegmentSize:         256,
		MaxRetainedSegments: 200,
	})
	assert.NoError(t, err)
	defer q.Drop()

	const messages = 5000
	for i := 0; i < messages; i++ {
		assert.NoError(t, q.Enqueue([]byte(strconv.Itoa(i))))
	}

	cg, err := q.ConsumerGroup("slow")
	assert.NoError(t, err)

	value, err := cg.GetNext()
	assert.NoError(t, err)
	assert.Equal(t, "0", string(value))

	for i := 0; i < messages; i++ {
		_, err := q.GetNext()
		assert.NoError(t, err)
	}

	for i := 1; i < messages; i++ {
		value, err := cg.GetNext()
		assert.NoError(t, err)
		assert.Equal(t, strconv.Itoa(i), string(value))
	}

	_, err = cg.GetNext()
	assert.EqualError(t, err, queue.ErrIsEmpty.Error())
}

func TestConsumerGroupResumesAfterRestartWithRetainedSegments(t *testing.T) {
	dir := t.TempDir()
	opts := &queue.Options{
		SegmentSize:         128,
		MaxRetainedSegments: 100,
	}

	q, err := CGQueueOpenWithOptions("retention_restart", dir, opts)
	assert.NoError(t, err)

	const messages = 500
	for i := 0; i < messages; i++ {
		assert.NoError(t, q.Enqueue([]byte(strconv.Itoa(i))))
	}

	cg, err := q.ConsumerGroup("slow")
	assert.NoError(t, err)
	value, err := cg.GetNext()
	assert.NoError(t, err)
	assert.Equal(t, "0", string(value))

	for i := 0; i < messages; i++ {
		_, err := q.GetNext()
		assert.NoError(t, err)
	}
	q.Close()

	q, err = CGQueueOpenWithOptions("retention_restart", dir, opts)
	assert.NoError(t, err)
	defer q.Drop()

	cg, err = q.ConsumerGroup("slow")
	assert.NoError(t, err)
	for i := 1; i < messages; i++ {
		value, err := cg.GetNext()
		assert.NoError(t, err)
		assert.Equal(t, strconv.Itoa(i), string(value))
	}
}
