package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func TestQueueStateIsPersistedWithEnqueue(t *testing.T) {
	q, err := Open("state", t.TempDir(), &Options{})
	assert.NoError(t, err)
	defer q.Drop()

	for i := 0; i < 5; i++ {
		assert.NoError(t, q.Enqueue([]byte{byte(i)}))
	}

	state, found, err := q.loadState()
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, queueState{head: 0, tail: 5}, state)
}

func TestOpenRepairsUncommittedTailState(t *testing.T) {
	dir := t.TempDir()
	q, err := Open("state", dir, &Options{})
	assert.NoError(t, err)

	for i := 0; i < 3; i++ {
		assert.NoError(t, q.Enqueue([]byte{byte(i)}))
	}

	stateKey := q.metaKey(queueStateName)
	assert.NoError(t, q.db.Put(stateKey, encodeQueueState(queueState{head: 0, tail: 4}), nil))
	q.Close()

	q, err = Open("state", dir, &Options{})
	assert.NoError(t, err)
	defer q.Drop()

	assert.EqualValues(t, 0, q.Head())
	assert.EqualValues(t, 3, q.Tail())
	assert.EqualValues(t, 3, q.Length())

	state, found, err := q.loadState()
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, queueState{head: 0, tail: 3}, state)
}

func TestOpenRestoresMissingStateFromItems(t *testing.T) {
	dir := t.TempDir()
	q, err := Open("state", dir, &Options{})
	assert.NoError(t, err)

	for i := 0; i < 4; i++ {
		assert.NoError(t, q.Enqueue([]byte{byte(i)}))
	}
	assert.NoError(t, q.db.Delete(q.metaKey(queueStateName), nil))
	q.Close()

	q, err = Open("state", dir, &Options{})
	assert.NoError(t, err)
	defer q.Drop()

	assert.EqualValues(t, 0, q.Head())
	assert.EqualValues(t, 4, q.Tail())
	assert.EqualValues(t, 4, q.Length())

	state, found, err := q.loadState()
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, queueState{head: 0, tail: 4}, state)
}

func TestOpenRepairsInvertedEmptyQueueState(t *testing.T) {
	dir := t.TempDir()
	q, err := Open("state", dir, &Options{})
	assert.NoError(t, err)

	assert.NoError(t, q.db.Put(
		q.metaKey(queueStateName),
		encodeQueueState(queueState{head: 10, tail: 5}),
		nil,
	))
	q.Close()

	q, err = Open("state", dir, &Options{})
	assert.NoError(t, err)
	defer q.Drop()

	assert.EqualValues(t, 10, q.Head())
	assert.EqualValues(t, 10, q.Tail())
	assert.EqualValues(t, 0, q.Length())
	assert.NoError(t, q.Enqueue([]byte("next")))
	assert.EqualValues(t, 11, q.Tail())
}

func TestSharedQueueStateIsNamespaced(t *testing.T) {
	dir := t.TempDir()
	db, err := leveldb.OpenFile(dir+"/shared", &opt.Options{})
	assert.NoError(t, err)
	defer db.Close()

	first, err := OpenShared("first", "first:", db)
	assert.NoError(t, err)
	second, err := OpenShared("second", "second:", db)
	assert.NoError(t, err)

	assert.NoError(t, first.Enqueue([]byte("one")))
	assert.NoError(t, second.Enqueue([]byte("two")))
	assert.NoError(t, second.Enqueue([]byte("three")))

	assert.EqualValues(t, 1, first.Length())
	assert.EqualValues(t, 2, second.Length())
}
