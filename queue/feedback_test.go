package queue

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_EnqueueFullReturnsImmediateRetryFeedback(t *testing.T) {
	q, err := Open("feedback_full", t.TempDir(), &Options{
		MaxLength:      1,
		FullRetryAfter: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Drop()

	if err := q.Enqueue([]byte("first")); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	err = q.Enqueue([]byte("second"))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected full queue error")
	}
	assert.ErrorIs(t, err, ErrQueueFull)
	assert.Less(t, elapsed, 20*time.Millisecond, "full queue rejection must be immediate")

	var fullErr *FullError
	if !errors.As(err, &fullErr) {
		t.Fatalf("expected *FullError, got %T: %v", err, err)
	}
	assert.EqualValues(t, 1, fullErr.Length)
	assert.EqualValues(t, 1, fullErr.Capacity)
	assert.Equal(t, 100*time.Millisecond, fullErr.RetryAfter)
	assert.Contains(t, err.Error(), "Retry-After: 100ms")
	assert.Contains(t, err.Error(), "retry_after_ms=100")

	value, err := q.GetNext()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "first", string(value))

	if err := q.Enqueue([]byte("second")); err != nil {
		t.Fatal(err)
	}
	assert.EqualValues(t, 1, q.Length())
}

func Test_RetryAfterUsesDrainRate(t *testing.T) {
	q, err := Open("feedback_drain", t.TempDir(), &Options{MaxLength: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Drop()

	now := time.Now()
	q.Lock()
	q.recordDequeueLocked(now.Add(-20 * time.Millisecond))
	q.recordDequeueLocked(now)
	q.Unlock()

	retryAfter := q.RetryAfter()
	assert.Greater(t, retryAfter, time.Duration(0))
	assert.LessOrEqual(t, retryAfter, 20*time.Millisecond)
}
