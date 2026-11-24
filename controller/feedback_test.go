package controller

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/bogdanovich/siberite/repository"
	"github.com/stretchr/testify/assert"
)

func Test_Controller_FullQueueResponseHasRetryFeedback(t *testing.T) {
	repo, err := repository.NewRepositoryWithOptions(t.TempDir(), &repository.Options{
		MaxQueueLength: 1,
		FullRetryAfter: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer repo.CloseAllQueues()

	conn := newMockTCPConn()
	c := NewSession(conn, repo)
	defer c.FinishSession()

	q, err := repo.GetQueue("feedback")
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue([]byte("full")); err != nil {
		t.Fatal(err)
	}

	fmt.Fprint(&conn.ReadBuffer, "set feedback 0 0 1\r\nx\r\n")
	err = c.Dispatch()

	if err == nil {
		t.Fatal("expected dispatch error")
	}
	var controllerErr *Error
	if !errors.As(err, &controllerErr) {
		t.Fatalf("expected *Error, got %T: %v", err, err)
	}
	assert.Equal(t, serverError, controllerErr.Type)
	assert.Contains(t, conn.WriteBuffer.String(), "SERVER_ERROR queue: is full")
	assert.Contains(t, conn.WriteBuffer.String(), "Retry-After: 100ms")
	assert.Contains(t, conn.WriteBuffer.String(), "retry_after_ms=100")
}
