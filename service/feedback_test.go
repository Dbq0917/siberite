package service

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_FullQueueFixedRetryIntervalDoesNotLeakConnections(t *testing.T) {
	svc := New(t.TempDir())
	svc.MaxQueueLength = 1
	svc.FullRetryAfter = 100 * time.Millisecond
	if err := svc.initRepository(); err != nil {
		t.Fatal(err)
	}

	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	go svc.ServeTCP(listener)
	defer svc.Stop()

	addr := listener.Addr().String()
	line, err := dialAndSet(addr, "feedback-storm", "full")
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "STORED\r\n", line)
	waitCurrentConnections(t, svc, 0)

	stopSampling := make(chan struct{})
	samplingDone := make(chan struct{})
	var maxCurrent uint64
	go func() {
		defer close(samplingDone)
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopSampling:
				return
			case <-ticker.C:
				current := atomic.LoadUint64(&svc.repo.Stats.CurrentConnections)
				for {
					max := atomic.LoadUint64(&maxCurrent)
					if current <= max || atomic.CompareAndSwapUint64(&maxCurrent, max, current) {
						break
					}
				}
			}
		}
	}()

	for attempt := 0; attempt < 5; attempt++ {
		start := time.Now()
		line, err := dialAndSet(addr, "feedback-storm", "retry")
		elapsed := time.Since(start)

		if err != nil {
			t.Fatal(err)
		}
		assert.Contains(t, line, "SERVER_ERROR queue: is full")
		assert.Contains(t, line, "Retry-After: 100ms")
		assert.Contains(t, line, "retry_after_ms=100")
		assert.Less(t, elapsed, 100*time.Millisecond, "rejection should return immediately")

		waitCurrentConnections(t, svc, 0)
		time.Sleep(100 * time.Millisecond)
	}

	close(stopSampling)
	<-samplingDone

	assert.LessOrEqual(t, atomic.LoadUint64(&maxCurrent), uint64(1),
		"fixed-interval sequential retries must not accumulate sessions")
	assert.EqualValues(t, 0, atomic.LoadUint64(&svc.repo.Stats.CurrentConnections))
	assert.EqualValues(t, 6, atomic.LoadUint64(&svc.repo.Stats.TotalConnections),
		"one initial write plus five retries should each create a connection")
}

func Test_MaxConnectionsClosesExcessSessions(t *testing.T) {
	svc := New(t.TempDir())
	svc.MaxConnections = 2
	if err := svc.initRepository(); err != nil {
		t.Fatal(err)
	}

	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	go svc.ServeTCP(listener)
	defer svc.Stop()

	addr := listener.Addr().String()
	first := dialAndHold(t, addr)
	second := dialAndHold(t, addr)
	defer first.Close()
	defer second.Close()
	waitCurrentConnections(t, svc, 2)

	third, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer third.Close()
	if err := third.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	buffer := make([]byte, 1)
	_, err = third.Read(buffer)
	assert.ErrorIs(t, err, io.EOF)
	assert.LessOrEqual(t, atomic.LoadUint64(&svc.repo.Stats.CurrentConnections), uint64(2))

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	waitCurrentConnections(t, svc, 0)
}

func dialAndSet(addr, queueName, value string) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return "", err
	}

	_, err = fmt.Fprintf(conn, "set %s 0 0 %d\r\n%s\r\n", queueName, len(value), value)
	if err != nil {
		return "", err
	}
	return bufio.NewReader(conn).ReadString('\n')
}

func dialAndHold(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func waitCurrentConnections(t *testing.T, svc *Service, expected uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadUint64(&svc.repo.Stats.CurrentConnections) == expected {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if current := atomic.LoadUint64(&svc.repo.Stats.CurrentConnections); current != expected {
		t.Fatalf("current connections = %d, want %d", current, expected)
	}
}
