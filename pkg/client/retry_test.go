package client

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func fastTestPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 4, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}
}

func newRetryTestClient(t *testing.T, serverURL string) *Client {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "client.db")
	c, err := Open(dbPath, serverURL, WithRetryPolicy(fastTestPolicy()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestBackoffGrowsExponentiallyAndRespectsMax(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 10, BaseDelay: 10 * time.Millisecond, MaxDelay: 100 * time.Millisecond}

	for attempt := 0; attempt < 8; attempt++ {
		d := p.backoff(attempt)
		if d < 0 || d > p.MaxDelay {
			t.Fatalf("attempt %d: backoff %v out of bounds [0, %v]", attempt, d, p.MaxDelay)
		}
	}
}

func TestBackoffZeroValuePolicyIsNormalized(t *testing.T) {
	var p RetryPolicy
	normalized := p.normalized()
	if normalized.MaxAttempts < 1 {
		t.Fatalf("expected a zero-value policy to normalize to at least 1 attempt, got %d", normalized.MaxAttempts)
	}
	if normalized.BaseDelay <= 0 || normalized.MaxDelay <= 0 {
		t.Fatalf("expected a zero-value policy to normalize to positive delays, got %+v", normalized)
	}
}

func TestDoJSONRetriesOn5xxThenSucceeds(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"error":"try again"}`))
			return
		}
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"device_id":"d1","device_token":"t1"}`))
	}))
	defer srv.Close()

	c := newRetryTestClient(t, srv.URL)
	if err := c.Register(context.Background(), "Laptop"); err != nil {
		t.Fatalf("expected Register to eventually succeed after transient 503s, got: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("expected exactly 3 attempts (2 failures + 1 success), got %d", got)
	}
}

func TestDoJSONDoesNotRetry4xx(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"device_id is required"}`))
	}))
	defer srv.Close()

	c := newRetryTestClient(t, srv.URL)
	err := c.Register(context.Background(), "Laptop")
	if err == nil {
		t.Fatalf("expected Register to fail against a server that always 400s")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected exactly 1 attempt for a non-retryable 4xx, got %d", got)
	}
}

func TestDoJSONGivesUpAfterMaxAttempts(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"down"}`))
	}))
	defer srv.Close()

	policy := fastTestPolicy()
	c := newRetryTestClient(t, srv.URL)

	err := c.Register(context.Background(), "Laptop")
	if err == nil {
		t.Fatalf("expected Register to fail against a server that always 500s")
	}
	if got := atomic.LoadInt32(&calls); int(got) != policy.MaxAttempts {
		t.Fatalf("expected exactly %d attempts, got %d", policy.MaxAttempts, got)
	}
}

func TestDoJSONRetriesOnConnectionFailureThenSucceeds(t *testing.T) {
	// Reserve an address, close the listener immediately (so the first
	// attempt hits connection refused), then start a real server on that
	// same address shortly after — simulating a server that is briefly
	// unreachable and then comes up.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve listener: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	c := newRetryTestClient(t, "http://"+addr)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/devices/register", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"device_id":"d1","device_token":"t1"}`))
	})
	delayedSrv := &http.Server{Addr: addr, Handler: mux}
	t.Cleanup(func() { delayedSrv.Close() })
	go func() {
		time.Sleep(5 * time.Millisecond)
		delayedSrv.ListenAndServe()
	}()

	if err := c.Register(context.Background(), "Laptop"); err != nil {
		t.Fatalf("expected Register to eventually succeed once the server comes up, got: %v", err)
	}
}
