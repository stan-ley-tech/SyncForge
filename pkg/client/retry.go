package client

import (
	"math/rand"
	"time"
)

// RetryPolicy configures how the client retries transient failures —
// network errors and 5xx server responses — when talking to the sync
// server. Because push is idempotent (op ids are idempotency keys) and
// pull has no side effects, retrying either is always safe; only the
// backoff schedule needs tuning.
//
// 4xx responses (bad request, unauthorized, not found, ...) are never
// retried: the server has already rejected the request as invalid, and
// retrying it verbatim cannot change that.
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

// DefaultRetryPolicy allows up to 5 attempts with full-jitter exponential
// backoff between 100ms and 2s.
var DefaultRetryPolicy = RetryPolicy{
	MaxAttempts: 5,
	BaseDelay:   100 * time.Millisecond,
	MaxDelay:    2 * time.Second,
}

func (p RetryPolicy) normalized() RetryPolicy {
	if p.MaxAttempts < 1 {
		p.MaxAttempts = 1
	}
	if p.BaseDelay <= 0 {
		p.BaseDelay = DefaultRetryPolicy.BaseDelay
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = DefaultRetryPolicy.MaxDelay
	}
	return p
}

// backoff returns a full-jitter exponential delay for the given zero-based
// retry attempt (attempt 0 is the delay before the second overall try).
// Full jitter (a uniform random draw between 0 and the exponential cap,
// rather than always waiting the full cap) avoids many retrying clients
// staying in lockstep after a shared outage.
func (p RetryPolicy) backoff(attempt int) time.Duration {
	maxD := p.MaxDelay
	d := p.BaseDelay
	for i := 0; i < attempt && d < maxD; i++ {
		d *= 2
	}
	if d > maxD {
		d = maxD
	}
	return time.Duration(rand.Int63n(int64(d) + 1))
}

// retryableError marks an error from one HTTP attempt as safe to retry
// (a network failure or 5xx response), as opposed to a definitive
// rejection (4xx) that retrying cannot fix.
type retryableError struct{ err error }

func (r *retryableError) Error() string { return r.err.Error() }
func (r *retryableError) Unwrap() error { return r.err }
