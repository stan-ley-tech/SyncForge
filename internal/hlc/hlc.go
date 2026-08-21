// Package hlc implements a Hybrid Logical Clock: a timestamp that combines
// wall-clock time with a logical counter so that timestamps are both
// roughly wall-time meaningful and totally, monotonically ordered even
// across clock skew between devices.
//
// A Timestamp's string form is lexicographically sortable, which lets it
// double as the deterministic tiebreaker for conflict resolution: given two
// concurrent writes to the same record, the write with the greater
// Timestamp always wins, on every replica, regardless of arrival order.
package hlc

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// Timestamp is an HLC value: a physical time component, a logical counter
// that disambiguates events within the same physical tick, and the id of
// the node that produced it (the final tiebreaker for otherwise-equal
// clocks from different nodes).
type Timestamp struct {
	Physical int64  // Unix nanoseconds
	Logical  uint32 // logical counter, reset whenever Physical advances
	NodeID   string // device/server id, breaks ties between equal (Physical, Logical) pairs
}

// Compare returns -1, 0, or 1 if t sorts before, equal to, or after other.
func (t Timestamp) Compare(other Timestamp) int {
	switch {
	case t.Physical < other.Physical:
		return -1
	case t.Physical > other.Physical:
		return 1
	}
	switch {
	case t.Logical < other.Logical:
		return -1
	case t.Logical > other.Logical:
		return 1
	}
	return strings.Compare(t.NodeID, other.NodeID)
}

// After reports whether t is strictly greater than other.
func (t Timestamp) After(other Timestamp) bool {
	return t.Compare(other) > 0
}

// String renders the timestamp as a fixed-width, lexicographically
// sortable string: "<19-digit physical nanos>-<10-digit logical>-<nodeID>".
func (t Timestamp) String() string {
	return fmt.Sprintf("%019d-%010d-%s", t.Physical, t.Logical, t.NodeID)
}

// Clock is a Hybrid Logical Clock for a single node. It is safe for
// concurrent use.
type Clock struct {
	mu     sync.Mutex
	last   Timestamp
	nodeID string
	now    func() time.Time // overridable for tests
}

// NewClock creates a Clock for the given node id.
func NewClock(nodeID string) *Clock {
	return &Clock{nodeID: nodeID, now: time.Now}
}

// Now advances the clock for a local event and returns the new timestamp.
// It is monotonic: the returned timestamp is always strictly greater than
// every timestamp previously produced or Observed by this clock.
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	phys := c.now().UnixNano()
	if phys <= c.last.Physical {
		c.last.Logical++
	} else {
		c.last.Physical = phys
		c.last.Logical = 0
	}
	c.last.NodeID = c.nodeID
	return c.last
}

// Observe folds a timestamp received from another node into the clock, so
// that subsequent local events are ordered after it. It implements the
// standard HLC receive rule (Kulkarni et al.) and returns the resulting
// local timestamp.
func (c *Clock) Observe(remote Timestamp) Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	phys := c.now().UnixNano()
	l, lMsg := c.last.Physical, remote.Physical
	newPhys := max(phys, l, lMsg)

	var newLogical uint32
	switch {
	case newPhys == l && newPhys == lMsg:
		newLogical = max(c.last.Logical, remote.Logical) + 1
	case newPhys == l:
		newLogical = c.last.Logical + 1
	case newPhys == lMsg:
		newLogical = remote.Logical + 1
	default:
		newLogical = 0
	}

	c.last = Timestamp{Physical: newPhys, Logical: newLogical, NodeID: c.nodeID}
	return c.last
}
