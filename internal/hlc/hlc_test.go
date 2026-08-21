package hlc

import (
	"testing"
	"time"
)

func TestTimestampCompareAndString(t *testing.T) {
	a := Timestamp{Physical: 100, Logical: 0, NodeID: "A"}
	b := Timestamp{Physical: 100, Logical: 1, NodeID: "A"}
	c := Timestamp{Physical: 200, Logical: 0, NodeID: "A"}
	d := Timestamp{Physical: 100, Logical: 0, NodeID: "B"}

	if a.Compare(b) >= 0 {
		t.Fatalf("expected a < b, got compare=%d", a.Compare(b))
	}
	if b.Compare(c) >= 0 {
		t.Fatalf("expected b < c, got compare=%d", b.Compare(c))
	}
	if a.Compare(d) >= 0 {
		t.Fatalf("expected a < d (NodeID tiebreak), got compare=%d", a.Compare(d))
	}
	if !c.After(a) {
		t.Fatalf("expected c.After(a) to be true")
	}
	if a.Compare(a) != 0 {
		t.Fatalf("expected a == a")
	}

	// String form must sort the same way as Compare, for the same set of
	// timestamps, since conflict resolution relies on comparing strings
	// stored in the oplog.
	if a.String() >= b.String() {
		t.Fatalf("expected a.String() < b.String(), got %q >= %q", a.String(), b.String())
	}
	if b.String() >= c.String() {
		t.Fatalf("expected b.String() < c.String(), got %q >= %q", b.String(), c.String())
	}
}

func TestClockNowIsMonotonic(t *testing.T) {
	fixed := time.Unix(0, 1_000_000_000) // fixed wall time, forces logical bumps
	clk := NewClock("device-a")
	clk.now = func() time.Time { return fixed }

	var prev Timestamp
	for i := 0; i < 5; i++ {
		ts := clk.Now()
		if i > 0 && !ts.After(prev) {
			t.Fatalf("iteration %d: expected strictly increasing timestamps, got %+v after %+v", i, ts, prev)
		}
		prev = ts
	}
}

func TestClockNowAdvancesWithWallClock(t *testing.T) {
	tick := int64(1_000_000_000)
	clk := NewClock("device-a")
	clk.now = func() time.Time { return time.Unix(0, tick) }

	first := clk.Now()
	tick += 1_000_000
	second := clk.Now()

	if second.Physical <= first.Physical {
		t.Fatalf("expected physical component to advance with wall clock")
	}
	if second.Logical != 0 {
		t.Fatalf("expected logical counter to reset to 0 when physical time advances, got %d", second.Logical)
	}
}

func TestClockObserveAdoptsAheadRemote(t *testing.T) {
	fixed := time.Unix(0, 1_000_000_000)
	clk := NewClock("device-a")
	clk.now = func() time.Time { return fixed }

	remote := Timestamp{Physical: fixed.UnixNano() + 5_000_000_000, Logical: 3, NodeID: "device-b"}
	observed := clk.Observe(remote)

	if observed.Physical != remote.Physical {
		t.Fatalf("expected observed physical to adopt the ahead remote physical, got %d want %d", observed.Physical, remote.Physical)
	}
	if observed.Logical != remote.Logical+1 {
		t.Fatalf("expected observed logical = remote.Logical+1 = %d, got %d", remote.Logical+1, observed.Logical)
	}
	if observed.NodeID != "device-a" {
		t.Fatalf("expected Observe to stamp the local node id, got %q", observed.NodeID)
	}
}

func TestClockObserveKeepsLocalWhenAhead(t *testing.T) {
	tick := int64(10_000_000_000)
	clk := NewClock("device-a")
	clk.now = func() time.Time { return time.Unix(0, tick) }
	local := clk.Now() // physical=10s, logical=0

	remote := Timestamp{Physical: 1_000_000_000, Logical: 9, NodeID: "device-b"}
	observed := clk.Observe(remote)

	if observed.Physical != local.Physical {
		t.Fatalf("expected local physical to dominate a stale remote, got %d want %d", observed.Physical, local.Physical)
	}
	if observed.Logical != local.Logical+1 {
		t.Fatalf("expected logical bump when local physical dominates, got %d", observed.Logical)
	}
}

func TestClockObserveIsMonotonicAndDeterministic(t *testing.T) {
	// Two clocks that exchange timestamps back and forth must never produce
	// equal or decreasing values, and replaying the exact same sequence of
	// Now()/Observe() calls against fresh clocks must reproduce the same
	// totally-ordered sequence, which is what the conflict resolver relies on.
	run := func() []Timestamp {
		fixed := time.Unix(0, 1_000_000_000)
		a := NewClock("device-a")
		a.now = func() time.Time { return fixed }
		b := NewClock("device-b")
		b.now = func() time.Time { return fixed }

		var seq []Timestamp
		t1 := a.Now()
		seq = append(seq, t1)
		t2 := b.Observe(t1)
		seq = append(seq, t2)
		t3 := a.Observe(t2)
		seq = append(seq, t3)
		t4 := b.Now()
		seq = append(seq, t4)
		return seq
	}

	seqA := run()
	seqB := run()

	for i := range seqA {
		if seqA[i] != seqB[i] {
			t.Fatalf("non-deterministic replay at index %d: %+v vs %+v", i, seqA[i], seqB[i])
		}
		if i > 0 && !seqA[i].After(seqA[i-1]) {
			t.Fatalf("sequence not strictly increasing at index %d: %+v then %+v", i, seqA[i-1], seqA[i])
		}
	}
}
