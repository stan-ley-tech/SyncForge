package conflict

import (
	"encoding/json"
	"testing"

	"github.com/stan-ley-tech/SyncForge/internal/hlc"
	"github.com/stan-ley-tech/SyncForge/internal/oplog"
	"github.com/stan-ley-tech/SyncForge/internal/record"
	"github.com/stan-ley-tech/SyncForge/internal/vector"
)

func makeOp(id, device string, vv vector.Vector, physical int64, payload string) oplog.Op {
	return oplog.Op{
		ID:            id,
		DeviceID:      device,
		Collection:    "notes",
		RecordID:      "note-1",
		Type:          oplog.Update,
		Payload:       json.RawMessage(payload),
		VersionVector: vv,
		HLC:           hlc.Timestamp{Physical: physical, Logical: 0, NodeID: device},
	}
}

func TestResolveFirstWriteAlwaysWins(t *testing.T) {
	op := makeOp("op-1", "device-a", vector.Vector{"device-a": 1}, 100, `{"v":1}`)
	d := Resolve(record.Record{}, false, op)

	if d.Conflicted {
		t.Fatalf("first write must never be flagged as a conflict")
	}
	if d.WinnerOpID != "op-1" {
		t.Fatalf("expected op-1 to win trivially, got %q", d.WinnerOpID)
	}
	if d.Record.WinningOpID != "op-1" || string(d.Record.Payload) != `{"v":1}` {
		t.Fatalf("unexpected resulting record: %+v", d.Record)
	}
}

func TestResolveFastForwardAncestor(t *testing.T) {
	existing := record.Record{
		VersionVector: vector.Vector{"device-a": 1},
		HLC:           hlc.Timestamp{Physical: 100, NodeID: "device-a"},
		WinningOpID:   "op-1",
		Payload:       json.RawMessage(`{"v":1}`),
	}
	// op-2 causally descends from op-1 (same device, incremented counter):
	// this is an ordinary sequential edit, not a conflict.
	op2 := makeOp("op-2", "device-a", vector.Vector{"device-a": 2}, 200, `{"v":2}`)

	d := Resolve(existing, true, op2)

	if d.Conflicted {
		t.Fatalf("a causal fast-forward must not be flagged as a conflict")
	}
	if d.WinnerOpID != "op-2" {
		t.Fatalf("expected op-2 to win the fast-forward, got %q", d.WinnerOpID)
	}
}

func TestResolveStaleReplayIsNoOp(t *testing.T) {
	existing := record.Record{
		VersionVector: vector.Vector{"device-a": 2},
		HLC:           hlc.Timestamp{Physical: 200, NodeID: "device-a"},
		WinningOpID:   "op-2",
		Payload:       json.RawMessage(`{"v":2}`),
	}
	// A stale op the record has already moved past (e.g. redelivered out of
	// order over an unreliable network).
	staleOp := makeOp("op-1", "device-a", vector.Vector{"device-a": 1}, 100, `{"v":1}`)

	d := Resolve(existing, true, staleOp)

	if d.Conflicted {
		t.Fatalf("a stale replay must not be flagged as a conflict")
	}
	if d.WinnerOpID != "op-2" || string(d.Record.Payload) != `{"v":2}` {
		t.Fatalf("expected the existing state to be kept unchanged, got %+v", d.Record)
	}
}

func TestResolveConcurrentWritesPickGreaterHLCDeterministically(t *testing.T) {
	existing := record.Record{
		VersionVector: vector.Vector{"device-a": 1},
		HLC:           hlc.Timestamp{Physical: 100, NodeID: "device-a"},
		WinningOpID:   "op-a",
		Payload:       json.RawMessage(`{"from":"a"}`),
	}
	// op-b never saw op-a's write (disjoint device component): concurrent.
	opB := makeOp("op-b", "device-b", vector.Vector{"device-b": 1}, 200, `{"from":"b"}`)

	d := Resolve(existing, true, opB)

	if !d.Conflicted {
		t.Fatalf("expected concurrent writes to be flagged as a conflict")
	}
	if d.WinnerOpID != "op-b" || d.LoserOpID != "op-a" {
		t.Fatalf("expected op-b (greater HLC) to win, got winner=%q loser=%q", d.WinnerOpID, d.LoserOpID)
	}
	want := vector.Vector{"device-a": 1, "device-b": 1}
	if d.Record.VersionVector.Compare(want) != vector.Equal {
		t.Fatalf("expected merged version vector %v, got %v", want, d.Record.VersionVector)
	}

	// Symmetric case: the earlier-HLC op arrives second. The existing
	// (later-HLC) state must be kept, still flagged as a conflict, and the
	// vector must still merge in the loser's component — the resolver must
	// reach the identical verdict independent of arrival order, since that
	// is what lets replicas converge no matter which order they sync in.
	existing2 := record.Record{
		VersionVector: vector.Vector{"device-b": 1},
		HLC:           hlc.Timestamp{Physical: 200, NodeID: "device-b"},
		WinningOpID:   "op-b",
		Payload:       json.RawMessage(`{"from":"b"}`),
	}
	opA := makeOp("op-a", "device-a", vector.Vector{"device-a": 1}, 100, `{"from":"a"}`)
	d2 := Resolve(existing2, true, opA)

	if !d2.Conflicted {
		t.Fatalf("expected concurrent writes to be flagged as a conflict")
	}
	if d2.WinnerOpID != "op-b" || d2.LoserOpID != "op-a" {
		t.Fatalf("expected op-b to win regardless of arrival order, got winner=%q loser=%q", d2.WinnerOpID, d2.LoserOpID)
	}
	if string(d2.Record.Payload) != string(d.Record.Payload) {
		t.Fatalf("expected identical resulting payload regardless of arrival order: %s vs %s", d2.Record.Payload, d.Record.Payload)
	}
}

func TestResolveConflictBetweenEditAndDeleteTombstone(t *testing.T) {
	existing := record.Record{
		VersionVector: vector.Vector{"device-a": 1},
		HLC:           hlc.Timestamp{Physical: 100, NodeID: "device-a"},
		WinningOpID:   "op-edit",
		Payload:       json.RawMessage(`{"v":1}`),
	}
	deleteOp := oplog.Op{
		ID:            "op-delete",
		DeviceID:      "device-b",
		Collection:    "notes",
		RecordID:      "note-1",
		Type:          oplog.Delete,
		VersionVector: vector.Vector{"device-b": 1},
		HLC:           hlc.Timestamp{Physical: 200, NodeID: "device-b"},
	}

	d := Resolve(existing, true, deleteOp)

	if !d.Conflicted {
		t.Fatalf("expected edit-vs-delete to be flagged as a conflict")
	}
	if !d.Record.Deleted || d.Record.Payload != nil {
		t.Fatalf("expected the later delete to win deterministically, got %+v", d.Record)
	}
}

// TestResolveConvergesRegardlessOfOrder is the property behind the "three
// devices, any reconnect order, same result" claim: three ops, all
// pairwise concurrent (disjoint device components), applied one at a time
// in every possible order, must converge on the identical final record —
// the one with the globally greatest HLC — every time.
func TestResolveConvergesRegardlessOfOrder(t *testing.T) {
	opA := makeOp("op-a", "device-a", vector.Vector{"device-a": 1}, 100, `{"from":"a"}`)
	opB := makeOp("op-b", "device-b", vector.Vector{"device-b": 1}, 300, `{"from":"b"}`) // greatest HLC
	opC := makeOp("op-c", "device-c", vector.Vector{"device-c": 1}, 200, `{"from":"c"}`)

	orders := [][]oplog.Op{
		{opA, opB, opC},
		{opA, opC, opB},
		{opB, opA, opC},
		{opB, opC, opA},
		{opC, opA, opB},
		{opC, opB, opA},
	}

	wantVector := vector.Vector{"device-a": 1, "device-b": 1, "device-c": 1}

	var firstResult record.Record
	for i, order := range orders {
		var existing record.Record
		existsAlready := false
		for _, op := range order {
			d := Resolve(existing, existsAlready, op)
			existing = d.Record
			existsAlready = true
		}

		if existing.WinningOpID != "op-b" {
			t.Fatalf("order %d: expected op-b (greatest HLC) to win convergence, got %q", i, existing.WinningOpID)
		}
		if existing.VersionVector.Compare(wantVector) != vector.Equal {
			t.Fatalf("order %d: expected fully-merged version vector %v, got %v", i, wantVector, existing.VersionVector)
		}
		if i == 0 {
			firstResult = existing
		} else if string(existing.Payload) != string(firstResult.Payload) {
			t.Fatalf("order %d: payload diverged from order 0: %s vs %s", i, existing.Payload, firstResult.Payload)
		}
	}
}
