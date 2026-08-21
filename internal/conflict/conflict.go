// Package conflict implements SyncForge's deterministic conflict
// resolution: the single function, shared by the server and every client,
// that decides what a record looks like after a new op is applied on top
// of whatever state is already there.
//
// Determinism comes from two properties working together:
//
//  1. Version vectors detect *whether* there is a conflict at all. If the
//     incoming op's vector is a causal descendant of the current record
//     (or vice versa), there is no conflict — one write simply happened
//     after the other, so the later one wins outright.
//  2. When the vectors are Concurrent — neither write saw the other — the
//     op with the strictly greater Hybrid Logical Clock timestamp wins.
//     HLC.Compare is a total order (ties are broken by node id), so
//     "greatest HLC wins" is commutative and associative: folding any
//     number of pairwise-concurrent writes through Resolve, in any order,
//     converges on the single globally-greatest one. That is what lets
//     three independently-offline devices reconnect in any order and
//     still agree.
//
// The losing write is never discarded: callers are expected to keep it in
// the immutable oplog and record the conflict (see storage/sqlite.Conflict)
// for audit purposes. Resolve only decides what the materialized Record
// should become.
package conflict

import (
	"github.com/stan-ley-tech/SyncForge/internal/oplog"
	"github.com/stan-ley-tech/SyncForge/internal/record"
	"github.com/stan-ley-tech/SyncForge/internal/vector"
)

// Decision is the result of resolving one op against the current state of
// a record.
type Decision struct {
	// Record is the new materialized state.
	Record record.Record

	// Conflicted is true if op's version vector was Concurrent with the
	// prior record state — i.e. a genuine conflict was detected and
	// resolved, as opposed to a plain causal fast-forward or a stale
	// replay.
	Conflicted bool

	// WinnerOpID and LoserOpID identify the two sides of a conflict.
	// LoserOpID is empty unless Conflicted is true.
	WinnerOpID string
	LoserOpID  string
}

// Resolve applies op on top of the current record state. existsAlready
// must be false if no op has ever touched this (collection, recordID)
// before (op then wins trivially, as the first write).
func Resolve(existing record.Record, existsAlready bool, op oplog.Op) Decision {
	if !existsAlready {
		return Decision{Record: fromOp(op), Conflicted: false, WinnerOpID: op.ID}
	}

	switch existing.VersionVector.Compare(op.VersionVector) {
	case vector.Equal, vector.Descendant:
		// op is a stale or already-seen write: the record already reflects
		// everything op knows about (or more). No-op.
		return Decision{Record: existing, Conflicted: false, WinnerOpID: existing.WinningOpID}

	case vector.Ancestor:
		// op strictly advances the record's causal history: a plain
		// fast-forward, not a conflict.
		return Decision{Record: fromOp(op), Conflicted: false, WinnerOpID: op.ID}

	default: // vector.Concurrent
		merged := vector.Merge(existing.VersionVector, op.VersionVector)
		if op.HLC.After(existing.HLC) {
			winner := fromOp(op)
			winner.VersionVector = merged
			return Decision{
				Record:     winner,
				Conflicted: true,
				WinnerOpID: op.ID,
				LoserOpID:  existing.WinningOpID,
			}
		}
		loser := existing
		loser.VersionVector = merged
		return Decision{
			Record:     loser,
			Conflicted: true,
			WinnerOpID: existing.WinningOpID,
			LoserOpID:  op.ID,
		}
	}
}

func fromOp(op oplog.Op) record.Record {
	return record.Record{
		Collection:    op.Collection,
		RecordID:      op.RecordID,
		Payload:       op.Payload,
		Deleted:       op.Type == oplog.Delete,
		VersionVector: op.VersionVector,
		HLC:           op.HLC,
		WinningOpID:   op.ID,
	}
}
