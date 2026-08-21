// Package record defines Record, the materialized current state of a
// synced object — the result of folding a record's operation history down
// to "what does this record look like right now." Both the server and
// every client keep one materialized Record per (collection, record id);
// sync's job is to keep them converged.
package record

import (
	"encoding/json"

	"github.com/stan-ley-tech/SyncForge/internal/hlc"
	"github.com/stan-ley-tech/SyncForge/internal/vector"
)

// Record is the current materialized state of one synced object.
type Record struct {
	Collection string
	RecordID   string

	// Payload is the current value as JSON. Nil when Deleted is true.
	Payload json.RawMessage
	Deleted bool // tombstone: the record existed and was deleted

	// VersionVector and HLC reflect the write that produced this state
	// (the winner of any conflict resolution that occurred).
	VersionVector vector.Vector
	HLC           hlc.Timestamp

	// WinningOpID is the id of the op currently reflected in this record.
	WinningOpID string
}
