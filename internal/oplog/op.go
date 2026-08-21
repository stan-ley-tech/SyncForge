// Package oplog defines the operation log entry: the single append-only
// unit that both the client and the server exchange during sync. Every
// change to a record — create, update, or delete — is represented as one
// Op, identified by a client-generated id that doubles as an idempotency
// key.
package oplog

import (
	"encoding/json"
	"errors"

	"github.com/stan-ley-tech/SyncForge/internal/hlc"
	"github.com/stan-ley-tech/SyncForge/internal/vector"
)

// Type identifies what kind of change an Op represents.
type Type string

const (
	Create Type = "create"
	Update Type = "update"
	Delete Type = "delete"
)

// Op is one entry in an operation log: a single, self-contained change to
// one record.
type Op struct {
	// ID is a client-generated UUID. It is the idempotency key: pushing the
	// same Op twice (e.g. after a retry) must have no additional effect.
	ID string

	// DeviceID is the device that originated this op.
	DeviceID string

	// Collection and RecordID identify the record being changed. SyncForge
	// is data-model agnostic: Collection is just an application-chosen
	// namespace (e.g. "notes", "contacts").
	Collection string
	RecordID   string

	Type Type

	// Payload is the full record snapshot as JSON. Conflict resolution
	// operates on whole records, not field-level diffs. Payload is nil for
	// Delete ops.
	Payload json.RawMessage

	// VersionVector is the record's version vector as of this write
	// (i.e. after the originating device incremented its own component).
	VersionVector vector.Vector

	// HLC is the hybrid logical clock timestamp of this write. It is the
	// deterministic tiebreaker when two ops are concurrent.
	HLC hlc.Timestamp

	// ServerSeq is the monotonic checkpoint the server assigned this op
	// when it was first accepted. It is 0 for an op that has not yet been
	// pushed to (and accepted by) the server.
	ServerSeq int64
}

// Validate checks that an Op has the fields required to be stored or
// transmitted.
func (o Op) Validate() error {
	switch {
	case o.ID == "":
		return errors.New("oplog: missing op id")
	case o.DeviceID == "":
		return errors.New("oplog: missing device id")
	case o.Collection == "":
		return errors.New("oplog: missing collection")
	case o.RecordID == "":
		return errors.New("oplog: missing record id")
	case o.Type != Create && o.Type != Update && o.Type != Delete:
		return errors.New("oplog: invalid op type")
	case o.Type != Delete && o.Payload == nil:
		return errors.New("oplog: missing payload for non-delete op")
	case o.HLC.NodeID == "":
		return errors.New("oplog: missing hlc timestamp")
	}
	return nil
}
