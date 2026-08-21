// Package api defines SyncForge's REST wire protocol: the JSON request and
// response shapes exchanged between the client SDK and the server, plus
// conversions to and from the internal oplog/vector/hlc domain types.
package api

import (
	"encoding/json"
	"fmt"

	"github.com/stan-ley-tech/SyncForge/internal/hlc"
	"github.com/stan-ley-tech/SyncForge/internal/oplog"
	"github.com/stan-ley-tech/SyncForge/internal/vector"
)

// HLC is the wire form of a hybrid logical clock timestamp.
type HLC struct {
	Physical int64  `json:"physical"`
	Logical  uint32 `json:"logical"`
	NodeID   string `json:"node_id"`
}

func hlcToWire(t hlc.Timestamp) HLC {
	return HLC{Physical: t.Physical, Logical: t.Logical, NodeID: t.NodeID}
}

func (h HLC) toDomain() hlc.Timestamp {
	return hlc.Timestamp{Physical: h.Physical, Logical: h.Logical, NodeID: h.NodeID}
}

// Op is the wire form of an oplog.Op.
type Op struct {
	ID            string            `json:"id"`
	DeviceID      string            `json:"device_id"`
	Collection    string            `json:"collection"`
	RecordID      string            `json:"record_id"`
	Type          string            `json:"type"`
	Payload       json.RawMessage   `json:"payload,omitempty"`
	VersionVector map[string]uint64 `json:"version_vector"`
	HLC           HLC               `json:"hlc"`
	ServerSeq     int64             `json:"server_seq,omitempty"`
}

// FromOplog converts a domain oplog.Op into its wire form.
func FromOplog(op oplog.Op) Op {
	return Op{
		ID:            op.ID,
		DeviceID:      op.DeviceID,
		Collection:    op.Collection,
		RecordID:      op.RecordID,
		Type:          string(op.Type),
		Payload:       op.Payload,
		VersionVector: map[string]uint64(op.VersionVector),
		HLC:           hlcToWire(op.HLC),
		ServerSeq:     op.ServerSeq,
	}
}

// ToOplog converts a wire Op back into the domain type, validating it.
func (o Op) ToOplog() (oplog.Op, error) {
	t := oplog.Type(o.Type)
	if t != oplog.Create && t != oplog.Update && t != oplog.Delete {
		return oplog.Op{}, fmt.Errorf("api: invalid op type %q", o.Type)
	}
	op := oplog.Op{
		ID:            o.ID,
		DeviceID:      o.DeviceID,
		Collection:    o.Collection,
		RecordID:      o.RecordID,
		Type:          t,
		Payload:       o.Payload,
		VersionVector: vector.Vector(o.VersionVector),
		HLC:           o.HLC.toDomain(),
		ServerSeq:     o.ServerSeq,
	}
	if err := op.Validate(); err != nil {
		return oplog.Op{}, err
	}
	return op, nil
}

// RegisterDeviceRequest is the body of POST /v1/devices/register.
type RegisterDeviceRequest struct {
	DeviceName string `json:"device_name"`
}

// RegisterDeviceResponse is returned once, at registration time — the
// token is not recoverable afterwards (the server only stores its hash).
type RegisterDeviceResponse struct {
	DeviceID    string `json:"device_id"`
	DeviceToken string `json:"device_token"`
}

// PushRequest is the body of POST /v1/sync/push.
type PushRequest struct {
	Ops []Op `json:"ops"`
}

// ConflictInfo reports one conflict detected while applying a push.
type ConflictInfo struct {
	Collection  string `json:"collection"`
	RecordID    string `json:"record_id"`
	WinningOpID string `json:"winning_op_id"`
	LosingOpID  string `json:"losing_op_id"`
}

// PushResponse is returned from POST /v1/sync/push. Accepted lists every
// op id that is now durably stored server-side, whether this call inserted
// it or it was already there from a previous, retried attempt — that
// symmetry is what makes push idempotent from the client's perspective.
type PushResponse struct {
	Accepted         []string       `json:"accepted"`
	Conflicts        []ConflictInfo `json:"conflicts"`
	ServerCheckpoint int64          `json:"server_checkpoint"`
}

// PullResponse is returned from GET /v1/sync/pull.
type PullResponse struct {
	Ops            []Op  `json:"ops"`
	NextCheckpoint int64 `json:"next_checkpoint"`
	HasMore        bool  `json:"has_more"`
}

// StatusResponse is returned from GET /v1/sync/status.
type StatusResponse struct {
	DeviceID         string `json:"device_id"`
	ServerCheckpoint int64  `json:"server_checkpoint"`
	ConflictCount    int    `json:"conflict_count"`
}

// RecordResponse is returned from GET /v1/records/{collection}/{id}.
type RecordResponse struct {
	Collection    string            `json:"collection"`
	RecordID      string            `json:"record_id"`
	Payload       json.RawMessage   `json:"payload,omitempty"`
	Deleted       bool              `json:"deleted"`
	VersionVector map[string]uint64 `json:"version_vector"`
}

// ErrorResponse is the body of any non-2xx response.
type ErrorResponse struct {
	Error string `json:"error"`
}
