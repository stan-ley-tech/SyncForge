package sqlite

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stan-ley-tech/SyncForge/internal/hlc"
	"github.com/stan-ley-tech/SyncForge/internal/oplog"
	"github.com/stan-ley-tech/SyncForge/internal/record"
	"github.com/stan-ley-tech/SyncForge/internal/vector"
)

func openTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func sampleOp(id, deviceID string, seqHint uint32) oplog.Op {
	return oplog.Op{
		ID:            id,
		DeviceID:      deviceID,
		Collection:    "notes",
		RecordID:      "note-1",
		Type:          oplog.Create,
		Payload:       json.RawMessage(`{"title":"hello"}`),
		VersionVector: vector.Vector{deviceID: 1},
		HLC:           hlc.Timestamp{Physical: 1_000, Logical: seqHint, NodeID: deviceID},
	}
}

func TestAppendOpAssignsServerSeqAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	op := sampleOp("op-1", "device-a", 0)

	stored1, inserted1, err := db.AppendOp(ctx, op, true)
	if err != nil {
		t.Fatalf("AppendOp: %v", err)
	}
	if !inserted1 {
		t.Fatalf("expected first AppendOp to insert")
	}
	if stored1.ServerSeq != 1 {
		t.Fatalf("expected first server_seq = 1, got %d", stored1.ServerSeq)
	}

	// Retry with the same op id (as a client retrying after a network
	// error would) must not insert a second row or assign a new seq.
	stored2, inserted2, err := db.AppendOp(ctx, op, true)
	if err != nil {
		t.Fatalf("AppendOp retry: %v", err)
	}
	if inserted2 {
		t.Fatalf("expected retried AppendOp to be a no-op (idempotent)")
	}
	if stored2.ServerSeq != stored1.ServerSeq {
		t.Fatalf("expected idempotent replay to return the original server_seq, got %d want %d", stored2.ServerSeq, stored1.ServerSeq)
	}

	next := sampleOp("op-2", "device-a", 1)
	stored3, _, err := db.AppendOp(ctx, next, true)
	if err != nil {
		t.Fatalf("AppendOp second op: %v", err)
	}
	if stored3.ServerSeq != 2 {
		t.Fatalf("expected monotonically increasing server_seq, got %d", stored3.ServerSeq)
	}
}

func TestAppendOpWithoutAssignSeqKeepsGivenValue(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	op := sampleOp("op-1", "device-a", 0)
	op.ServerSeq = 0 // purely local, not yet pushed

	stored, _, err := db.AppendOp(ctx, op, false)
	if err != nil {
		t.Fatalf("AppendOp: %v", err)
	}
	if stored.ServerSeq != 0 {
		t.Fatalf("expected client-side AppendOp to leave ServerSeq at 0, got %d", stored.ServerSeq)
	}
}

func TestOpsSincePagination(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	for i := 0; i < 5; i++ {
		op := sampleOp("op-"+string(rune('a'+i)), "device-a", uint32(i))
		if _, _, err := db.AppendOp(ctx, op, true); err != nil {
			t.Fatalf("AppendOp %d: %v", i, err)
		}
	}

	page1, next1, more1, err := db.OpsSince(ctx, 0, "", 2)
	if err != nil {
		t.Fatalf("OpsSince page1: %v", err)
	}
	if len(page1) != 2 || !more1 {
		t.Fatalf("expected page1 of 2 with more=true, got %d ops more=%v", len(page1), more1)
	}
	if next1 != page1[len(page1)-1].ServerSeq {
		t.Fatalf("expected nextCheckpoint to be the last returned server_seq")
	}

	page2, next2, more2, err := db.OpsSince(ctx, next1, "", 2)
	if err != nil {
		t.Fatalf("OpsSince page2: %v", err)
	}
	if len(page2) != 2 || !more2 {
		t.Fatalf("expected page2 of 2 with more=true, got %d ops more=%v", len(page2), more2)
	}

	page3, _, more3, err := db.OpsSince(ctx, next2, "", 2)
	if err != nil {
		t.Fatalf("OpsSince page3: %v", err)
	}
	if len(page3) != 1 || more3 {
		t.Fatalf("expected final page of 1 with more=false, got %d ops more=%v", len(page3), more3)
	}
}

func TestUnpushedOpsAndMarkPushed(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	local := sampleOp("op-local", "device-a", 0)
	local.ServerSeq = 0
	if _, _, err := db.AppendOp(ctx, local, false); err != nil {
		t.Fatalf("AppendOp local: %v", err)
	}

	remote := sampleOp("op-remote", "device-b", 0)
	remote.ServerSeq = 7 // as if pulled from the server
	if _, _, err := db.AppendOp(ctx, remote, false); err != nil {
		t.Fatalf("AppendOp remote: %v", err)
	}

	unpushed, err := db.UnpushedOps(ctx, "device-a")
	if err != nil {
		t.Fatalf("UnpushedOps: %v", err)
	}
	if len(unpushed) != 1 || unpushed[0].ID != "op-local" {
		t.Fatalf("expected only device-a's own unpushed op, got %+v", unpushed)
	}

	if err := db.MarkPushed(ctx, []string{"op-local"}); err != nil {
		t.Fatalf("MarkPushed: %v", err)
	}
	unpushed, err = db.UnpushedOps(ctx, "device-a")
	if err != nil {
		t.Fatalf("UnpushedOps after MarkPushed: %v", err)
	}
	if len(unpushed) != 0 {
		t.Fatalf("expected no unpushed ops after MarkPushed, got %+v", unpushed)
	}
}

func TestRecordUpsertAndList(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	rec := record.Record{
		Collection:    "notes",
		RecordID:      "note-1",
		Payload:       json.RawMessage(`{"title":"v1"}`),
		VersionVector: vector.Vector{"device-a": 1},
		HLC:           hlc.Timestamp{Physical: 1, Logical: 0, NodeID: "device-a"},
		WinningOpID:   "op-1",
	}
	if err := db.PutRecord(ctx, rec); err != nil {
		t.Fatalf("PutRecord: %v", err)
	}

	got, found, err := db.GetRecord(ctx, "notes", "note-1")
	if err != nil {
		t.Fatalf("GetRecord: %v", err)
	}
	if !found {
		t.Fatalf("expected record to be found")
	}
	if string(got.Payload) != string(rec.Payload) {
		t.Fatalf("payload mismatch: got %s want %s", got.Payload, rec.Payload)
	}
	if got.VersionVector["device-a"] != 1 {
		t.Fatalf("version vector not persisted correctly: %v", got.VersionVector)
	}

	rec.Payload = json.RawMessage(`{"title":"v2"}`)
	rec.WinningOpID = "op-2"
	if err := db.PutRecord(ctx, rec); err != nil {
		t.Fatalf("PutRecord update: %v", err)
	}
	got, _, err = db.GetRecord(ctx, "notes", "note-1")
	if err != nil {
		t.Fatalf("GetRecord after update: %v", err)
	}
	if string(got.Payload) != `{"title":"v2"}` {
		t.Fatalf("expected upsert to overwrite payload, got %s", got.Payload)
	}

	list, err := db.ListRecords(ctx, "notes")
	if err != nil {
		t.Fatalf("ListRecords: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 record, got %d", len(list))
	}

	_, found, err = db.GetRecord(ctx, "notes", "does-not-exist")
	if err != nil {
		t.Fatalf("GetRecord missing: %v", err)
	}
	if found {
		t.Fatalf("expected missing record to report found=false")
	}
}

func TestDeviceRegistryRoundTrip(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	dev := Device{DeviceID: "device-a", Name: "Laptop", TokenHash: "hash123", CreatedAt: time.Now()}
	if err := db.CreateDevice(ctx, dev); err != nil {
		t.Fatalf("CreateDevice: %v", err)
	}

	byID, found, err := db.DeviceByID(ctx, "device-a")
	if err != nil || !found {
		t.Fatalf("DeviceByID: found=%v err=%v", found, err)
	}
	if byID.Name != "Laptop" {
		t.Fatalf("unexpected device name: %q", byID.Name)
	}

	byToken, found, err := db.DeviceByTokenHash(ctx, "hash123")
	if err != nil || !found {
		t.Fatalf("DeviceByTokenHash: found=%v err=%v", found, err)
	}
	if byToken.DeviceID != "device-a" {
		t.Fatalf("unexpected device id: %q", byToken.DeviceID)
	}

	_, found, err = db.DeviceByID(ctx, "does-not-exist")
	if err != nil {
		t.Fatalf("DeviceByID missing: %v", err)
	}
	if found {
		t.Fatalf("expected missing device to report found=false")
	}
}

func TestKVRoundTrip(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	_, found, err := db.GetKV(ctx, "last_checkpoint")
	if err != nil {
		t.Fatalf("GetKV missing: %v", err)
	}
	if found {
		t.Fatalf("expected missing key to report found=false")
	}

	if err := db.SetKV(ctx, "last_checkpoint", "42"); err != nil {
		t.Fatalf("SetKV: %v", err)
	}
	value, found, err := db.GetKV(ctx, "last_checkpoint")
	if err != nil || !found || value != "42" {
		t.Fatalf("GetKV: value=%q found=%v err=%v", value, found, err)
	}

	if err := db.SetKV(ctx, "last_checkpoint", "43"); err != nil {
		t.Fatalf("SetKV overwrite: %v", err)
	}
	value, _, err = db.GetKV(ctx, "last_checkpoint")
	if err != nil || value != "43" {
		t.Fatalf("expected SetKV to overwrite existing value, got %q err=%v", value, err)
	}
}

func TestConflictAudit(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	c := Conflict{
		Collection:    "notes",
		RecordID:      "note-1",
		WinningOpID:   "op-2",
		LosingOpID:    "op-1",
		DetectedAtSeq: 5,
		DetectedAt:    time.Now(),
	}
	if err := db.RecordConflict(ctx, c); err != nil {
		t.Fatalf("RecordConflict: %v", err)
	}

	list, err := db.ListConflicts(ctx)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 conflict, got %d", len(list))
	}
	if list[0].WinningOpID != "op-2" || list[0].LosingOpID != "op-1" {
		t.Fatalf("unexpected conflict contents: %+v", list[0])
	}
}
