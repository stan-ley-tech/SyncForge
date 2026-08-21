package client

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/stan-ley-tech/SyncForge/internal/server"
	"github.com/stan-ley-tech/SyncForge/internal/storage/sqlite"
)

func newTestClient(t *testing.T, serverURL string) *Client {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "client.db")
	c, err := Open(dbPath, serverURL)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func newTestServerURL(t *testing.T) string {
	t.Helper()
	db, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	srv := httptest.NewServer(server.New(db).Handler())
	t.Cleanup(srv.Close)
	return srv.URL
}

type note struct {
	Title string `json:"title"`
}

func TestLocalCRUDWorksFullyOffline(t *testing.T) {
	ctx := context.Background()
	// No server ever contacted: DeviceID must still be assigned locally,
	// and Put/Get/List/Delete must all work without ever calling Sync.
	c := newTestClient(t, "http://unused.invalid")

	if c.DeviceID() == "" {
		t.Fatalf("expected a locally-assigned device id")
	}

	if err := c.Put(ctx, "notes", "n1", note{Title: "hello"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	rec, found, err := c.Get(ctx, "notes", "n1")
	if err != nil || !found {
		t.Fatalf("Get: found=%v err=%v", found, err)
	}
	var got note
	if err := json.Unmarshal(rec.Value, &got); err != nil || got.Title != "hello" {
		t.Fatalf("unexpected record value: %s (err=%v)", rec.Value, err)
	}

	if err := c.Put(ctx, "notes", "n2", note{Title: "second"}); err != nil {
		t.Fatalf("Put n2: %v", err)
	}
	list, err := c.List(ctx, "notes")
	if err != nil || len(list) != 2 {
		t.Fatalf("List: got %d records, err=%v", len(list), err)
	}

	pending, err := c.PendingChanges(ctx)
	if err != nil || pending != 2 {
		t.Fatalf("expected 2 pending changes, got %d (err=%v)", pending, err)
	}

	if err := c.Delete(ctx, "notes", "n1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, found, err = c.Get(ctx, "notes", "n1")
	if err != nil || found {
		t.Fatalf("expected n1 to be gone after Delete, found=%v err=%v", found, err)
	}
	list, err = c.List(ctx, "notes")
	if err != nil || len(list) != 1 {
		t.Fatalf("expected List to exclude the deleted record, got %d", len(list))
	}
}

func TestSyncRequiresRegistration(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(t, newTestServerURL(t))
	if err := c.Put(ctx, "notes", "n1", note{Title: "hi"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := c.Sync(ctx); err == nil {
		t.Fatalf("expected Sync before Register to fail")
	}
}

func TestSyncPushesLocalWritesToServer(t *testing.T) {
	ctx := context.Background()
	serverURL := newTestServerURL(t)
	c := newTestClient(t, serverURL)

	if err := c.Register(ctx, "Device A"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := c.Put(ctx, "notes", "n1", note{Title: "hello"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	result, err := c.Sync(ctx)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if result.Pushed != 1 {
		t.Fatalf("expected 1 pushed op, got %d", result.Pushed)
	}
	if len(result.Conflicts) != 0 {
		t.Fatalf("expected no conflicts, got %+v", result.Conflicts)
	}

	pending, err := c.PendingChanges(ctx)
	if err != nil || pending != 0 {
		t.Fatalf("expected no pending changes after Sync, got %d (err=%v)", pending, err)
	}
}

func TestSyncPullsChangesFromAnotherDevice(t *testing.T) {
	ctx := context.Background()
	serverURL := newTestServerURL(t)

	a := newTestClient(t, serverURL)
	if err := a.Register(ctx, "Device A"); err != nil {
		t.Fatalf("Register A: %v", err)
	}
	if err := a.Put(ctx, "notes", "shared", note{Title: "from A"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := a.Sync(ctx); err != nil {
		t.Fatalf("Sync A: %v", err)
	}

	b := newTestClient(t, serverURL)
	if err := b.Register(ctx, "Device B"); err != nil {
		t.Fatalf("Register B: %v", err)
	}

	// Before syncing, device B has never heard of this record.
	if _, found, err := b.Get(ctx, "notes", "shared"); err != nil || found {
		t.Fatalf("expected device B not to have the record yet, found=%v err=%v", found, err)
	}

	result, err := b.Sync(ctx)
	if err != nil {
		t.Fatalf("Sync B: %v", err)
	}
	if result.Pulled != 1 {
		t.Fatalf("expected device B to pull 1 op, got %d", result.Pulled)
	}

	rec, found, err := b.Get(ctx, "notes", "shared")
	if err != nil || !found {
		t.Fatalf("expected device B to now have the record, found=%v err=%v", found, err)
	}
	var got note
	json.Unmarshal(rec.Value, &got)
	if got.Title != "from A" {
		t.Fatalf("expected device B's copy to match device A's write, got %q", got.Title)
	}
}

func TestSyncResolvesConflictIdenticallyOnBothDevices(t *testing.T) {
	ctx := context.Background()
	serverURL := newTestServerURL(t)

	a := newTestClient(t, serverURL)
	b := newTestClient(t, serverURL)
	if err := a.Register(ctx, "Device A"); err != nil {
		t.Fatalf("Register A: %v", err)
	}
	if err := b.Register(ctx, "Device B"); err != nil {
		t.Fatalf("Register B: %v", err)
	}

	// Both devices independently create the same record while "offline"
	// (neither has synced yet, so neither has seen the other's write).
	if err := a.Put(ctx, "notes", "shared", note{Title: "from A"}); err != nil {
		t.Fatalf("Put A: %v", err)
	}
	if err := b.Put(ctx, "notes", "shared", note{Title: "from B"}); err != nil {
		t.Fatalf("Put B: %v", err)
	}

	if _, err := a.Sync(ctx); err != nil {
		t.Fatalf("Sync A: %v", err)
	}
	resultB, err := b.Sync(ctx)
	if err != nil {
		t.Fatalf("Sync B: %v", err)
	}
	// The same underlying conflict is independently detected twice within
	// one Sync() call: once server-side when B pushes op-B against the
	// already-stored op-A, and once again client-side when B's pull phase
	// (in the same call) applies op-A against its own already-applied
	// op-B. Both detections must agree on the winner/loser — that
	// agreement, not the count, is the determinism property under test.
	if len(resultB.Conflicts) == 0 {
		t.Fatalf("expected device B's sync to surface at least one detected conflict, got none")
	}
	for _, c := range resultB.Conflicts {
		if c.WinningOpID != resultB.Conflicts[0].WinningOpID || c.LosingOpID != resultB.Conflicts[0].LosingOpID {
			t.Fatalf("expected every conflict report to agree on winner/loser, got %+v", resultB.Conflicts)
		}
	}

	// A needs one more sync to learn about B's write (and the conflict).
	resultA2, err := a.Sync(ctx)
	if err != nil {
		t.Fatalf("second Sync A: %v", err)
	}
	if resultA2.Pulled == 0 {
		t.Fatalf("expected device A's second sync to pull B's op")
	}

	recA, _, _ := a.Get(ctx, "notes", "shared")
	recB, _, _ := b.Get(ctx, "notes", "shared")
	if string(recA.Value) != string(recB.Value) {
		t.Fatalf("expected both devices to converge on the same value, got A=%s B=%s", recA.Value, recB.Value)
	}
}
