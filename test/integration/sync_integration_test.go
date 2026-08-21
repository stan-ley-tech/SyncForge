// Package integration proves SyncForge's headline claim end-to-end,
// through the real HTTP server, real SQLite storage, and the real client
// SDK (no mocking of any layer): take three devices offline, have them
// independently modify the same record, reconnect them, and watch
// SyncForge deterministically resolve the conflict — the same way no
// matter what order the devices reconnect in.
package integration

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stan-ley-tech/SyncForge/internal/server"
	"github.com/stan-ley-tech/SyncForge/internal/storage/sqlite"
	"github.com/stan-ley-tech/SyncForge/pkg/api"
	"github.com/stan-ley-tech/SyncForge/pkg/client"
)

type profile struct {
	Name string `json:"name"`
}

func newIntegrationServer(t *testing.T) string {
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

func newDevice(t *testing.T, serverURL, name string) *client.Client {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), name+".db")
	c, err := client.Open(dbPath, serverURL)
	if err != nil {
		t.Fatalf("Open %s: %v", name, err)
	}
	t.Cleanup(func() { c.Close() })

	if err := c.Register(context.Background(), name); err != nil {
		t.Fatalf("Register %s: %v", name, err)
	}
	return c
}

func recordName(t *testing.T, c *client.Client, recordID string) string {
	t.Helper()
	rec, found, err := c.Get(context.Background(), "profiles", recordID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatalf("expected record %q to be present", recordID)
	}
	var p profile
	if err := json.Unmarshal(rec.Value, &p); err != nil {
		t.Fatalf("unmarshal record: %v", err)
	}
	return p.Name
}

// TestThreeDevicesConvergeRegardlessOfReconnectOrder is SyncForge's
// headline scenario:
//
//	             ┌── Device A
//	             │
//	Server ──────┼── Device B
//	             │
//	             └── Device C
//
// A, B, and C establish a shared record, then go offline and each edit it
// independently — genuinely concurrently, none having seen the others'
// change. They reconnect in every one of the 6 possible orders. In every
// case, the server and all three devices must converge on the identical
// value (the edit with the latest Hybrid Logical Clock), and the sync must
// have gone through real conflict detection, not a lucky fast-forward.
func TestThreeDevicesConvergeRegardlessOfReconnectOrder(t *testing.T) {
	orders := [][]string{
		{"A", "B", "C"},
		{"A", "C", "B"},
		{"B", "A", "C"},
		{"B", "C", "A"},
		{"C", "A", "B"},
		{"C", "B", "A"},
	}

	for _, order := range orders {
		t.Run(strings.Join(order, ""), func(t *testing.T) {
			ctx := context.Background()
			serverURL := newIntegrationServer(t)

			a := newDevice(t, serverURL, "device-a")
			b := newDevice(t, serverURL, "device-b")
			c := newDevice(t, serverURL, "device-c")
			devices := map[string]*client.Client{"A": a, "B": b, "C": c}

			// Establish a common ancestor: device A creates the record and
			// everyone syncs, so B and C's later offline edits are genuine
			// concurrent modifications of a shared starting point, not
			// independent creations.
			if err := a.Put(ctx, "profiles", "user-1", profile{Name: "initial"}); err != nil {
				t.Fatalf("A create: %v", err)
			}
			if _, err := a.Sync(ctx); err != nil {
				t.Fatalf("A initial sync: %v", err)
			}
			if _, err := b.Sync(ctx); err != nil {
				t.Fatalf("B initial sync: %v", err)
			}
			if _, err := c.Sync(ctx); err != nil {
				t.Fatalf("C initial sync: %v", err)
			}

			// All three go offline and independently edit the same
			// record. Small sleeps guarantee strictly increasing
			// wall-clock HLCs, so the expected winner (device C, which
			// edits last) is unambiguous regardless of reconnect order.
			if err := a.Put(ctx, "profiles", "user-1", profile{Name: "Alice's edit"}); err != nil {
				t.Fatalf("A offline edit: %v", err)
			}
			time.Sleep(2 * time.Millisecond)
			if err := b.Put(ctx, "profiles", "user-1", profile{Name: "Bob's edit"}); err != nil {
				t.Fatalf("B offline edit: %v", err)
			}
			time.Sleep(2 * time.Millisecond)
			if err := c.Put(ctx, "profiles", "user-1", profile{Name: "Carol's edit"}); err != nil {
				t.Fatalf("C offline edit: %v", err)
			}

			// Reconnect in this permutation's order, then a second round
			// so every device catches up on whatever it missed from a
			// device that reconnected after it in round one.
			var allConflicts []api.ConflictInfo
			for _, round := range [2][]string{order, order} {
				for _, name := range round {
					result, err := devices[name].Sync(ctx)
					if err != nil {
						t.Fatalf("Sync %s: %v", name, err)
					}
					allConflicts = append(allConflicts, result.Conflicts...)
				}
			}

			if len(allConflicts) == 0 {
				t.Fatalf("expected at least one conflict to have been detected during reconnect")
			}

			const want = "Carol's edit"
			if got := recordName(t, a, "user-1"); got != want {
				t.Fatalf("order %v: device A converged to %q, want %q", order, got, want)
			}
			if got := recordName(t, b, "user-1"); got != want {
				t.Fatalf("order %v: device B converged to %q, want %q", order, got, want)
			}
			if got := recordName(t, c, "user-1"); got != want {
				t.Fatalf("order %v: device C converged to %q, want %q", order, got, want)
			}

			// A brand new device joining afterwards, knowing nothing of
			// the history, must land on the same server-authoritative
			// value too.
			observer := newDevice(t, serverURL, "observer")
			if _, err := observer.Sync(ctx); err != nil {
				t.Fatalf("observer sync: %v", err)
			}
			if got := recordName(t, observer, "user-1"); got != want {
				t.Fatalf("order %v: a freshly-joined observer device saw %q, want %q", order, got, want)
			}
		})
	}
}

// TestThreeDevicesConcurrentDeleteVsEditConverges covers the tombstone
// side of the same claim: one device deletes a record while the other two
// concurrently edit it. All three, and the server, must still converge
// deterministically on whichever change has the latest HLC.
func TestThreeDevicesConcurrentDeleteVsEditConverges(t *testing.T) {
	ctx := context.Background()
	serverURL := newIntegrationServer(t)

	a := newDevice(t, serverURL, "device-a")
	b := newDevice(t, serverURL, "device-b")
	c := newDevice(t, serverURL, "device-c")

	if err := a.Put(ctx, "profiles", "user-1", profile{Name: "initial"}); err != nil {
		t.Fatalf("A create: %v", err)
	}
	if _, err := a.Sync(ctx); err != nil {
		t.Fatalf("A initial sync: %v", err)
	}
	if _, err := b.Sync(ctx); err != nil {
		t.Fatalf("B initial sync: %v", err)
	}
	if _, err := c.Sync(ctx); err != nil {
		t.Fatalf("C initial sync: %v", err)
	}

	if err := a.Put(ctx, "profiles", "user-1", profile{Name: "Alice's edit"}); err != nil {
		t.Fatalf("A offline edit: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := b.Delete(ctx, "profiles", "user-1"); err != nil {
		t.Fatalf("B offline delete: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	// C edits last, so C's edit should win over both A's earlier edit and
	// B's delete.
	if err := c.Put(ctx, "profiles", "user-1", profile{Name: "Carol's edit"}); err != nil {
		t.Fatalf("C offline edit: %v", err)
	}

	for round := 0; round < 2; round++ {
		for _, d := range []*client.Client{a, b, c} {
			if _, err := d.Sync(ctx); err != nil {
				t.Fatalf("Sync: %v", err)
			}
		}
	}

	for name, d := range map[string]*client.Client{"A": a, "B": b, "C": c} {
		rec, found, err := d.Get(ctx, "profiles", "user-1")
		if err != nil {
			t.Fatalf("%s Get: %v", name, err)
		}
		if !found {
			t.Fatalf("%s: expected the later edit to win over the delete, but the record reads as deleted", name)
		}
		var p profile
		if err := json.Unmarshal(rec.Value, &p); err != nil {
			t.Fatalf("%s: unmarshal: %v", name, err)
		}
		if p.Name != "Carol's edit" {
			t.Fatalf("%s converged to %q, want %q", name, p.Name, "Carol's edit")
		}
	}
}
