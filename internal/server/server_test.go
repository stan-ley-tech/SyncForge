package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stan-ley-tech/SyncForge/internal/storage/sqlite"
	"github.com/stan-ley-tech/SyncForge/pkg/api"
)

func newTestServer(t *testing.T) (*httptest.Server, *sqlite.DB) {
	t.Helper()
	db, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	srv := httptest.NewServer(New(db).Handler())
	t.Cleanup(srv.Close)
	return srv, db
}

func doJSON(t *testing.T, method, url, token string, body, out any) *http.Response {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(b)
	} else {
		reader = bytes.NewReader(nil)
	}

	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	if out != nil {
		defer resp.Body.Close()
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("decode response: %v", err)
		}
	}
	return resp
}

func registerDevice(t *testing.T, baseURL, name string) api.RegisterDeviceResponse {
	t.Helper()
	var out api.RegisterDeviceResponse
	resp := doJSON(t, http.MethodPost, baseURL+"/v1/devices/register", "", api.RegisterDeviceRequest{DeviceName: name}, &out)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("register device: expected 201, got %d", resp.StatusCode)
	}
	if out.DeviceID == "" || out.DeviceToken == "" {
		t.Fatalf("register device: expected non-empty id/token, got %+v", out)
	}
	return out
}

func TestRegisterDevice(t *testing.T) {
	srv, _ := newTestServer(t)
	dev := registerDevice(t, srv.URL, "Laptop")
	if dev.DeviceID == "" {
		t.Fatalf("expected a device id")
	}
}

func TestPushRequiresAuth(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := doJSON(t, http.MethodPost, srv.URL+"/v1/sync/push", "", api.PushRequest{}, nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without a token, got %d", resp.StatusCode)
	}

	resp = doJSON(t, http.MethodPost, srv.URL+"/v1/sync/push", "not-a-real-token", api.PushRequest{}, nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 with an invalid token, got %d", resp.StatusCode)
	}
}

func makeWireOp(id, deviceID string, physical int64) api.Op {
	return api.Op{
		ID:            id,
		DeviceID:      deviceID,
		Collection:    "notes",
		RecordID:      "note-1",
		Type:          "create",
		Payload:       json.RawMessage(`{"title":"hello"}`),
		VersionVector: map[string]uint64{deviceID: 1},
		HLC:           api.HLC{Physical: physical, Logical: 0, NodeID: deviceID},
	}
}

func TestPushIsIdempotentAndAssignsCheckpoints(t *testing.T) {
	srv, _ := newTestServer(t)
	dev := registerDevice(t, srv.URL, "Laptop")

	op := makeWireOp("op-1", dev.DeviceID, 100)

	var first api.PushResponse
	resp := doJSON(t, http.MethodPost, srv.URL+"/v1/sync/push", dev.DeviceToken, api.PushRequest{Ops: []api.Op{op}}, &first)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if len(first.Accepted) != 1 || first.Accepted[0] != "op-1" {
		t.Fatalf("expected op-1 accepted, got %+v", first.Accepted)
	}
	if first.ServerCheckpoint != 1 {
		t.Fatalf("expected checkpoint 1 after first push, got %d", first.ServerCheckpoint)
	}

	// Retry the exact same push (as a client would after a dropped
	// response): must be acknowledged again without bumping the checkpoint.
	var second api.PushResponse
	doJSON(t, http.MethodPost, srv.URL+"/v1/sync/push", dev.DeviceToken, api.PushRequest{Ops: []api.Op{op}}, &second)
	if second.ServerCheckpoint != 1 {
		t.Fatalf("expected retried push to leave checkpoint at 1, got %d", second.ServerCheckpoint)
	}
	if len(second.Accepted) != 1 || second.Accepted[0] != "op-1" {
		t.Fatalf("expected retried push to still acknowledge op-1, got %+v", second.Accepted)
	}
}

func TestPushRejectsSpoofedDeviceID(t *testing.T) {
	srv, _ := newTestServer(t)
	dev := registerDevice(t, srv.URL, "Laptop")

	op := makeWireOp("op-1", "someone-elses-device", 100)
	resp := doJSON(t, http.MethodPost, srv.URL+"/v1/sync/push", dev.DeviceToken, api.PushRequest{Ops: []api.Op{op}}, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for a spoofed device_id, got %d", resp.StatusCode)
	}
}

func TestPushDetectsConflictBetweenTwoDevices(t *testing.T) {
	srv, _ := newTestServer(t)
	a := registerDevice(t, srv.URL, "Device A")
	b := registerDevice(t, srv.URL, "Device B")

	opA := api.Op{
		ID: "op-a", DeviceID: a.DeviceID, Collection: "notes", RecordID: "note-1", Type: "create",
		Payload: json.RawMessage(`{"from":"a"}`), VersionVector: map[string]uint64{a.DeviceID: 1},
		HLC: api.HLC{Physical: 100, NodeID: a.DeviceID},
	}
	opB := api.Op{
		ID: "op-b", DeviceID: b.DeviceID, Collection: "notes", RecordID: "note-1", Type: "create",
		Payload: json.RawMessage(`{"from":"b"}`), VersionVector: map[string]uint64{b.DeviceID: 1},
		HLC: api.HLC{Physical: 200, NodeID: b.DeviceID}, // strictly greater HLC: wins
	}

	doJSON(t, http.MethodPost, srv.URL+"/v1/sync/push", a.DeviceToken, api.PushRequest{Ops: []api.Op{opA}}, nil)

	var pushB api.PushResponse
	doJSON(t, http.MethodPost, srv.URL+"/v1/sync/push", b.DeviceToken, api.PushRequest{Ops: []api.Op{opB}}, &pushB)

	if len(pushB.Conflicts) != 1 {
		t.Fatalf("expected exactly one conflict reported, got %+v", pushB.Conflicts)
	}
	if pushB.Conflicts[0].WinningOpID != "op-b" || pushB.Conflicts[0].LosingOpID != "op-a" {
		t.Fatalf("expected op-b to win deterministically, got %+v", pushB.Conflicts[0])
	}

	var rec api.RecordResponse
	resp := doJSON(t, http.MethodGet, srv.URL+"/v1/records/notes/note-1", a.DeviceToken, nil, &rec)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 fetching the record, got %d", resp.StatusCode)
	}
	if string(rec.Payload) != `{"from":"b"}` {
		t.Fatalf("expected the record to reflect the winning op, got %s", rec.Payload)
	}
}

func TestPullIsIncrementalAndPartial(t *testing.T) {
	srv, _ := newTestServer(t)
	a := registerDevice(t, srv.URL, "Device A")

	ops := []api.Op{
		{ID: "op-1", DeviceID: a.DeviceID, Collection: "notes", RecordID: "n1", Type: "create",
			Payload: json.RawMessage(`{}`), VersionVector: map[string]uint64{a.DeviceID: 1}, HLC: api.HLC{Physical: 1, NodeID: a.DeviceID}},
		{ID: "op-2", DeviceID: a.DeviceID, Collection: "contacts", RecordID: "c1", Type: "create",
			Payload: json.RawMessage(`{}`), VersionVector: map[string]uint64{a.DeviceID: 1}, HLC: api.HLC{Physical: 2, NodeID: a.DeviceID}},
		{ID: "op-3", DeviceID: a.DeviceID, Collection: "notes", RecordID: "n2", Type: "create",
			Payload: json.RawMessage(`{}`), VersionVector: map[string]uint64{a.DeviceID: 1}, HLC: api.HLC{Physical: 3, NodeID: a.DeviceID}},
	}
	doJSON(t, http.MethodPost, srv.URL+"/v1/sync/push", a.DeviceToken, api.PushRequest{Ops: ops}, nil)

	var all api.PullResponse
	doJSON(t, http.MethodGet, srv.URL+"/v1/sync/pull?since=0", a.DeviceToken, nil, &all)
	if len(all.Ops) != 3 || all.HasMore {
		t.Fatalf("expected all 3 ops in one page, got %d (hasMore=%v)", len(all.Ops), all.HasMore)
	}

	var notesOnly api.PullResponse
	doJSON(t, http.MethodGet, srv.URL+"/v1/sync/pull?since=0&collection=notes", a.DeviceToken, nil, &notesOnly)
	if len(notesOnly.Ops) != 2 {
		t.Fatalf("expected partial sync to return only the 2 notes ops, got %d", len(notesOnly.Ops))
	}

	var page1 api.PullResponse
	doJSON(t, http.MethodGet, srv.URL+"/v1/sync/pull?since=0&limit=1", a.DeviceToken, nil, &page1)
	if len(page1.Ops) != 1 || !page1.HasMore {
		t.Fatalf("expected a 1-op page with more remaining, got %d ops hasMore=%v", len(page1.Ops), page1.HasMore)
	}

	var page2 api.PullResponse
	doJSON(t, http.MethodGet, srv.URL+"/v1/sync/pull?since="+strconv.FormatInt(page1.NextCheckpoint, 10)+"&limit=1", a.DeviceToken, nil, &page2)
	if len(page2.Ops) != 1 || page2.Ops[0].ID == page1.Ops[0].ID {
		t.Fatalf("expected the second page to return a different op, got %+v then %+v", page1.Ops, page2.Ops)
	}
}

func TestStatusReportsCheckpointAndConflictCount(t *testing.T) {
	srv, _ := newTestServer(t)
	dev := registerDevice(t, srv.URL, "Laptop")

	op := makeWireOp("op-1", dev.DeviceID, 100)
	doJSON(t, http.MethodPost, srv.URL+"/v1/sync/push", dev.DeviceToken, api.PushRequest{Ops: []api.Op{op}}, nil)

	var status api.StatusResponse
	doJSON(t, http.MethodGet, srv.URL+"/v1/sync/status", dev.DeviceToken, nil, &status)
	if status.DeviceID != dev.DeviceID {
		t.Fatalf("expected status for the authenticated device, got %+v", status)
	}
	if status.ServerCheckpoint != 1 {
		t.Fatalf("expected checkpoint 1, got %d", status.ServerCheckpoint)
	}
	if status.ConflictCount != 0 {
		t.Fatalf("expected 0 conflicts, got %d", status.ConflictCount)
	}
}

func TestGetRecordNotFound(t *testing.T) {
	srv, _ := newTestServer(t)
	dev := registerDevice(t, srv.URL, "Laptop")

	resp := doJSON(t, http.MethodGet, srv.URL+"/v1/records/notes/does-not-exist", dev.DeviceToken, nil, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}
