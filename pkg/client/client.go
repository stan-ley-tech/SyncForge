// Package client is SyncForge's offline-first client SDK: local reads and
// writes never touch the network, and go through the exact same
// deterministic conflict resolution engine the server uses. Sync() is an
// explicit step that reconciles local changes with a server when
// connectivity is available.
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/stan-ley-tech/SyncForge/internal/conflict"
	"github.com/stan-ley-tech/SyncForge/internal/hlc"
	"github.com/stan-ley-tech/SyncForge/internal/oplog"
	"github.com/stan-ley-tech/SyncForge/internal/storage/sqlite"
	"github.com/stan-ley-tech/SyncForge/pkg/api"
)

const (
	kvDeviceID       = "device_id"
	kvDeviceToken    = "device_token"
	kvLastCheckpoint = "last_checkpoint"
)

// Client is a SyncForge client SDK instance: one local, offline-capable
// store representing a single device.
type Client struct {
	db         *sqlite.DB
	deviceID   string
	clock      *hlc.Clock
	baseURL    string
	httpClient *http.Client
}

// Record is an application-facing view of a synced record: just the
// payload the caller put in, decoupled from the internal storage/oplog
// representation.
type Record struct {
	Collection string
	RecordID   string
	Value      json.RawMessage
}

// SyncResult summarizes what one Sync() call did.
type SyncResult struct {
	Pushed    int
	Pulled    int
	Conflicts []api.ConflictInfo
}

// Open opens (creating if necessary) a local SyncForge store at dbPath and
// returns a Client that will sync against serverURL. Opening a store for
// the first time assigns it a permanent, locally-generated device id —
// this happens before any network access, so a device has a stable
// identity (and can make offline writes) even if it never reaches a
// server.
func Open(dbPath, serverURL string) (*Client, error) {
	db, err := sqlite.Open(dbPath)
	if err != nil {
		return nil, fmt.Errorf("client: open local store: %w", err)
	}

	ctx := context.Background()
	deviceID, found, err := db.GetKV(ctx, kvDeviceID)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("client: read device id: %w", err)
	}
	if !found {
		deviceID = uuid.NewString()
		if err := db.SetKV(ctx, kvDeviceID, deviceID); err != nil {
			db.Close()
			return nil, fmt.Errorf("client: persist device id: %w", err)
		}
	}

	return &Client{
		db:         db,
		deviceID:   deviceID,
		clock:      hlc.NewClock(deviceID),
		baseURL:    serverURL,
		httpClient: http.DefaultClient,
	}, nil
}

// Close closes the local store.
func (c *Client) Close() error {
	return c.db.Close()
}

// DeviceID returns this client's locally-generated device identity.
func (c *Client) DeviceID() string {
	return c.deviceID
}

// Register associates this device's identity with a bearer token on the
// server, under the given human-readable name. It is safe to call again
// (e.g. after losing the response to a network error): re-registering the
// same device id rotates its token rather than failing.
func (c *Client) Register(ctx context.Context, deviceName string) error {
	var resp api.RegisterDeviceResponse
	req := api.RegisterDeviceRequest{DeviceID: c.deviceID, DeviceName: deviceName}
	if err := c.doJSON(ctx, http.MethodPost, "/v1/devices/register", "", req, &resp); err != nil {
		return fmt.Errorf("client: register device: %w", err)
	}
	return c.db.SetKV(ctx, kvDeviceToken, resp.DeviceToken)
}

// Put creates or updates a record. The write is applied to the local store
// immediately and is visible to Get/List right away; it is not sent to the
// server until Sync is called.
func (c *Client) Put(ctx context.Context, collection, recordID string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("client: marshal value: %w", err)
	}
	_, err = c.applyLocalWrite(ctx, collection, recordID, payload, false)
	return err
}

// Delete tombstones a record locally. Like Put, it takes effect
// immediately and is only sent to the server on the next Sync.
func (c *Client) Delete(ctx context.Context, collection, recordID string) error {
	_, err := c.applyLocalWrite(ctx, collection, recordID, nil, true)
	return err
}

// Get returns a record's current local value. found is false if the
// record does not exist or has been deleted.
func (c *Client) Get(ctx context.Context, collection, recordID string) (rec Record, found bool, err error) {
	stored, exists, err := c.db.GetRecord(ctx, collection, recordID)
	if err != nil || !exists || stored.Deleted {
		return Record{}, false, err
	}
	return Record{Collection: stored.Collection, RecordID: stored.RecordID, Value: stored.Payload}, true, nil
}

// List returns every non-deleted record in a collection.
func (c *Client) List(ctx context.Context, collection string) ([]Record, error) {
	stored, err := c.db.ListRecords(ctx, collection)
	if err != nil {
		return nil, err
	}
	out := make([]Record, 0, len(stored))
	for _, rec := range stored {
		if rec.Deleted {
			continue
		}
		out = append(out, Record{Collection: rec.Collection, RecordID: rec.RecordID, Value: rec.Payload})
	}
	return out, nil
}

// PendingChanges returns the number of local writes not yet acknowledged
// by a server (i.e. what the next Sync would push).
func (c *Client) PendingChanges(ctx context.Context) (int, error) {
	ops, err := c.db.UnpushedOps(ctx, c.deviceID)
	if err != nil {
		return 0, err
	}
	return len(ops), nil
}

// applyLocalWrite is the single code path for every local mutation
// (Put and Delete both funnel through it): build an Op from the current
// local state, append it to the local oplog, and fold it into the
// materialized record via the same conflict.Resolve the server and remote
// sync both use. Since the op's version vector always strictly advances
// past whatever is already there, Resolve always takes the fast-forward
// path here — there is no special-casing for "this is my own write."
func (c *Client) applyLocalWrite(ctx context.Context, collection, recordID string, payload json.RawMessage, del bool) (Record, error) {
	existing, found, err := c.db.GetRecord(ctx, collection, recordID)
	if err != nil {
		return Record{}, err
	}

	opType := oplog.Update
	switch {
	case del:
		opType = oplog.Delete
	case !found || existing.Deleted:
		opType = oplog.Create
	}

	op := oplog.Op{
		ID:            uuid.NewString(),
		DeviceID:      c.deviceID,
		Collection:    collection,
		RecordID:      recordID,
		Type:          opType,
		Payload:       payload,
		VersionVector: existing.VersionVector.Increment(c.deviceID),
		HLC:           c.clock.Now(),
	}

	stored, _, err := c.db.AppendOp(ctx, op, false)
	if err != nil {
		return Record{}, err
	}

	decision := conflict.Resolve(existing, found, stored)
	if err := c.db.PutRecord(ctx, decision.Record); err != nil {
		return Record{}, err
	}
	return Record{Collection: collection, RecordID: recordID, Value: decision.Record.Payload}, nil
}

func (c *Client) requireToken(ctx context.Context) (string, error) {
	token, found, err := c.db.GetKV(ctx, kvDeviceToken)
	if err != nil {
		return "", err
	}
	if !found {
		return "", errors.New("client: device is not registered with a server; call Register first")
	}
	return token, nil
}
