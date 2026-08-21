// Package sqlite is the storage layer shared by the server and the client
// SDK: a pure-Go (no CGO), file- or memory-backed SQLite database holding
// the operation log, the materialized record view, the conflict audit
// trail, device registrations, and small key/value sync state.
//
// The same schema and Go type serves both roles. The server uses the full
// surface (device registry, checkpoint assignment). The client SDK uses it
// as its local, offline-capable store and only a subset of the methods
// (device registry table sits unused there).
package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite" // registers the "sqlite" driver
)

// DB is a handle to a SyncForge SQLite store.
type DB struct {
	sqldb *sql.DB
}

const schema = `
CREATE TABLE IF NOT EXISTS oplog (
	id             INTEGER PRIMARY KEY AUTOINCREMENT,
	op_id          TEXT NOT NULL UNIQUE,
	device_id      TEXT NOT NULL,
	collection     TEXT NOT NULL,
	record_id      TEXT NOT NULL,
	op_type        TEXT NOT NULL,
	payload        TEXT,
	version_vector TEXT NOT NULL,
	hlc_physical   INTEGER NOT NULL,
	hlc_logical    INTEGER NOT NULL,
	hlc_node       TEXT NOT NULL,
	server_seq     INTEGER NOT NULL DEFAULT 0,
	pushed         INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_oplog_server_seq ON oplog(server_seq);
CREATE INDEX IF NOT EXISTS idx_oplog_collection ON oplog(collection);
CREATE INDEX IF NOT EXISTS idx_oplog_device_pushed ON oplog(device_id, pushed);

CREATE TABLE IF NOT EXISTS records (
	collection     TEXT NOT NULL,
	record_id      TEXT NOT NULL,
	payload        TEXT,
	deleted        INTEGER NOT NULL DEFAULT 0,
	version_vector TEXT NOT NULL,
	hlc_physical   INTEGER NOT NULL,
	hlc_logical    INTEGER NOT NULL,
	hlc_node       TEXT NOT NULL,
	winning_op_id  TEXT NOT NULL,
	PRIMARY KEY (collection, record_id)
);

CREATE TABLE IF NOT EXISTS conflicts (
	id                INTEGER PRIMARY KEY AUTOINCREMENT,
	collection        TEXT NOT NULL,
	record_id         TEXT NOT NULL,
	winning_op_id     TEXT NOT NULL,
	losing_op_id      TEXT NOT NULL,
	detected_at_seq   INTEGER NOT NULL DEFAULT 0,
	detected_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS devices (
	device_id    TEXT PRIMARY KEY,
	name         TEXT NOT NULL,
	token_hash   TEXT NOT NULL,
	created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_devices_token_hash ON devices(token_hash);

CREATE TABLE IF NOT EXISTS server_counter (
	id    INTEGER PRIMARY KEY CHECK (id = 1),
	next  INTEGER NOT NULL
);
INSERT OR IGNORE INTO server_counter (id, next) VALUES (1, 1);

CREATE TABLE IF NOT EXISTS kv (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
`

// Open opens (creating if necessary) a SyncForge store at path. Use
// ":memory:" for an ephemeral in-process store (handy in tests and for the
// demo CLI).
//
// The connection pool is capped at a single connection: SQLite serializes
// writers at the file level regardless, and pinning one Go connection
// turns that into straightforward mutual exclusion instead of driver-level
// SQLITE_BUSY retries.
func Open(path string) (*DB, error) {
	sqldb, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("sqlite: open %s: %w", path, err)
	}
	sqldb.SetMaxOpenConns(1)

	if _, err := sqldb.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		sqldb.Close()
		return nil, fmt.Errorf("sqlite: set pragma: %w", err)
	}
	if _, err := sqldb.Exec(schema); err != nil {
		sqldb.Close()
		return nil, fmt.Errorf("sqlite: apply schema: %w", err)
	}
	return &DB{sqldb: sqldb}, nil
}

// Close closes the underlying database connection.
func (d *DB) Close() error {
	return d.sqldb.Close()
}

// WithTx runs fn inside a transaction, committing on success and rolling
// back if fn returns an error.
func (d *DB) WithTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := d.sqldb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // no-op if already committed

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// CurrentCheckpoint returns the server_seq of the most recently assigned
// op — the server's current sync checkpoint (0 if none has ever been
// assigned).
func (d *DB) CurrentCheckpoint(ctx context.Context) (int64, error) {
	var next int64
	if err := d.sqldb.QueryRowContext(ctx, `SELECT next FROM server_counter WHERE id = 1`).Scan(&next); err != nil {
		return 0, err
	}
	return next - 1, nil
}
