package sqlite

import (
	"context"
	"database/sql"
	"errors"
)

// GetKV reads a small piece of local state (e.g. the client's own device
// id, or the last-pulled server checkpoint).
func (d *DB) GetKV(ctx context.Context, key string) (string, bool, error) {
	var value string
	err := d.sqldb.QueryRowContext(ctx, `SELECT value FROM kv WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

// SetKV upserts a small piece of local state.
func (d *DB) SetKV(ctx context.Context, key, value string) error {
	_, err := d.sqldb.ExecContext(ctx, `INSERT INTO kv (key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}
