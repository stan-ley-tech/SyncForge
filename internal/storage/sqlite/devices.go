package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Device is a registered client device, identified by DeviceID and
// authenticated by a hash of its bearer token (the server never stores the
// raw token).
type Device struct {
	DeviceID  string
	Name      string
	TokenHash string
	CreatedAt time.Time
}

// CreateDevice registers a device. Registering the same DeviceID again
// rotates its name/token (created_at is preserved) rather than failing, so
// a client that lost its registration response can safely retry.
func (d *DB) CreateDevice(ctx context.Context, dev Device) error {
	_, err := d.sqldb.ExecContext(ctx, `
		INSERT INTO devices (device_id, name, token_hash, created_at) VALUES (?, ?, ?, ?)
		ON CONFLICT(device_id) DO UPDATE SET name = excluded.name, token_hash = excluded.token_hash`,
		dev.DeviceID, dev.Name, dev.TokenHash, dev.CreatedAt.UTC().Format(time.RFC3339Nano))
	return err
}

// DeviceByID looks up a device by id.
func (d *DB) DeviceByID(ctx context.Context, id string) (Device, bool, error) {
	return scanDevice(d.sqldb.QueryRowContext(ctx,
		`SELECT device_id, name, token_hash, created_at FROM devices WHERE device_id = ?`, id))
}

// DeviceByTokenHash looks up a device by its hashed bearer token, used to
// authenticate incoming requests.
func (d *DB) DeviceByTokenHash(ctx context.Context, tokenHash string) (Device, bool, error) {
	return scanDevice(d.sqldb.QueryRowContext(ctx,
		`SELECT device_id, name, token_hash, created_at FROM devices WHERE token_hash = ?`, tokenHash))
}

func scanDevice(row *sql.Row) (Device, bool, error) {
	var dev Device
	var createdAt string
	err := row.Scan(&dev.DeviceID, &dev.Name, &dev.TokenHash, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Device{}, false, nil
	}
	if err != nil {
		return Device{}, false, err
	}
	dev.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return Device{}, false, err
	}
	return dev, true, nil
}
