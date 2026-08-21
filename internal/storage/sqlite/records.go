package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/stan-ley-tech/SyncForge/internal/hlc"
	"github.com/stan-ley-tech/SyncForge/internal/record"
	"github.com/stan-ley-tech/SyncForge/internal/vector"
)

// GetRecord returns the current materialized state of (collection,
// recordID), or found=false if no op has ever touched that record.
func (d *DB) GetRecord(ctx context.Context, collection, recordID string) (record.Record, bool, error) {
	row := d.sqldb.QueryRowContext(ctx, `SELECT collection, record_id, payload, deleted,
		version_vector, hlc_physical, hlc_logical, hlc_node, winning_op_id
		FROM records WHERE collection = ? AND record_id = ?`, collection, recordID)

	rec, err := scanRecord(row)
	if errors.Is(err, sql.ErrNoRows) {
		return record.Record{}, false, nil
	}
	if err != nil {
		return record.Record{}, false, err
	}
	return rec, true, nil
}

// PutRecord upserts the materialized state for a record.
func (d *DB) PutRecord(ctx context.Context, rec record.Record) error {
	vvJSON, err := json.Marshal(rec.VersionVector)
	if err != nil {
		return fmt.Errorf("marshal version vector: %w", err)
	}
	_, err = d.sqldb.ExecContext(ctx, `
		INSERT INTO records (collection, record_id, payload, deleted, version_vector,
			hlc_physical, hlc_logical, hlc_node, winning_op_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(collection, record_id) DO UPDATE SET
			payload = excluded.payload,
			deleted = excluded.deleted,
			version_vector = excluded.version_vector,
			hlc_physical = excluded.hlc_physical,
			hlc_logical = excluded.hlc_logical,
			hlc_node = excluded.hlc_node,
			winning_op_id = excluded.winning_op_id`,
		rec.Collection, rec.RecordID, payloadOrNil(rec.Payload), rec.Deleted, string(vvJSON),
		rec.HLC.Physical, rec.HLC.Logical, rec.HLC.NodeID, rec.WinningOpID,
	)
	return err
}

// ListRecords returns every materialized record in a collection, including
// tombstoned (deleted) ones, ordered by record id.
func (d *DB) ListRecords(ctx context.Context, collection string) ([]record.Record, error) {
	rows, err := d.sqldb.QueryContext(ctx, `SELECT collection, record_id, payload, deleted,
		version_vector, hlc_physical, hlc_logical, hlc_node, winning_op_id
		FROM records WHERE collection = ? ORDER BY record_id`, collection)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []record.Record
	for rows.Next() {
		rec, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func scanRecord(r rowScanner) (record.Record, error) {
	var (
		rec      record.Record
		payload  sql.NullString
		vvJSON   string
		physical int64
		logical  uint32
		node     string
	)
	if err := r.Scan(&rec.Collection, &rec.RecordID, &payload, &rec.Deleted,
		&vvJSON, &physical, &logical, &node, &rec.WinningOpID); err != nil {
		return record.Record{}, err
	}
	if payload.Valid {
		rec.Payload = json.RawMessage(payload.String)
	}
	var vv vector.Vector
	if err := json.Unmarshal([]byte(vvJSON), &vv); err != nil {
		return record.Record{}, fmt.Errorf("unmarshal version vector: %w", err)
	}
	rec.VersionVector = vv
	rec.HLC = hlc.Timestamp{Physical: physical, Logical: logical, NodeID: node}
	return rec, nil
}
