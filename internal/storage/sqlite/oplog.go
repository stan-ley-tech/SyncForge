package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/stan-ley-tech/SyncForge/internal/hlc"
	"github.com/stan-ley-tech/SyncForge/internal/oplog"
	"github.com/stan-ley-tech/SyncForge/internal/vector"
)

// AppendOp inserts op into the log. If assignSeq is true, the store assigns
// the next monotonic server_seq atomically as part of this insert (the
// server's role); otherwise op.ServerSeq is stored as given (the client's
// role, either 0 for a purely local op or a value echoed back from a
// server pull).
//
// AppendOp is idempotent on op.ID: if an op with the same ID was already
// stored, the existing row is returned unchanged and inserted is false.
// This is what makes retried pushes safe.
func (d *DB) AppendOp(ctx context.Context, op oplog.Op, assignSeq bool) (stored oplog.Op, inserted bool, err error) {
	if err := op.Validate(); err != nil {
		return oplog.Op{}, false, err
	}

	err = d.WithTx(ctx, func(tx *sql.Tx) error {
		var existingSeq int64
		scanErr := tx.QueryRowContext(ctx, `SELECT server_seq FROM oplog WHERE op_id = ?`, op.ID).Scan(&existingSeq)
		if scanErr == nil {
			op.ServerSeq = existingSeq
			inserted = false
			return nil
		}
		if !errors.Is(scanErr, sql.ErrNoRows) {
			return fmt.Errorf("check existing op: %w", scanErr)
		}

		if assignSeq {
			seq, seqErr := nextServerSeq(ctx, tx)
			if seqErr != nil {
				return fmt.Errorf("assign server_seq: %w", seqErr)
			}
			op.ServerSeq = seq
		}

		vvJSON, mErr := json.Marshal(op.VersionVector)
		if mErr != nil {
			return fmt.Errorf("marshal version vector: %w", mErr)
		}

		_, execErr := tx.ExecContext(ctx, `
			INSERT INTO oplog (
				op_id, device_id, collection, record_id, op_type, payload,
				version_vector, hlc_physical, hlc_logical, hlc_node, server_seq, pushed
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)`,
			op.ID, op.DeviceID, op.Collection, op.RecordID, string(op.Type), payloadOrNil(op.Payload),
			string(vvJSON), op.HLC.Physical, op.HLC.Logical, op.HLC.NodeID, op.ServerSeq,
		)
		if execErr != nil {
			return fmt.Errorf("insert op: %w", execErr)
		}
		inserted = true
		return nil
	})
	if err != nil {
		return oplog.Op{}, false, err
	}
	return op, inserted, nil
}

func nextServerSeq(ctx context.Context, tx *sql.Tx) (int64, error) {
	if _, err := tx.ExecContext(ctx, `UPDATE server_counter SET next = next + 1 WHERE id = 1`); err != nil {
		return 0, err
	}
	var next int64
	if err := tx.QueryRowContext(ctx, `SELECT next FROM server_counter WHERE id = 1`).Scan(&next); err != nil {
		return 0, err
	}
	return next - 1, nil
}

// OpsSince returns ops with server_seq > since (optionally restricted to a
// single collection), ordered by server_seq ascending, up to limit rows.
// nextCheckpoint is the server_seq to pass as `since` on the following
// call; hasMore indicates there are additional ops beyond limit.
func (d *DB) OpsSince(ctx context.Context, since int64, collection string, limit int) (ops []oplog.Op, nextCheckpoint int64, hasMore bool, err error) {
	if limit <= 0 {
		limit = 100
	}
	query := `SELECT op_id, device_id, collection, record_id, op_type, payload,
		version_vector, hlc_physical, hlc_logical, hlc_node, server_seq
		FROM oplog WHERE server_seq > ? AND server_seq > 0`
	args := []any{since}
	if collection != "" {
		query += ` AND collection = ?`
		args = append(args, collection)
	}
	query += ` ORDER BY server_seq ASC LIMIT ?`
	args = append(args, limit+1)

	rows, err := d.sqldb.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, since, false, err
	}
	defer rows.Close()

	for rows.Next() {
		op, scanErr := scanOp(rows)
		if scanErr != nil {
			return nil, since, false, scanErr
		}
		ops = append(ops, op)
	}
	if err := rows.Err(); err != nil {
		return nil, since, false, err
	}

	nextCheckpoint = since
	if len(ops) > limit {
		hasMore = true
		ops = ops[:limit]
	}
	if len(ops) > 0 {
		nextCheckpoint = ops[len(ops)-1].ServerSeq
	}
	return ops, nextCheckpoint, hasMore, nil
}

// UnpushedOps returns ops that deviceID originated locally and have not
// yet been acknowledged by a server push (the client's outbox).
func (d *DB) UnpushedOps(ctx context.Context, deviceID string) ([]oplog.Op, error) {
	rows, err := d.sqldb.QueryContext(ctx, `SELECT op_id, device_id, collection, record_id, op_type, payload,
		version_vector, hlc_physical, hlc_logical, hlc_node, server_seq
		FROM oplog WHERE device_id = ? AND pushed = 0 ORDER BY id ASC`, deviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ops []oplog.Op
	for rows.Next() {
		op, scanErr := scanOp(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		ops = append(ops, op)
	}
	return ops, rows.Err()
}

// MarkPushed marks the given op ids as acknowledged by the server so
// UnpushedOps stops returning them.
func (d *DB) MarkPushed(ctx context.Context, opIDs []string) error {
	if len(opIDs) == 0 {
		return nil
	}
	return d.WithTx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, `UPDATE oplog SET pushed = 1 WHERE op_id = ?`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, id := range opIDs {
			if _, err := stmt.ExecContext(ctx, id); err != nil {
				return err
			}
		}
		return nil
	})
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanOp(r rowScanner) (oplog.Op, error) {
	var (
		op       oplog.Op
		opType   string
		payload  sql.NullString
		vvJSON   string
		physical int64
		logical  uint32
		node     string
	)
	if err := r.Scan(&op.ID, &op.DeviceID, &op.Collection, &op.RecordID, &opType, &payload,
		&vvJSON, &physical, &logical, &node, &op.ServerSeq); err != nil {
		return oplog.Op{}, err
	}
	op.Type = oplog.Type(opType)
	if payload.Valid {
		op.Payload = json.RawMessage(payload.String)
	}
	var vv vector.Vector
	if err := json.Unmarshal([]byte(vvJSON), &vv); err != nil {
		return oplog.Op{}, fmt.Errorf("unmarshal version vector: %w", err)
	}
	op.VersionVector = vv
	op.HLC = hlc.Timestamp{Physical: physical, Logical: logical, NodeID: node}
	return op, nil
}

func payloadOrNil(p json.RawMessage) any {
	if p == nil {
		return nil
	}
	return string(p)
}
