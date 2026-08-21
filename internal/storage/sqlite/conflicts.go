package sqlite

import (
	"context"
	"time"
)

// Conflict is an audit-trail row recording that two concurrent ops touched
// the same record and how it was resolved. The losing op is never deleted
// from the oplog — this row just records which op won and when.
type Conflict struct {
	ID            int64
	Collection    string
	RecordID      string
	WinningOpID   string
	LosingOpID    string
	DetectedAtSeq int64
	DetectedAt    time.Time
}

// RecordConflict appends a conflict audit entry.
func (d *DB) RecordConflict(ctx context.Context, c Conflict) error {
	_, err := d.sqldb.ExecContext(ctx, `INSERT INTO conflicts
		(collection, record_id, winning_op_id, losing_op_id, detected_at_seq, detected_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		c.Collection, c.RecordID, c.WinningOpID, c.LosingOpID, c.DetectedAtSeq,
		c.DetectedAt.UTC().Format(time.RFC3339Nano))
	return err
}

// ListConflicts returns every recorded conflict, oldest first.
func (d *DB) ListConflicts(ctx context.Context) ([]Conflict, error) {
	rows, err := d.sqldb.QueryContext(ctx, `SELECT id, collection, record_id, winning_op_id,
		losing_op_id, detected_at_seq, detected_at FROM conflicts ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Conflict
	for rows.Next() {
		var c Conflict
		var detectedAt string
		if err := rows.Scan(&c.ID, &c.Collection, &c.RecordID, &c.WinningOpID,
			&c.LosingOpID, &c.DetectedAtSeq, &detectedAt); err != nil {
			return nil, err
		}
		c.DetectedAt, err = time.Parse(time.RFC3339Nano, detectedAt)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
