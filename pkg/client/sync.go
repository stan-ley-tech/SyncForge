package client

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/stan-ley-tech/SyncForge/internal/conflict"
	"github.com/stan-ley-tech/SyncForge/internal/storage/sqlite"
	"github.com/stan-ley-tech/SyncForge/pkg/api"
)

const pullPageSize = 100

// Sync reconciles local changes with the server: it pushes every local op
// that has not yet been acknowledged, then pulls (and applies) every
// server op this device has not yet seen. Both directions run every
// pulled/received op through the same conflict.Resolve the server uses,
// which is what guarantees this device converges to the same state as the
// server and every other device, regardless of sync order.
func (c *Client) Sync(ctx context.Context) (SyncResult, error) {
	token, err := c.requireToken(ctx)
	if err != nil {
		return SyncResult{}, err
	}

	var result SyncResult

	pushed, conflicts, err := c.push(ctx, token)
	if err != nil {
		return result, err
	}
	result.Pushed = pushed
	result.Conflicts = append(result.Conflicts, conflicts...)

	pulled, conflicts, err := c.pull(ctx, token)
	if err != nil {
		return result, err
	}
	result.Pulled = pulled
	result.Conflicts = append(result.Conflicts, conflicts...)

	return result, nil
}

func (c *Client) push(ctx context.Context, token string) (pushed int, conflicts []api.ConflictInfo, err error) {
	unpushed, err := c.db.UnpushedOps(ctx, c.deviceID)
	if err != nil {
		return 0, nil, fmt.Errorf("client: read unpushed ops: %w", err)
	}
	if len(unpushed) == 0 {
		return 0, nil, nil
	}

	wireOps := make([]api.Op, len(unpushed))
	for i, op := range unpushed {
		wireOps[i] = api.FromOplog(op)
	}

	var resp api.PushResponse
	if err := c.doJSON(ctx, http.MethodPost, "/v1/sync/push", token, api.PushRequest{Ops: wireOps}, &resp); err != nil {
		return 0, nil, fmt.Errorf("client: push: %w", err)
	}
	if err := c.db.MarkPushed(ctx, resp.Accepted); err != nil {
		return 0, nil, fmt.Errorf("client: mark pushed: %w", err)
	}
	return len(resp.Accepted), resp.Conflicts, nil
}

func (c *Client) pull(ctx context.Context, token string) (pulled int, conflicts []api.ConflictInfo, err error) {
	since, err := c.checkpoint(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("client: read checkpoint: %w", err)
	}

	for {
		var resp api.PullResponse
		path := fmt.Sprintf("/v1/sync/pull?since=%d&limit=%d", since, pullPageSize)
		if err := c.doJSON(ctx, http.MethodGet, path, token, nil, &resp); err != nil {
			return pulled, conflicts, fmt.Errorf("client: pull: %w", err)
		}

		for _, dto := range resp.Ops {
			op, err := dto.ToOplog()
			if err != nil {
				return pulled, conflicts, fmt.Errorf("client: decode pulled op: %w", err)
			}
			c.clock.Observe(op.HLC)

			stored, inserted, err := c.db.AppendOp(ctx, op, false)
			if err != nil {
				return pulled, conflicts, fmt.Errorf("client: store pulled op: %w", err)
			}
			if !inserted {
				continue // already applied (e.g. our own write, echoed back)
			}
			pulled++

			existing, found, err := c.db.GetRecord(ctx, stored.Collection, stored.RecordID)
			if err != nil {
				return pulled, conflicts, fmt.Errorf("client: load record: %w", err)
			}
			decision := conflict.Resolve(existing, found, stored)
			if err := c.db.PutRecord(ctx, decision.Record); err != nil {
				return pulled, conflicts, fmt.Errorf("client: save record: %w", err)
			}
			if decision.Conflicted {
				info := api.ConflictInfo{
					Collection:  stored.Collection,
					RecordID:    stored.RecordID,
					WinningOpID: decision.WinnerOpID,
					LosingOpID:  decision.LoserOpID,
				}
				if err := c.db.RecordConflict(ctx, sqlite.Conflict{
					Collection:    info.Collection,
					RecordID:      info.RecordID,
					WinningOpID:   info.WinningOpID,
					LosingOpID:    info.LosingOpID,
					DetectedAtSeq: stored.ServerSeq,
					DetectedAt:    time.Now(),
				}); err != nil {
					return pulled, conflicts, fmt.Errorf("client: record conflict: %w", err)
				}
				conflicts = append(conflicts, info)
			}
		}

		since = resp.NextCheckpoint
		if err := c.setCheckpoint(ctx, since); err != nil {
			return pulled, conflicts, fmt.Errorf("client: save checkpoint: %w", err)
		}
		if !resp.HasMore {
			return pulled, conflicts, nil
		}
	}
}

func (c *Client) checkpoint(ctx context.Context) (int64, error) {
	v, found, err := c.db.GetKV(ctx, kvLastCheckpoint)
	if err != nil || !found {
		return 0, err
	}
	return strconv.ParseInt(v, 10, 64)
}

func (c *Client) setCheckpoint(ctx context.Context, checkpoint int64) error {
	return c.db.SetKV(ctx, kvLastCheckpoint, strconv.FormatInt(checkpoint, 10))
}
