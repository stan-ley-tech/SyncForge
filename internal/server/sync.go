package server

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/stan-ley-tech/SyncForge/internal/conflict"
	"github.com/stan-ley-tech/SyncForge/internal/storage/sqlite"
	"github.com/stan-ley-tech/SyncForge/pkg/api"
)

// handlePush accepts a batch of ops from an authenticated device, applies
// each through the deterministic conflict resolver, and durably assigns
// each a server checkpoint. It is idempotent: ops already stored (matched
// by id) are acknowledged again without side effects, which is what makes
// client-side retries safe.
func (s *Server) handlePush(w http.ResponseWriter, r *http.Request) {
	device := deviceFromContext(r.Context())

	var req api.PushRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	ctx := r.Context()
	accepted := make([]string, 0, len(req.Ops))
	var conflicts []api.ConflictInfo

	for _, dto := range req.Ops {
		op, err := dto.ToOplog()
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid op: "+err.Error())
			return
		}
		if op.DeviceID != device.DeviceID {
			writeError(w, http.StatusForbidden, "op device_id does not match authenticated device")
			return
		}

		stored, inserted, err := s.db.AppendOp(ctx, op, true)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "storing op: "+err.Error())
			return
		}

		if inserted {
			existing, found, err := s.db.GetRecord(ctx, stored.Collection, stored.RecordID)
			if err != nil {
				writeError(w, http.StatusInternalServerError, "loading record: "+err.Error())
				return
			}
			decision := conflict.Resolve(existing, found, stored)
			if err := s.db.PutRecord(ctx, decision.Record); err != nil {
				writeError(w, http.StatusInternalServerError, "saving record: "+err.Error())
				return
			}
			if decision.Conflicted {
				if err := s.db.RecordConflict(ctx, sqlite.Conflict{
					Collection:    stored.Collection,
					RecordID:      stored.RecordID,
					WinningOpID:   decision.WinnerOpID,
					LosingOpID:    decision.LoserOpID,
					DetectedAtSeq: stored.ServerSeq,
					DetectedAt:    nowFunc(),
				}); err != nil {
					writeError(w, http.StatusInternalServerError, "recording conflict: "+err.Error())
					return
				}
				conflicts = append(conflicts, api.ConflictInfo{
					Collection:  stored.Collection,
					RecordID:    stored.RecordID,
					WinningOpID: decision.WinnerOpID,
					LosingOpID:  decision.LoserOpID,
				})
			}
		}

		accepted = append(accepted, stored.ID)
	}

	checkpoint, err := s.db.CurrentCheckpoint(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "reading checkpoint: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, api.PushResponse{
		Accepted:         accepted,
		Conflicts:        conflicts,
		ServerCheckpoint: checkpoint,
	})
}

// handlePull returns ops accepted after the given checkpoint, optionally
// restricted to one collection, incrementally and in pages.
func (s *Server) handlePull(w http.ResponseWriter, r *http.Request) {
	since, err := parseInt64(r.URL.Query().Get("since"), 0)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid since: "+err.Error())
		return
	}
	limit, err := parseInt(r.URL.Query().Get("limit"), 0)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid limit: "+err.Error())
		return
	}
	collection := r.URL.Query().Get("collection")

	ops, next, hasMore, err := s.db.OpsSince(r.Context(), since, collection, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "reading ops: "+err.Error())
		return
	}

	dtos := make([]api.Op, len(ops))
	for i, op := range ops {
		dtos[i] = api.FromOplog(op)
	}

	writeJSON(w, http.StatusOK, api.PullResponse{
		Ops:            dtos,
		NextCheckpoint: next,
		HasMore:        hasMore,
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	device := deviceFromContext(r.Context())
	ctx := r.Context()

	checkpoint, err := s.db.CurrentCheckpoint(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "reading checkpoint: "+err.Error())
		return
	}
	conflicts, err := s.db.ListConflicts(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "reading conflicts: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, api.StatusResponse{
		DeviceID:         device.DeviceID,
		ServerCheckpoint: checkpoint,
		ConflictCount:    len(conflicts),
	})
}

func (s *Server) handleGetRecord(w http.ResponseWriter, r *http.Request) {
	collection := r.PathValue("collection")
	id := r.PathValue("id")

	rec, found, err := s.db.GetRecord(r.Context(), collection, id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "reading record: "+err.Error())
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "record not found")
		return
	}

	writeJSON(w, http.StatusOK, api.RecordResponse{
		Collection:    rec.Collection,
		RecordID:      rec.RecordID,
		Payload:       rec.Payload,
		Deleted:       rec.Deleted,
		VersionVector: map[string]uint64(rec.VersionVector),
	})
}

func parseInt64(s string, def int64) (int64, error) {
	if s == "" {
		return def, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

func parseInt(s string, def int) (int, error) {
	if s == "" {
		return def, nil
	}
	return strconv.Atoi(s)
}
