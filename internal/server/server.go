// Package server implements SyncForge's REST API: device registration and
// the push/pull sync endpoints, backed by a storage/sqlite.DB and the
// deterministic internal/conflict resolution engine.
package server

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/stan-ley-tech/SyncForge/internal/storage/sqlite"
	"github.com/stan-ley-tech/SyncForge/pkg/api"
)

// Server is a SyncForge REST API server.
type Server struct {
	db     *sqlite.DB
	logger *log.Logger
}

// New creates a Server backed by db.
func New(db *sqlite.DB) *Server {
	return &Server{db: db, logger: log.Default()}
}

// Handler returns the http.Handler implementing SyncForge's REST API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/devices/register", s.handleRegisterDevice)
	mux.HandleFunc("POST /v1/sync/push", s.requireAuth(s.handlePush))
	mux.HandleFunc("GET /v1/sync/pull", s.requireAuth(s.handlePull))
	mux.HandleFunc("GET /v1/sync/status", s.requireAuth(s.handleStatus))
	mux.HandleFunc("GET /v1/records/{collection}/{id}", s.requireAuth(s.handleGetRecord))
	return mux
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if body == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("server: encode response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, api.ErrorResponse{Error: msg})
}

// nowFunc is a tiny seam so tests can control "now" where it matters
// (currently only conflict audit timestamps, which don't affect
// determinism but do affect the audit record's DetectedAt field).
var nowFunc = time.Now
