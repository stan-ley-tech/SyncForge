package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/stan-ley-tech/SyncForge/internal/storage/sqlite"
	"github.com/stan-ley-tech/SyncForge/pkg/api"
)

type contextKey int

const deviceContextKey contextKey = iota

// deviceFromContext returns the authenticated device attached by
// requireAuth. It must only be called from within a requireAuth-wrapped
// handler.
func deviceFromContext(ctx context.Context) sqlite.Device {
	dev, _ := ctx.Value(deviceContextKey).(sqlite.Device)
	return dev
}

// requireAuth wraps a handler so it only runs for requests bearing a valid
// device token, issued by handleRegisterDevice.
func (s *Server) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authz := r.Header.Get("Authorization")
		token, ok := strings.CutPrefix(authz, "Bearer ")
		if !ok || token == "" {
			writeError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}

		dev, found, err := s.db.DeviceByTokenHash(r.Context(), hashToken(token))
		if err != nil {
			writeError(w, http.StatusInternalServerError, "looking up device: "+err.Error())
			return
		}
		if !found {
			writeError(w, http.StatusUnauthorized, "invalid device token")
			return
		}

		next(w, r.WithContext(context.WithValue(r.Context(), deviceContextKey, dev)))
	}
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func newDeviceToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func (s *Server) handleRegisterDevice(w http.ResponseWriter, r *http.Request) {
	var req api.RegisterDeviceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if strings.TrimSpace(req.DeviceID) == "" {
		writeError(w, http.StatusBadRequest, "device_id is required")
		return
	}
	if strings.TrimSpace(req.DeviceName) == "" {
		writeError(w, http.StatusBadRequest, "device_name is required")
		return
	}

	token, err := newDeviceToken()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "generating device token: "+err.Error())
		return
	}

	dev := sqlite.Device{
		DeviceID:  req.DeviceID,
		Name:      req.DeviceName,
		TokenHash: hashToken(token),
		CreatedAt: nowFunc(),
	}
	if err := s.db.CreateDevice(r.Context(), dev); err != nil {
		writeError(w, http.StatusInternalServerError, "registering device: "+err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, api.RegisterDeviceResponse{
		DeviceID:    dev.DeviceID,
		DeviceToken: token,
	})
}
