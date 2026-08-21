package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/stan-ley-tech/SyncForge/pkg/api"
)

// doJSON sends a JSON request to path on the configured server and decodes
// a JSON response into out (if non-nil). A non-2xx status is returned as
// an error carrying the server's error message where available.
func (c *Client) doJSON(ctx context.Context, method, path, token string, body, out any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		reader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errResp api.ErrorResponse
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		return fmt.Errorf("%s %s: %d %s", method, path, resp.StatusCode, errResp.Error)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}
