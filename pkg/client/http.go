package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/stan-ley-tech/SyncForge/pkg/api"
)

// doJSON sends a JSON request to path on the configured server and decodes
// a JSON response into out (if non-nil), retrying transient failures
// (network errors and 5xx responses) per c.retryPolicy with exponential
// backoff. A 4xx response is returned immediately without retrying.
func (c *Client) doJSON(ctx context.Context, method, path, token string, body, out any) error {
	var bodyBytes []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		bodyBytes = b
	}

	policy := c.retryPolicy.normalized()
	var lastErr error

	for attempt := 0; attempt < policy.MaxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(policy.backoff(attempt - 1)):
			}
		}

		err := c.attemptJSON(ctx, method, path, token, bodyBytes, out)
		if err == nil {
			return nil
		}

		var retryable *retryableError
		if !errors.As(err, &retryable) {
			return err
		}
		lastErr = err
	}

	return fmt.Errorf("giving up after %d attempts: %w", policy.MaxAttempts, lastErr)
}

// attemptJSON performs exactly one HTTP round trip. Network errors and 5xx
// responses are wrapped in retryableError so doJSON knows to retry them;
// every other failure (bad request body, 4xx, response decode error) is
// returned as-is, terminal.
func (c *Client) attemptJSON(ctx context.Context, method, path, token string, bodyBytes []byte, out any) error {
	var reader io.Reader
	if bodyBytes != nil {
		reader = bytes.NewReader(bodyBytes)
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
		return &retryableError{fmt.Errorf("%s %s: %w", method, path, err)}
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if out == nil {
			return nil
		}
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
		return nil
	}

	var errResp api.ErrorResponse
	_ = json.NewDecoder(resp.Body).Decode(&errResp)
	wrapped := fmt.Errorf("%s %s: %d %s", method, path, resp.StatusCode, errResp.Error)
	if resp.StatusCode >= 500 {
		return &retryableError{wrapped}
	}
	return wrapped
}
