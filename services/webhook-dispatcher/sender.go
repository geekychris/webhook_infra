package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/example/event-gateway/internal/model"
	"github.com/example/event-gateway/internal/signature"
)

// Sender delivers CloudEvents batches to webhook endpoints.
type Sender struct {
	client *http.Client
	logger *slog.Logger
}

// NewSender creates a webhook sender with appropriate timeouts.
func NewSender(logger *slog.Logger) *Sender {
	return &Sender{
		client: &http.Client{
			Timeout: 30 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		logger: logger,
	}
}

// SendBatch sends a batch of CloudEvents to a webhook endpoint with HMAC signature.
func (s *Sender) SendBatch(ctx context.Context, url string, signingKey []byte, events []model.CloudEvent) error {
	body, err := json.Marshal(events)
	if err != nil {
		return fmt.Errorf("marshal batch: %w", err)
	}

	ts := strconv.FormatInt(time.Now().Unix(), 10)
	sig := signature.Sign(signingKey, ts, body)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/cloudevents-batch+json")
	req.Header.Set("X-Webhook-Signature", "sha256="+sig)
	req.Header.Set("X-Webhook-Timestamp", ts)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("delivery failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	return fmt.Errorf("delivery rejected: status %d", resp.StatusCode)
}
