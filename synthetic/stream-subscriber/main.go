package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/event-gateway/internal/config"
	"github.com/example/event-gateway/internal/observability"
	"nhooyr.io/websocket"
)

type eventMessage struct {
	Type     string          `json:"type"`
	Position uint64          `json:"position"`
	Event    json.RawMessage `json:"event"`
}

type ackMessage struct {
	Type     string `json:"type"`
	Position uint64 `json:"position"`
}

func main() {
	logger := observability.NewLogger("stream-subscriber")

	gatewayURL := config.Env("GATEWAY_URL", "ws://localhost:8090/v1/stream")
	subID := config.Env("SUBSCRIPTION_ID", "test-sub-1")
	token := config.Env("AUTH_TOKEN", "test-token")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	url := fmt.Sprintf("%s?subscription_id=%s&token=%s", gatewayURL, subID, token)

	var totalReceived int64
	for {
		select {
		case <-ctx.Done():
			logger.Info("stream subscriber shutting down", "total_received", totalReceived)
			return
		default:
		}

		logger.Info("connecting to stream gateway", "url", gatewayURL)
		received, err := runSession(ctx, url, logger)
		totalReceived += received
		if err != nil {
			logger.Error("session ended", "error", err, "received_this_session", received)
		}

		// Reconnect with backoff
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func runSession(ctx context.Context, url string, logger *slog.Logger) (int64, error) {
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return 0, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "bye")

	var received int64
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return received, fmt.Errorf("read: %w", err)
		}

		var msg eventMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			logger.Warn("invalid message", "error", err)
			continue
		}

		if msg.Type == "event" {
			received++

			// Send ACK
			ack := ackMessage{Type: "ack", Position: msg.Position}
			ackData, _ := json.Marshal(ack)
			if err := conn.Write(ctx, websocket.MessageText, ackData); err != nil {
				return received, fmt.Errorf("write ack: %w", err)
			}

			if received%100 == 0 {
				logger.Info("events received", "count", received, "last_position", msg.Position)
			}
		}
	}
}
