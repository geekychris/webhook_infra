package main

import (
	"log/slog"
	"net/http"
	"strconv"

	queuev1 "github.com/example/event-gateway/internal/proto/queue/v1"
	"nhooyr.io/websocket"
)

// StreamHandler handles WebSocket upgrade requests for event streaming.
type StreamHandler struct {
	queueClient queuev1.QueueServiceClient
	instanceID  string
	logger      *slog.Logger
}

// NewStreamHandler creates a new stream handler.
func NewStreamHandler(queueClient queuev1.QueueServiceClient, instanceID string, logger *slog.Logger) *StreamHandler {
	return &StreamHandler{
		queueClient: queueClient,
		instanceID:  instanceID,
		logger:      logger,
	}
}

// HandleUpgrade handles the WebSocket upgrade and starts a streaming session.
// Query params: subscription_id, token, resume_after (optional position).
func (h *StreamHandler) HandleUpgrade(w http.ResponseWriter, r *http.Request) {
	subID := r.URL.Query().Get("subscription_id")
	if subID == "" {
		http.Error(w, `{"error":"subscription_id required"}`, http.StatusBadRequest)
		return
	}

	token := r.URL.Query().Get("token")
	if token == "" {
		http.Error(w, `{"error":"token required"}`, http.StatusUnauthorized)
		return
	}

	// Parse optional resume position
	var resumeAfter uint64
	if ra := r.URL.Query().Get("resume_after"); ra != "" {
		var err error
		resumeAfter, err = strconv.ParseUint(ra, 10, 64)
		if err != nil {
			http.Error(w, `{"error":"invalid resume_after"}`, http.StatusBadRequest)
			return
		}
	}

	// TODO: validate token against subscriber registry
	_ = token

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		h.logger.Error("websocket accept failed", "error", err)
		return
	}

	session := NewSession(conn, subID, h.queueClient, h.instanceID, resumeAfter, h.logger)

	h.logger.Info("streaming session started",
		"subscription_id", subID,
		"resume_after", resumeAfter,
	)

	if err := session.Run(r.Context()); err != nil {
		h.logger.Error("session ended with error",
			"subscription_id", subID,
			"error", err,
		)
	}
}
