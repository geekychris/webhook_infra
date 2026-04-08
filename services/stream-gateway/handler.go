package main

import (
	"log/slog"
	"net/http"
	"strconv"

	"github.com/example/event-gateway/internal/readyqueue"
	queuev1 "github.com/example/event-gateway/internal/proto/queue/v1"
	"nhooyr.io/websocket"
)

// StreamHandler handles WebSocket upgrade requests for event streaming.
type StreamHandler struct {
	queueClient queuev1.QueueServiceClient
	readyQ      *readyqueue.Queue
	instanceID  string
	logger      *slog.Logger
}

func NewStreamHandler(queueClient queuev1.QueueServiceClient, readyQ *readyqueue.Queue, instanceID string, logger *slog.Logger) *StreamHandler {
	return &StreamHandler{
		queueClient: queueClient,
		readyQ:      readyQ,
		instanceID:  instanceID,
		logger:      logger,
	}
}

// HandleUpgrade handles the WebSocket upgrade and starts a streaming session.
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

	var resumeAfter uint64
	if ra := r.URL.Query().Get("resume_after"); ra != "" {
		var err error
		resumeAfter, err = strconv.ParseUint(ra, 10, 64)
		if err != nil {
			http.Error(w, `{"error":"invalid resume_after"}`, http.StatusBadRequest)
			return
		}
	}

	_ = token // TODO: validate against registry

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		h.logger.Error("websocket accept failed", "error", err)
		return
	}

	session := NewSession(conn, subID, h.queueClient, h.readyQ, h.instanceID, resumeAfter, h.logger)

	h.logger.Info("streaming session started", "subscription_id", subID, "resume_after", resumeAfter)

	if err := session.Run(r.Context()); err != nil {
		h.logger.Error("session ended with error", "subscription_id", subID, "error", err)
	}
}
