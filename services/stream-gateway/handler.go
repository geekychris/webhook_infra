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
	auth        *TokenAuth
	instanceID  string
	logger      *slog.Logger
}

func NewStreamHandler(queueClient queuev1.QueueServiceClient, readyQ *readyqueue.Queue, auth *TokenAuth, instanceID string, logger *slog.Logger) *StreamHandler {
	return &StreamHandler{
		queueClient: queueClient,
		readyQ:      readyQ,
		auth:        auth,
		instanceID:  instanceID,
		logger:      logger,
	}
}

// HandleUpgrade handles the WebSocket upgrade and starts a streaming session.
//
// Authentication: Token can be provided via:
//   - Query parameter: ?token=<value>
//   - Authorization header: Bearer <value>
//
// The token is validated against the subscriber's stream_auth field in the registry.
func (h *StreamHandler) HandleUpgrade(w http.ResponseWriter, r *http.Request) {
	subID := r.URL.Query().Get("subscription_id")
	if subID == "" {
		http.Error(w, `{"error":"subscription_id required"}`, http.StatusBadRequest)
		return
	}

	token := ExtractToken(r)
	if token == "" {
		http.Error(w, `{"error":"token required — provide via ?token= or Authorization: Bearer header"}`, http.StatusUnauthorized)
		return
	}

	// Validate token against registry
	if h.auth != nil {
		subscriberID, err := h.auth.Validate(r.Context(), subID, token)
		if err != nil {
			h.logger.Warn("stream auth failed",
				"subscription_id", subID,
				"error", err,
			)
			http.Error(w, `{"error":"authentication failed: `+err.Error()+`"}`, http.StatusForbidden)
			return
		}
		h.logger.Info("stream authenticated",
			"subscription_id", subID,
			"subscriber_id", subscriberID,
		)
	}

	var resumeAfter uint64
	// resume_from=N means "start delivering from position N (inclusive)"
	// resume_after=N means "start delivering after position N (exclusive, N+1)"
	// resume_from takes priority if both are provided.
	if rf := r.URL.Query().Get("resume_from"); rf != "" {
		pos, err := strconv.ParseUint(rf, 10, 64)
		if err != nil {
			http.Error(w, `{"error":"invalid resume_from"}`, http.StatusBadRequest)
			return
		}
		if pos > 0 {
			resumeAfter = pos - 1 // convert inclusive to exclusive
		}
	} else if ra := r.URL.Query().Get("resume_after"); ra != "" {
		var err error
		resumeAfter, err = strconv.ParseUint(ra, 10, 64)
		if err != nil {
			http.Error(w, `{"error":"invalid resume_after"}`, http.StatusBadRequest)
			return
		}
	}

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
