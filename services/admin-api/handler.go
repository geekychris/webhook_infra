package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	queuev1 "github.com/example/event-gateway/internal/proto/queue/v1"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// AdminHandler implements admin API endpoints.
type AdminHandler struct {
	pool        *pgxpool.Pool
	queueClient queuev1.QueueServiceClient
	logger      *slog.Logger
}

// NewAdminHandler creates a new admin handler.
func NewAdminHandler(pool *pgxpool.Pool, queueClient queuev1.QueueServiceClient, logger *slog.Logger) *AdminHandler {
	return &AdminHandler{
		pool:        pool,
		queueClient: queueClient,
		logger:      logger,
	}
}

type subscriptionSummary struct {
	ID           string `json:"id"`
	SubscriberID string `json:"subscriber_id"`
	DeliveryMode string `json:"delivery_mode"`
	Active       bool   `json:"active"`
	EventTypes   []string `json:"event_types"`
}

// ListSubscriptions returns all subscriptions with their status.
func (h *AdminHandler) ListSubscriptions(w http.ResponseWriter, r *http.Request) {
	rows, err := h.pool.Query(r.Context(), `
		SELECT id, subscriber_id, delivery_mode, active, event_types
		FROM subscriptions
		ORDER BY created_at DESC
	`)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to query subscriptions")
		return
	}
	defer rows.Close()

	var subs []subscriptionSummary
	for rows.Next() {
		var s subscriptionSummary
		if err := rows.Scan(&s.ID, &s.SubscriberID, &s.DeliveryMode, &s.Active, &s.EventTypes); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to scan row")
			return
		}
		subs = append(subs, s)
	}

	writeJSON(w, http.StatusOK, subs)
}

type subscriptionStats struct {
	SubscriptionID string `json:"subscription_id"`
	TailSeq        uint64 `json:"tail_seq"`
	AckedSeq       uint64 `json:"acked_seq"`
	PendingCount   uint64 `json:"pending_count"`
	LeaseEpoch     uint64 `json:"lease_epoch"`
}

// GetStats returns queue statistics for a subscription.
func (h *AdminHandler) GetStats(w http.ResponseWriter, r *http.Request) {
	subID := chi.URLParam(r, "id")

	state, err := h.queueClient.GetState(r.Context(), &queuev1.GetStateRequest{
		SubscriptionId: subID,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get queue state")
		return
	}

	qs := state.GetState()
	stats := subscriptionStats{
		SubscriptionID: qs.GetSubscriptionId(),
		TailSeq:        qs.GetTailSeq(),
		AckedSeq:       qs.GetAckedSeq(),
		PendingCount:   qs.GetTailSeq() - qs.GetAckedSeq(),
		LeaseEpoch:     qs.GetLeaseEpoch(),
	}

	writeJSON(w, http.StatusOK, stats)
}

// PauseSubscription sets a subscription to inactive.
func (h *AdminHandler) PauseSubscription(w http.ResponseWriter, r *http.Request) {
	subID := chi.URLParam(r, "id")

	_, err := h.pool.Exec(r.Context(),
		"UPDATE subscriptions SET active = false, updated_at = now() WHERE id = $1",
		subID,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to pause subscription")
		return
	}

	h.logger.Info("subscription paused", "subscription_id", subID)
	writeJSON(w, http.StatusOK, map[string]string{"status": "paused"})
}

// ResumeSubscription sets a subscription to active.
func (h *AdminHandler) ResumeSubscription(w http.ResponseWriter, r *http.Request) {
	subID := chi.URLParam(r, "id")

	_, err := h.pool.Exec(r.Context(),
		"UPDATE subscriptions SET active = true, updated_at = now() WHERE id = $1",
		subID,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to resume subscription")
		return
	}

	h.logger.Info("subscription resumed", "subscription_id", subID)
	writeJSON(w, http.StatusOK, map[string]string{"status": "resumed"})
}

// ReplaySubscription resets the ack cursor to replay from a given position.
func (h *AdminHandler) ReplaySubscription(w http.ResponseWriter, r *http.Request) {
	subID := chi.URLParam(r, "id")

	fromStr := r.URL.Query().Get("from_position")
	if fromStr == "" {
		writeError(w, http.StatusBadRequest, "from_position query parameter required")
		return
	}

	fromPos, err := strconv.ParseUint(fromStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid from_position")
		return
	}

	_, err = h.queueClient.ResetCursor(r.Context(), &queuev1.ResetCursorRequest{
		SubscriptionId: subID,
		ToPosition:     fromPos,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to reset cursor")
		return
	}

	h.logger.Info("subscription replay started",
		"subscription_id", subID,
		"from_position", fromPos,
	)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":        "replaying",
		"from_position": fromPos,
	})
}

// AggregateHealth returns overall system health.
func (h *AdminHandler) AggregateHealth(w http.ResponseWriter, r *http.Request) {
	// Check database
	dbOK := true
	if err := h.pool.Ping(r.Context()); err != nil {
		dbOK = false
	}

	// Check queue service
	queueOK := true
	_, err := h.queueClient.ListReady(r.Context(), &queuev1.ListReadyRequest{Limit: 1})
	if err != nil {
		queueOK = false
	}

	status := http.StatusOK
	if !dbOK || !queueOK {
		status = http.StatusServiceUnavailable
	}

	writeJSON(w, status, map[string]interface{}{
		"database":      dbOK,
		"queue_service": queueOK,
	})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
