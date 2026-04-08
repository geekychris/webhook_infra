package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/example/event-gateway/internal/model"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
)

// Handler provides HTTP handlers for the subscriber-registry API.
type Handler struct {
	store  *Store
	logger *slog.Logger
}

// NewHandler creates a new Handler.
func NewHandler(store *Store, logger *slog.Logger) *Handler {
	return &Handler{store: store, logger: logger}
}

// ---------------------------------------------------------------------------
// Subscribers
// ---------------------------------------------------------------------------

type createSubscriberRequest struct {
	ID         string `json:"id"`          // optional: custom ID; auto-generated if empty
	Name       string `json:"name"`
	WebhookURL string `json:"webhook_url"`
	StreamAuth string `json:"stream_auth"`
}

type updateSubscriberRequest struct {
	Name       string `json:"name"`
	WebhookURL string `json:"webhook_url"`
	StreamAuth string `json:"stream_auth"`
}

// CreateSubscriber handles POST /v1/subscribers.
func (h *Handler) CreateSubscriber(w http.ResponseWriter, r *http.Request) {
	var req createSubscriberRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}

	sub := &model.Subscriber{
		ID:         model.SubscriberID(req.ID),
		Name:       req.Name,
		WebhookURL: req.WebhookURL,
		StreamAuth: req.StreamAuth,
	}

	created, err := h.store.CreateSubscriber(r.Context(), sub)
	if err != nil {
		h.logger.Error("creating subscriber", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to create subscriber")
		return
	}

	writeJSON(w, http.StatusCreated, created)
}

// ListSubscribers handles GET /v1/subscribers.
func (h *Handler) ListSubscribers(w http.ResponseWriter, r *http.Request) {
	offset, limit := parsePagination(r)

	subs, err := h.store.ListSubscribers(r.Context(), offset, limit)
	if err != nil {
		h.logger.Error("listing subscribers", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list subscribers")
		return
	}
	if subs == nil {
		subs = []model.Subscriber{}
	}

	writeJSON(w, http.StatusOK, subs)
}

// GetSubscriber handles GET /v1/subscribers/{id}.
func (h *Handler) GetSubscriber(w http.ResponseWriter, r *http.Request) {
	id := model.SubscriberID(chi.URLParam(r, "id"))

	sub, err := h.store.GetSubscriber(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "subscriber not found")
			return
		}
		h.logger.Error("getting subscriber", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get subscriber")
		return
	}

	writeJSON(w, http.StatusOK, sub)
}

// UpdateSubscriber handles PUT /v1/subscribers/{id}.
func (h *Handler) UpdateSubscriber(w http.ResponseWriter, r *http.Request) {
	id := model.SubscriberID(chi.URLParam(r, "id"))

	var req updateSubscriberRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}

	// Fetch existing to preserve signing key.
	existing, err := h.store.GetSubscriber(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "subscriber not found")
			return
		}
		h.logger.Error("getting subscriber for update", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get subscriber")
		return
	}

	existing.Name = req.Name
	existing.WebhookURL = req.WebhookURL
	existing.StreamAuth = req.StreamAuth

	updated, err := h.store.UpdateSubscriber(r.Context(), existing)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "subscriber not found")
			return
		}
		h.logger.Error("updating subscriber", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to update subscriber")
		return
	}

	writeJSON(w, http.StatusOK, updated)
}

// DeleteSubscriber handles DELETE /v1/subscribers/{id}.
func (h *Handler) DeleteSubscriber(w http.ResponseWriter, r *http.Request) {
	id := model.SubscriberID(chi.URLParam(r, "id"))

	if err := h.store.DeleteSubscriber(r.Context(), id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "subscriber not found")
			return
		}
		h.logger.Error("deleting subscriber", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to delete subscriber")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// Subscriptions
// ---------------------------------------------------------------------------

type createSubscriptionRequest struct {
	ID            string             `json:"id"` // optional: custom ID
	EventTypes    []string           `json:"event_types"`
	SourceFilter  string             `json:"source_filter"`
	TenantFilter  []model.TenantID   `json:"tenant_filter"`
	DeliveryMode  model.DeliveryMode `json:"delivery_mode"`
	MaxBatchSize  int                `json:"max_batch_size"`
	MaxBatchBytes int                `json:"max_batch_bytes"`
	LingerMS      int                `json:"linger_ms"`
	Active        *bool              `json:"active"`
}

type updateSubscriptionRequest struct {
	EventTypes    []string           `json:"event_types"`
	SourceFilter  string             `json:"source_filter"`
	TenantFilter  []model.TenantID   `json:"tenant_filter"`
	DeliveryMode  model.DeliveryMode `json:"delivery_mode"`
	MaxBatchSize  int                `json:"max_batch_size"`
	MaxBatchBytes int                `json:"max_batch_bytes"`
	LingerMS      int                `json:"linger_ms"`
	Active        *bool              `json:"active"`
}

// CreateSubscription handles POST /v1/subscribers/{subscriberID}/subscriptions.
func (h *Handler) CreateSubscription(w http.ResponseWriter, r *http.Request) {
	subscriberID := model.SubscriberID(chi.URLParam(r, "subscriberID"))

	// Verify subscriber exists.
	if _, err := h.store.GetSubscriber(r.Context(), subscriberID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "subscriber not found")
			return
		}
		h.logger.Error("checking subscriber", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to verify subscriber")
		return
	}

	var req createSubscriptionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	active := true
	if req.Active != nil {
		active = *req.Active
	}
	if req.DeliveryMode == "" {
		req.DeliveryMode = model.DeliveryWebhook
	}
	if req.SourceFilter == "" {
		req.SourceFilter = "*"
	}
	if req.MaxBatchSize == 0 {
		req.MaxBatchSize = 100
	}
	if req.MaxBatchBytes == 0 {
		req.MaxBatchBytes = 1048576
	}
	if req.LingerMS == 0 {
		req.LingerMS = 100
	}

	sub := &model.Subscription{
		ID:            model.SubscriptionID(req.ID),
		SubscriberID:  subscriberID,
		EventTypes:    req.EventTypes,
		SourceFilter:  req.SourceFilter,
		TenantFilter:  req.TenantFilter,
		DeliveryMode:  req.DeliveryMode,
		MaxBatchSize:  req.MaxBatchSize,
		MaxBatchBytes: req.MaxBatchBytes,
		LingerMS:      req.LingerMS,
		Active:        active,
	}

	created, err := h.store.CreateSubscription(r.Context(), sub)
	if err != nil {
		h.logger.Error("creating subscription", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to create subscription")
		return
	}

	writeJSON(w, http.StatusCreated, created)
}

// ListSubscriptions handles GET /v1/subscribers/{subscriberID}/subscriptions.
func (h *Handler) ListSubscriptions(w http.ResponseWriter, r *http.Request) {
	subscriberID := model.SubscriberID(chi.URLParam(r, "subscriberID"))
	offset, limit := parsePagination(r)

	subs, err := h.store.ListSubscriptions(r.Context(), subscriberID, offset, limit)
	if err != nil {
		h.logger.Error("listing subscriptions", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list subscriptions")
		return
	}
	if subs == nil {
		subs = []model.Subscription{}
	}

	writeJSON(w, http.StatusOK, subs)
}

// GetSubscription handles GET /v1/subscriptions/{id}.
func (h *Handler) GetSubscription(w http.ResponseWriter, r *http.Request) {
	id := model.SubscriptionID(chi.URLParam(r, "id"))

	sub, err := h.store.GetSubscription(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "subscription not found")
			return
		}
		h.logger.Error("getting subscription", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get subscription")
		return
	}

	writeJSON(w, http.StatusOK, sub)
}

// UpdateSubscription handles PUT /v1/subscriptions/{id}.
func (h *Handler) UpdateSubscription(w http.ResponseWriter, r *http.Request) {
	id := model.SubscriptionID(chi.URLParam(r, "id"))

	var req updateSubscriptionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	// Fetch existing subscription.
	existing, err := h.store.GetSubscription(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "subscription not found")
			return
		}
		h.logger.Error("getting subscription for update", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get subscription")
		return
	}

	// Apply updates.
	if req.EventTypes != nil {
		existing.EventTypes = req.EventTypes
	}
	if req.SourceFilter != "" {
		existing.SourceFilter = req.SourceFilter
	}
	if req.TenantFilter != nil {
		existing.TenantFilter = req.TenantFilter
	}
	if req.DeliveryMode != "" {
		existing.DeliveryMode = req.DeliveryMode
	}
	if req.MaxBatchSize != 0 {
		existing.MaxBatchSize = req.MaxBatchSize
	}
	if req.MaxBatchBytes != 0 {
		existing.MaxBatchBytes = req.MaxBatchBytes
	}
	if req.LingerMS != 0 {
		existing.LingerMS = req.LingerMS
	}
	if req.Active != nil {
		existing.Active = *req.Active
	}

	updated, err := h.store.UpdateSubscription(r.Context(), existing)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "subscription not found")
			return
		}
		h.logger.Error("updating subscription", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to update subscription")
		return
	}

	writeJSON(w, http.StatusOK, updated)
}

// DeleteSubscription handles DELETE /v1/subscriptions/{id}.
func (h *Handler) DeleteSubscription(w http.ResponseWriter, r *http.Request) {
	id := model.SubscriptionID(chi.URLParam(r, "id"))

	if err := h.store.DeleteSubscription(r.Context(), id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "subscription not found")
			return
		}
		h.logger.Error("deleting subscription", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to delete subscription")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func parsePagination(r *http.Request) (offset, limit int) {
	offset = 0
	limit = 50

	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}
	return offset, limit
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Best effort; headers already sent.
		slog.Error("encoding JSON response", "error", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
