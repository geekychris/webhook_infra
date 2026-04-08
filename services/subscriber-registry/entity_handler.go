package main

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/example/event-gateway/internal/entity"
	"github.com/go-chi/chi/v5"
)

// EntityHandler provides HTTP handlers for entity mapping CRUD.
type EntityHandler struct {
	store  *EntityStore
	logger *slog.Logger
}

// NewEntityHandler creates a new EntityHandler.
func NewEntityHandler(store *EntityStore, logger *slog.Logger) *EntityHandler {
	return &EntityHandler{store: store, logger: logger}
}

type entityMappingRequest struct {
	EntityType   string `json:"entity_type"`
	EntityID     string `json:"entity_id"`
	SubscriberID string `json:"subscriber_id"`
}

// CreateMapping handles POST /v1/entity-mappings.
func (h *EntityHandler) CreateMapping(w http.ResponseWriter, r *http.Request) {
	var req entityMappingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.EntityType == "" {
		writeError(w, http.StatusBadRequest, "entity_type is required")
		return
	}
	if req.EntityID == "" {
		writeError(w, http.StatusBadRequest, "entity_id is required")
		return
	}
	if req.SubscriberID == "" {
		writeError(w, http.StatusBadRequest, "subscriber_id is required")
		return
	}

	mapping, err := h.store.CreateMapping(r.Context(), req.EntityType, req.EntityID, req.SubscriberID)
	if err != nil {
		h.logger.Error("creating entity mapping", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to create entity mapping")
		return
	}

	writeJSON(w, http.StatusCreated, mapping)
}

// DeleteMapping handles DELETE /v1/entity-mappings.
func (h *EntityHandler) DeleteMapping(w http.ResponseWriter, r *http.Request) {
	var req entityMappingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.EntityType == "" {
		writeError(w, http.StatusBadRequest, "entity_type is required")
		return
	}
	if req.EntityID == "" {
		writeError(w, http.StatusBadRequest, "entity_id is required")
		return
	}
	if req.SubscriberID == "" {
		writeError(w, http.StatusBadRequest, "subscriber_id is required")
		return
	}

	if err := h.store.DeleteMapping(r.Context(), req.EntityType, req.EntityID, req.SubscriberID); err != nil {
		h.logger.Error("deleting entity mapping", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to delete entity mapping")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// ListMappings handles GET /v1/entity-mappings?entity_type=user&entity_id=u123.
func (h *EntityHandler) ListMappings(w http.ResponseWriter, r *http.Request) {
	entityType := r.URL.Query().Get("entity_type")
	entityID := r.URL.Query().Get("entity_id")

	if entityType == "" || entityID == "" {
		writeError(w, http.StatusBadRequest, "entity_type and entity_id query parameters are required")
		return
	}

	mappings, err := h.store.ListByEntity(r.Context(), entityType, entityID)
	if err != nil {
		h.logger.Error("listing entity mappings", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list entity mappings")
		return
	}
	if mappings == nil {
		mappings = []entity.EntityMapping{}
	}

	writeJSON(w, http.StatusOK, mappings)
}

// ListAll handles GET /v1/entity-mappings/all — returns every mapping in the database.
// Used by the ingestion-router's cache warmer for startup and periodic reconciliation.
func (h *EntityHandler) ListAll(w http.ResponseWriter, r *http.Request) {
	mappings, err := h.store.ListAll(r.Context())
	if err != nil {
		h.logger.Error("listing all entity mappings", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list entity mappings")
		return
	}
	if mappings == nil {
		mappings = []entity.EntityMapping{}
	}

	writeJSON(w, http.StatusOK, mappings)
}

// ListBySubscriber handles GET /v1/subscribers/{id}/entity-mappings.
func (h *EntityHandler) ListBySubscriber(w http.ResponseWriter, r *http.Request) {
	subscriberID := chi.URLParam(r, "id")

	mappings, err := h.store.ListBySubscriber(r.Context(), subscriberID)
	if err != nil {
		h.logger.Error("listing entity mappings by subscriber", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list entity mappings")
		return
	}
	if mappings == nil {
		mappings = []entity.EntityMapping{}
	}

	writeJSON(w, http.StatusOK, mappings)
}
