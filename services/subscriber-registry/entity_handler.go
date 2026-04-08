package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/example/event-gateway/internal/entity"
	"github.com/example/event-gateway/internal/model"
	"github.com/go-chi/chi/v5"
)

// EntityHandler provides HTTP handlers for entity mapping CRUD + CSV.
type EntityHandler struct {
	store  *EntityStore
	logger *slog.Logger
}

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
	if req.EntityType == "" || req.EntityID == "" || req.SubscriberID == "" {
		writeError(w, http.StatusBadRequest, "entity_type, entity_id, and subscriber_id are required")
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

// UpdateMapping handles PUT /v1/entity-mappings — touches updated_at.
func (h *EntityHandler) UpdateMapping(w http.ResponseWriter, r *http.Request) {
	var req entityMappingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.EntityType == "" || req.EntityID == "" || req.SubscriberID == "" {
		writeError(w, http.StatusBadRequest, "entity_type, entity_id, and subscriber_id are required")
		return
	}

	mapping, err := h.store.UpdateMapping(r.Context(), req.EntityType, req.EntityID, req.SubscriberID)
	if err != nil {
		writeError(w, http.StatusNotFound, "mapping not found")
		return
	}

	writeJSON(w, http.StatusOK, mapping)
}

// DeleteMapping handles DELETE /v1/entity-mappings.
func (h *EntityHandler) DeleteMapping(w http.ResponseWriter, r *http.Request) {
	var req entityMappingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.EntityType == "" || req.EntityID == "" || req.SubscriberID == "" {
		writeError(w, http.StatusBadRequest, "entity_type, entity_id, and subscriber_id are required")
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
		writeError(w, http.StatusInternalServerError, "failed to list entity mappings")
		return
	}
	if mappings == nil {
		mappings = []entity.EntityMapping{}
	}
	writeJSON(w, http.StatusOK, mappings)
}

// ListAll handles GET /v1/entity-mappings/all.
func (h *EntityHandler) ListAll(w http.ResponseWriter, r *http.Request) {
	mappings, err := h.store.ListAll(r.Context())
	if err != nil {
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
		writeError(w, http.StatusInternalServerError, "failed to list entity mappings")
		return
	}
	if mappings == nil {
		mappings = []entity.EntityMapping{}
	}
	writeJSON(w, http.StatusOK, mappings)
}

// ExportCSV handles GET /v1/entity-mappings/export.csv — downloads all mappings as CSV.
func (h *EntityHandler) ExportCSV(w http.ResponseWriter, r *http.Request) {
	mappings, err := h.store.ListAll(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to export")
		return
	}

	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", "attachment; filename=entity_mappings.csv")

	cw := csv.NewWriter(w)
	cw.Write([]string{"entity_type", "entity_id", "subscriber_id", "created_at", "updated_at"})
	for _, m := range mappings {
		cw.Write([]string{
			m.EntityType,
			m.EntityID,
			string(m.SubscriberID),
			m.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
			m.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
		})
	}
	cw.Flush()
}

// ImportCSV handles POST /v1/entity-mappings/import — uploads CSV to create/update mappings.
// CSV format: entity_type,entity_id,subscriber_id (header row optional).
func (h *EntityHandler) ImportCSV(w http.ResponseWriter, r *http.Request) {
	// Accept multipart form or raw CSV body.
	var reader io.Reader

	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
		file, _, err := r.FormFile("file")
		if err != nil {
			writeError(w, http.StatusBadRequest, "missing file field in multipart form")
			return
		}
		defer file.Close()
		reader = file
	} else {
		reader = r.Body
	}

	cr := csv.NewReader(reader)
	cr.TrimLeadingSpace = true

	records, err := cr.ReadAll()
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid CSV: "+err.Error())
		return
	}

	if len(records) == 0 {
		writeJSON(w, http.StatusOK, map[string]int{"imported": 0})
		return
	}

	// Skip header row if it looks like one.
	start := 0
	if len(records[0]) >= 3 && strings.EqualFold(records[0][0], "entity_type") {
		start = 1
	}

	var mappings []entity.EntityMapping
	for i := start; i < len(records); i++ {
		row := records[i]
		if len(row) < 3 {
			continue
		}
		entityType := strings.TrimSpace(row[0])
		entityID := strings.TrimSpace(row[1])
		subscriberID := strings.TrimSpace(row[2])
		if entityType == "" || entityID == "" || subscriberID == "" {
			continue
		}
		mappings = append(mappings, entity.EntityMapping{
			EntityType:   entityType,
			EntityID:     entityID,
			SubscriberID: model.SubscriberID(subscriberID),
		})
	}

	count, err := h.store.BulkCreateMappings(r.Context(), mappings)
	if err != nil {
		h.logger.Error("CSV import failed", "error", err)
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("import failed after %d rows: %v", count, err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]int{"imported": count, "total_rows": len(mappings)})
}
