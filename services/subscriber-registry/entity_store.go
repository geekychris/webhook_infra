package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/example/event-gateway/internal/entity"
	"github.com/example/event-gateway/internal/kafka"
	"github.com/example/event-gateway/internal/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EntityStore provides persistence for entity mappings.
type EntityStore struct {
	pool     *pgxpool.Pool
	producer *kafka.Producer
}

func NewEntityStore(pool *pgxpool.Pool, producer *kafka.Producer) *EntityStore {
	return &EntityStore{pool: pool, producer: producer}
}

// CreateMapping inserts or updates an entity mapping. Uses ON CONFLICT to upsert
// and update the updated_at timestamp on re-insert.
func (s *EntityStore) CreateMapping(ctx context.Context, entityType, entityID string, subscriberID string) (*entity.EntityMapping, error) {
	row := s.pool.QueryRow(ctx,
		`INSERT INTO entity_mappings (entity_type, entity_id, subscriber_id, updated_at)
		 VALUES ($1, $2, $3, now())
		 ON CONFLICT (entity_type, entity_id, subscriber_id)
		 DO UPDATE SET updated_at = now()
		 RETURNING entity_type, entity_id, subscriber_id, created_at, updated_at`,
		entityType, entityID, subscriberID,
	)

	var m entity.EntityMapping
	err := row.Scan(&m.EntityType, &m.EntityID, &m.SubscriberID, &m.CreatedAt, &m.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("inserting entity mapping: %w", err)
	}

	if s.producer != nil {
		evt := entity.EntityMappingEvent{
			Action:       "add",
			EntityType:   m.EntityType,
			EntityID:     m.EntityID,
			SubscriberID: m.SubscriberID,
		}
		if err := s.publishEvent(ctx, evt); err != nil {
			return &m, fmt.Errorf("publishing add event: %w", err)
		}
	}

	return &m, nil
}

// UpdateMapping updates the updated_at timestamp for an existing mapping.
func (s *EntityStore) UpdateMapping(ctx context.Context, entityType, entityID, subscriberID string) (*entity.EntityMapping, error) {
	row := s.pool.QueryRow(ctx,
		`UPDATE entity_mappings SET updated_at = now()
		 WHERE entity_type = $1 AND entity_id = $2 AND subscriber_id = $3
		 RETURNING entity_type, entity_id, subscriber_id, created_at, updated_at`,
		entityType, entityID, subscriberID,
	)

	var m entity.EntityMapping
	err := row.Scan(&m.EntityType, &m.EntityID, &m.SubscriberID, &m.CreatedAt, &m.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("updating entity mapping: %w", err)
	}
	return &m, nil
}

// DeleteMapping removes an entity mapping.
func (s *EntityStore) DeleteMapping(ctx context.Context, entityType, entityID, subscriberID string) error {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM entity_mappings
		 WHERE entity_type = $1 AND entity_id = $2 AND subscriber_id = $3`,
		entityType, entityID, subscriberID,
	)
	if err != nil {
		return fmt.Errorf("deleting entity mapping: %w", err)
	}

	if tag.RowsAffected() > 0 && s.producer != nil {
		evt := entity.EntityMappingEvent{
			Action:       "remove",
			EntityType:   entityType,
			EntityID:     entityID,
			SubscriberID: model.SubscriberID(subscriberID),
		}
		if err := s.publishEvent(ctx, evt); err != nil {
			return fmt.Errorf("publishing remove event: %w", err)
		}
	}

	return nil
}

// ListByEntity returns all mappings for the given entity type and ID.
func (s *EntityStore) ListByEntity(ctx context.Context, entityType, entityID string) ([]entity.EntityMapping, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT entity_type, entity_id, subscriber_id, created_at, COALESCE(updated_at, created_at)
		 FROM entity_mappings
		 WHERE entity_type = $1 AND entity_id = $2
		 ORDER BY created_at`,
		entityType, entityID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []entity.EntityMapping
	for rows.Next() {
		var m entity.EntityMapping
		if err := rows.Scan(&m.EntityType, &m.EntityID, &m.SubscriberID, &m.CreatedAt, &m.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// ListBySubscriber returns all mappings for the given subscriber ID.
func (s *EntityStore) ListBySubscriber(ctx context.Context, subscriberID string) ([]entity.EntityMapping, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT entity_type, entity_id, subscriber_id, created_at, COALESCE(updated_at, created_at)
		 FROM entity_mappings
		 WHERE subscriber_id = $1
		 ORDER BY created_at`,
		subscriberID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []entity.EntityMapping
	for rows.Next() {
		var m entity.EntityMapping
		if err := rows.Scan(&m.EntityType, &m.EntityID, &m.SubscriberID, &m.CreatedAt, &m.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// ListAll returns all entity mappings. Used for cache warming.
func (s *EntityStore) ListAll(ctx context.Context) ([]entity.EntityMapping, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT entity_type, entity_id, subscriber_id, created_at, COALESCE(updated_at, created_at)
		 FROM entity_mappings
		 ORDER BY entity_type, entity_id, subscriber_id`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []entity.EntityMapping
	for rows.Next() {
		var m entity.EntityMapping
		if err := rows.Scan(&m.EntityType, &m.EntityID, &m.SubscriberID, &m.CreatedAt, &m.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// BulkCreateMappings inserts multiple mappings in a single transaction.
// Used by CSV import. Returns the count of rows inserted.
func (s *EntityStore) BulkCreateMappings(ctx context.Context, mappings []entity.EntityMapping) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	count := 0
	for _, m := range mappings {
		tag, err := tx.Exec(ctx,
			`INSERT INTO entity_mappings (entity_type, entity_id, subscriber_id, updated_at)
			 VALUES ($1, $2, $3, now())
			 ON CONFLICT (entity_type, entity_id, subscriber_id)
			 DO UPDATE SET updated_at = now()`,
			m.EntityType, m.EntityID, string(m.SubscriberID),
		)
		if err != nil {
			return count, fmt.Errorf("inserting row %d: %w", count, err)
		}
		count += int(tag.RowsAffected())
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}

	// Publish Kafka events for each mapping
	if s.producer != nil {
		for _, m := range mappings {
			evt := entity.EntityMappingEvent{
				Action:       "add",
				EntityType:   m.EntityType,
				EntityID:     m.EntityID,
				SubscriberID: m.SubscriberID,
			}
			s.publishEvent(ctx, evt) // best-effort
		}
	}

	return count, nil
}

func (s *EntityStore) publishEvent(ctx context.Context, evt entity.EntityMappingEvent) error {
	value, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	key := []byte(evt.EntityType + ":" + evt.EntityID)
	return s.producer.Publish(ctx, key, value)
}
