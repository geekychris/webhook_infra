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

// NewEntityStore creates a new EntityStore backed by the given connection pool
// and optional Kafka producer (can be nil for tests).
func NewEntityStore(pool *pgxpool.Pool, producer *kafka.Producer) *EntityStore {
	return &EntityStore{pool: pool, producer: producer}
}

// CreateMapping inserts a new entity mapping. If the mapping already exists it
// is a no-op (ON CONFLICT DO NOTHING). An "add" event is published to Kafka
// when a new row is actually inserted.
func (s *EntityStore) CreateMapping(ctx context.Context, entityType, entityID string, subscriberID string) (*entity.EntityMapping, error) {
	row := s.pool.QueryRow(ctx,
		`INSERT INTO entity_mappings (entity_type, entity_id, subscriber_id)
		 VALUES ($1, $2, $3)
		 ON CONFLICT DO NOTHING
		 RETURNING entity_type, entity_id, subscriber_id, created_at`,
		entityType, entityID, subscriberID,
	)

	var m entity.EntityMapping
	err := row.Scan(&m.EntityType, &m.EntityID, &m.SubscriberID, &m.CreatedAt)
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

// DeleteMapping removes an entity mapping. A "remove" event is published to Kafka.
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
		`SELECT entity_type, entity_id, subscriber_id, created_at
		 FROM entity_mappings
		 WHERE entity_type = $1 AND entity_id = $2
		 ORDER BY created_at`,
		entityType, entityID,
	)
	if err != nil {
		return nil, fmt.Errorf("querying entity mappings by entity: %w", err)
	}
	defer rows.Close()

	var out []entity.EntityMapping
	for rows.Next() {
		var m entity.EntityMapping
		if err := rows.Scan(&m.EntityType, &m.EntityID, &m.SubscriberID, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning entity mapping: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// ListBySubscriber returns all mappings for the given subscriber ID.
func (s *EntityStore) ListBySubscriber(ctx context.Context, subscriberID string) ([]entity.EntityMapping, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT entity_type, entity_id, subscriber_id, created_at
		 FROM entity_mappings
		 WHERE subscriber_id = $1
		 ORDER BY created_at`,
		subscriberID,
	)
	if err != nil {
		return nil, fmt.Errorf("querying entity mappings by subscriber: %w", err)
	}
	defer rows.Close()

	var out []entity.EntityMapping
	for rows.Next() {
		var m entity.EntityMapping
		if err := rows.Scan(&m.EntityType, &m.EntityID, &m.SubscriberID, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning entity mapping: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// ListAll returns all entity mappings. Used for cache warming.
func (s *EntityStore) ListAll(ctx context.Context) ([]entity.EntityMapping, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT entity_type, entity_id, subscriber_id, created_at
		 FROM entity_mappings
		 ORDER BY created_at`,
	)
	if err != nil {
		return nil, fmt.Errorf("querying all entity mappings: %w", err)
	}
	defer rows.Close()

	var out []entity.EntityMapping
	for rows.Next() {
		var m entity.EntityMapping
		if err := rows.Scan(&m.EntityType, &m.EntityID, &m.SubscriberID, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning entity mapping: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// publishEvent marshals an EntityMappingEvent and publishes it to Kafka,
// keyed by entity_type:entity_id for consistent partitioning.
func (s *EntityStore) publishEvent(ctx context.Context, evt entity.EntityMappingEvent) error {
	value, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshalling entity mapping event: %w", err)
	}
	key := []byte(evt.EntityType + ":" + evt.EntityID)
	return s.producer.Publish(ctx, key, value)
}
