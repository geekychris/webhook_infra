package entity

import (
	"time"

	"github.com/example/event-gateway/internal/model"
)

const (
	EntityTypeUser    = "user"
	EntityTypeCompany = "company"
)

// EntityMapping represents a mapping from an entity (user, company, etc.) to a subscriber.
type EntityMapping struct {
	EntityType   string            `json:"entity_type"`
	EntityID     string            `json:"entity_id"`
	SubscriberID model.SubscriberID `json:"subscriber_id"`
	CreatedAt    time.Time         `json:"created_at"`
}

// EntityMappingEvent is published to the entity-mappings Kafka topic when mappings change.
type EntityMappingEvent struct {
	Action       string            `json:"action"` // "add" or "remove"
	EntityType   string            `json:"entity_type"`
	EntityID     string            `json:"entity_id"`
	SubscriberID model.SubscriberID `json:"subscriber_id"`
}
