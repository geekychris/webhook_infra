package model

import (
	"encoding/json"
	"time"
)

// ECSTEvent is the internal platform event envelope produced by upstream services.
type ECSTEvent struct {
	ID        string            `json:"event_id"`
	Type      string            `json:"event_type"`
	Source    string            `json:"source_service"`
	TenantID  TenantID          `json:"tenant_id"`
	EntityID  string            `json:"entity_id"`
	Timestamp time.Time         `json:"time"`
	Version   string            `json:"version"`
	Payload   json.RawMessage   `json:"payload"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// SendableEvent is the intermediate record published to the sendable-events Kafka topic.
// It carries a matched subscription ID and the already-mapped CloudEvent.
type SendableEvent struct {
	SubscriptionID SubscriptionID `json:"subscription_id"`
	SubscriberID   SubscriberID   `json:"subscriber_id"`
	Event          CloudEvent     `json:"event"`
}
