package model

import "time"

// SubscriberID uniquely identifies a subscriber (organization/partner).
type SubscriberID string

// SubscriptionID uniquely identifies a subscription within a subscriber.
type SubscriptionID string

// TenantID identifies a tenant in the upstream platform.
type TenantID string

// DeliveryMode specifies how events are delivered to subscribers.
type DeliveryMode string

const (
	DeliveryWebhook  DeliveryMode = "webhook"
	DeliveryStreaming DeliveryMode = "stream"
)

// Subscriber represents an external organization that receives events.
type Subscriber struct {
	ID         SubscriberID `json:"id" db:"id"`
	Name       string       `json:"name" db:"name"`
	WebhookURL string       `json:"webhook_url,omitempty" db:"webhook_url"`
	SigningKey  []byte       `json:"-" db:"signing_key"`
	StreamAuth string       `json:"stream_auth,omitempty" db:"stream_auth"`
	CreatedAt  time.Time    `json:"created_at" db:"created_at"`
	UpdatedAt  time.Time    `json:"updated_at" db:"updated_at"`
}

// Subscription defines what events a subscriber wants and how to deliver them.
type Subscription struct {
	ID            SubscriptionID `json:"id" db:"id"`
	SubscriberID  SubscriberID   `json:"subscriber_id" db:"subscriber_id"`
	EventTypes    []string       `json:"event_types" db:"event_types"`
	SourceFilter  string         `json:"source_filter" db:"source_filter"`
	TenantFilter  []TenantID     `json:"tenant_filter" db:"tenant_filter"`
	DeliveryMode  DeliveryMode   `json:"delivery_mode" db:"delivery_mode"`
	MaxBatchSize  int            `json:"max_batch_size" db:"max_batch_size"`
	MaxBatchBytes int            `json:"max_batch_bytes" db:"max_batch_bytes"`
	LingerMS      int            `json:"linger_ms" db:"linger_ms"`
	Active        bool           `json:"active" db:"active"`
	CreatedAt     time.Time      `json:"created_at" db:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at" db:"updated_at"`
}
