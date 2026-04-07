package model

// QueuePosition is a monotonically increasing sequence number within a subscription queue.
type QueuePosition uint64

// QueueEntry represents a single event stored in a subscription's durable queue.
type QueueEntry struct {
	Position       QueuePosition  `json:"position"`
	SubscriptionID SubscriptionID `json:"subscription_id"`
	Event          CloudEvent     `json:"event"`
	EnqueuedAt     int64          `json:"enqueued_at"` // unix nanos
}

// SubscriptionState tracks the delivery state for a subscription queue.
type SubscriptionState struct {
	SubscriptionID     SubscriptionID `json:"subscription_id"`
	TailSeq            QueuePosition  `json:"tail_seq"`
	AckedSeq           QueuePosition  `json:"acked_seq"`
	LeaseEpoch         Epoch          `json:"lease_epoch"`
	ConsecutiveFailures int           `json:"consecutive_failures"`
}
