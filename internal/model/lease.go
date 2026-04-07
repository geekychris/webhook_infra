package model

import "time"

// Epoch is a monotonically increasing fencing token for delivery leases.
type Epoch uint64

// DeliveryLease represents an exclusive ownership claim on a subscription's delivery.
type DeliveryLease struct {
	SubscriptionID SubscriptionID `json:"subscription_id"`
	HolderID       string         `json:"holder_id"`
	Epoch          Epoch          `json:"epoch"`
	AcquiredAt     time.Time      `json:"acquired_at"`
	ExpiresAt      time.Time      `json:"expires_at"`
	RenewedAt      time.Time      `json:"renewed_at"`
}
