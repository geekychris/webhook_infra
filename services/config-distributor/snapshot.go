package main

import (
	"context"

	configv1 "github.com/example/event-gateway/internal/proto/config/v1"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LoadSnapshot queries PostgreSQL for all active subscriptions (joined with
// subscribers) and returns them as a slice of protobuf SubscriptionConfig messages.
func LoadSnapshot(ctx context.Context, pool *pgxpool.Pool) ([]*configv1.SubscriptionConfig, error) {
	rows, err := pool.Query(ctx, `
		SELECT s.id, s.subscriber_id, s.event_types, s.source_filter, s.tenant_filter,
		       s.delivery_mode, s.max_batch_size, s.max_batch_bytes, s.linger_ms, s.active
		FROM subscriptions s
		JOIN subscribers sub ON sub.id = s.subscriber_id
		WHERE s.active = true
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var configs []*configv1.SubscriptionConfig
	for rows.Next() {
		var (
			id, subscriberID, sourceFilter, deliveryMode string
			eventTypes, tenantFilter                     []string
			maxBatchSize, maxBatchBytes, lingerMs        int32
			active                                       bool
		)
		if err := rows.Scan(
			&id, &subscriberID, &eventTypes, &sourceFilter, &tenantFilter,
			&deliveryMode, &maxBatchSize, &maxBatchBytes, &lingerMs, &active,
		); err != nil {
			return nil, err
		}

		configs = append(configs, &configv1.SubscriptionConfig{
			Id:            id,
			SubscriberId:  subscriberID,
			EventTypes:    eventTypes,
			SourceFilter:  sourceFilter,
			TenantFilter:  tenantFilter,
			DeliveryMode:  deliveryMode,
			MaxBatchSize:  maxBatchSize,
			MaxBatchBytes: maxBatchBytes,
			LingerMs:      lingerMs,
			Active:        active,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return configs, nil
}
