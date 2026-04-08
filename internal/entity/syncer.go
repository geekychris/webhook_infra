package entity

import (
	"context"
	"encoding/json"
	"log/slog"
)

// Syncer consumes entity-mapping events from Kafka and updates the Redis resolver.
type Syncer struct {
	resolver *Resolver
	logger   *slog.Logger
}

// NewSyncer creates a Syncer that writes mapping changes to the given resolver.
func NewSyncer(resolver *Resolver, logger *slog.Logger) *Syncer {
	return &Syncer{
		resolver: resolver,
		logger:   logger,
	}
}

// ProcessMessage handles a single Kafka message containing an EntityMappingEvent.
// Invalid messages are logged and skipped (nil error returned).
func (s *Syncer) ProcessMessage(ctx context.Context, key, value []byte) error {
	var evt EntityMappingEvent
	if err := json.Unmarshal(value, &evt); err != nil {
		s.logger.Error("failed to unmarshal entity mapping event",
			"error", err,
			"raw", string(value),
		)
		return nil
	}

	switch evt.Action {
	case "add":
		if err := s.resolver.AddMapping(ctx, evt.EntityType, evt.EntityID, evt.SubscriberID); err != nil {
			s.logger.Error("failed to add entity mapping",
				"entity_type", evt.EntityType,
				"entity_id", evt.EntityID,
				"subscriber_id", evt.SubscriberID,
				"error", err,
			)
			return nil
		}
	case "remove":
		if err := s.resolver.RemoveMapping(ctx, evt.EntityType, evt.EntityID, evt.SubscriberID); err != nil {
			s.logger.Error("failed to remove entity mapping",
				"entity_type", evt.EntityType,
				"entity_id", evt.EntityID,
				"subscriber_id", evt.SubscriberID,
				"error", err,
			)
			return nil
		}
	default:
		s.logger.Warn("unknown entity mapping action",
			"action", evt.Action,
			"entity_type", evt.EntityType,
			"entity_id", evt.EntityID,
		)
	}

	return nil
}
