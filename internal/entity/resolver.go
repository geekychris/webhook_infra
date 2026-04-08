package entity

import (
	"context"

	"github.com/example/event-gateway/internal/model"
	"github.com/redis/go-redis/v9"
)

// Resolver performs Redis-backed lookups from entity (type + ID) to subscriber IDs.
type Resolver struct {
	rdb *redis.Client
}

// NewResolver creates a Resolver backed by the given Redis client.
func NewResolver(rdb *redis.Client) *Resolver {
	return &Resolver{rdb: rdb}
}

// Resolve returns all subscriber IDs mapped to the given entity.
func (r *Resolver) Resolve(ctx context.Context, entityType, entityID string) ([]model.SubscriberID, error) {
	members, err := r.rdb.SMembers(ctx, entityKey(entityType, entityID)).Result()
	if err != nil {
		return nil, err
	}
	ids := make([]model.SubscriberID, len(members))
	for i, m := range members {
		ids[i] = model.SubscriberID(m)
	}
	return ids, nil
}

// AddMapping adds a subscriber ID to the entity's Redis set.
func (r *Resolver) AddMapping(ctx context.Context, entityType, entityID string, subscriberID model.SubscriberID) error {
	return r.rdb.SAdd(ctx, entityKey(entityType, entityID), string(subscriberID)).Err()
}

// RemoveMapping removes a subscriber ID from the entity's Redis set.
func (r *Resolver) RemoveMapping(ctx context.Context, entityType, entityID string, subscriberID model.SubscriberID) error {
	return r.rdb.SRem(ctx, entityKey(entityType, entityID), string(subscriberID)).Err()
}

// WarmCache bulk-populates the resolver cache from a slice of entity mappings using a pipeline.
// This is additive — it does not remove stale entries. Use FlushAndReload for a full reset.
func (r *Resolver) WarmCache(ctx context.Context, mappings []EntityMapping) error {
	if len(mappings) == 0 {
		return nil
	}
	pipe := r.rdb.Pipeline()
	for _, m := range mappings {
		pipe.SAdd(ctx, entityKey(m.EntityType, m.EntityID), string(m.SubscriberID))
	}
	_, err := pipe.Exec(ctx)
	return err
}

// FlushAndReload deletes all entity:* keys from Redis and repopulates from the
// provided mappings. This is used for periodic full reconciliation to heal any
// drift between the database and Redis cache.
func (r *Resolver) FlushAndReload(ctx context.Context, mappings []EntityMapping) error {
	// Delete all entity keys using SCAN to avoid blocking Redis with KEYS *
	var cursor uint64
	for {
		keys, nextCursor, err := r.rdb.Scan(ctx, cursor, "entity:*", 1000).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			r.rdb.Del(ctx, keys...)
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	// Repopulate from the authoritative source
	return r.WarmCache(ctx, mappings)
}

func entityKey(entityType, entityID string) string {
	return "entity:" + entityType + ":" + entityID
}
