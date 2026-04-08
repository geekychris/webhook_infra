// Package readyqueue provides a Redis-backed notification mechanism for
// signaling that subscriptions have pending events ready for delivery.
//
// This replaces polling-based discovery (scanning all queues) with an
// event-driven approach:
//
//   - Queue-writer calls Signal() after appending events to a subscription queue.
//   - Dispatcher calls Wait() which blocks until a subscription is ready.
//   - Fair ordering: subscriptions are served in the order they became ready
//     (sorted set scored by timestamp).
//
// Redis key: "ready" — a sorted set where:
//   - member = subscription_id
//   - score  = unix millisecond timestamp when first signaled
//
// This enables near real-time delivery without busy-spinning or RocksDB scanning,
// and scales to tens of thousands of subscriptions.
package readyqueue

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

const readyKey = "dispatch:ready"

// Queue manages the "ready to deliver" signal between queue-writers and dispatchers.
type Queue struct {
	rdb *redis.Client
}

// New creates a ready queue backed by the given Redis client.
func New(rdb *redis.Client) *Queue {
	return &Queue{rdb: rdb}
}

// Signal marks a subscription as having pending events ready for delivery.
// Uses NX (only add if not already present) so a subscription that's already
// in the ready set doesn't get its timestamp bumped — preserving FIFO order.
//
// This is called by the queue-writer after a successful Append.
func (q *Queue) Signal(ctx context.Context, subscriptionID string) error {
	score := float64(time.Now().UnixMilli())
	return q.rdb.ZAddNX(ctx, readyKey, redis.Z{
		Score:  score,
		Member: subscriptionID,
	}).Err()
}

// Wait blocks until a subscription is ready for delivery, removes it from the
// ready set, and returns its ID. If the context is cancelled, returns "".
//
// Uses BZPOPMIN for O(1) blocking pop of the oldest-waiting subscription.
// The timeout parameter controls how long to block before returning empty
// (to allow the caller to check for shutdown).
//
// This is called by the dispatcher in a tight loop.
func (q *Queue) Wait(ctx context.Context, timeout time.Duration) (subscriptionID string, err error) {
	result, err := q.rdb.BZPopMin(ctx, timeout, readyKey).Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil // timeout, no work
		}
		return "", err
	}
	// BZPopMin returns [key, member, score]
	return result.Member.(string), nil
}

// Requeue puts a subscription back in the ready set (e.g., after a partial
// delivery or when we know more events exist). Uses NX to preserve order.
func (q *Queue) Requeue(ctx context.Context, subscriptionID string) error {
	return q.Signal(ctx, subscriptionID)
}

// Remove explicitly removes a subscription from the ready set (e.g., when
// the subscription is paused or deleted).
func (q *Queue) Remove(ctx context.Context, subscriptionID string) error {
	return q.rdb.ZRem(ctx, readyKey, subscriptionID).Err()
}

// Len returns the number of subscriptions currently in the ready set.
func (q *Queue) Len(ctx context.Context) (int64, error) {
	return q.rdb.ZCard(ctx, readyKey).Result()
}

// Drain returns up to `limit` subscription IDs from the ready set without
// blocking. Used for batch dispatch when multiple subscriptions are ready.
func (q *Queue) Drain(ctx context.Context, limit int) ([]string, error) {
	// ZPOPMIN returns members with lowest scores (oldest-waiting first)
	results, err := q.rdb.ZPopMin(ctx, readyKey, int64(limit)).Result()
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(results))
	for _, z := range results {
		ids = append(ids, z.Member.(string))
	}
	return ids, nil
}

// Notify publishes to a per-subscription Redis channel so streaming sessions
// can wake immediately when new events are appended. This is separate from the
// ready sorted set (which is for webhook dispatchers). Streaming sessions
// subscribe to "notify:<subscription_id>".
func (q *Queue) Notify(ctx context.Context, subscriptionID string) error {
	return q.rdb.Publish(ctx, "notify:"+subscriptionID, "1").Err()
}

// Subscribe returns a channel that receives a message whenever new events are
// available for the given subscription. Used by the stream-gateway to replace
// polling. Close the returned channel's subscription when done.
func (q *Queue) Subscribe(ctx context.Context, subscriptionID string) <-chan struct{} {
	ch := make(chan struct{}, 1) // buffered to avoid blocking publisher
	pubsub := q.rdb.Subscribe(ctx, "notify:"+subscriptionID)

	go func() {
		defer pubsub.Close()
		defer close(ch)
		msgCh := pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-msgCh:
				if !ok {
					return
				}
				// Non-blocking send — if channel already has a signal, skip.
				select {
				case ch <- struct{}{}:
				default:
				}
			}
		}
	}()

	return ch
}
