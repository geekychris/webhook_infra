package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/example/event-gateway/internal/entity"
	"github.com/example/event-gateway/internal/kafka"
	"github.com/example/event-gateway/internal/mapper"
	"github.com/example/event-gateway/internal/matcher"
	"github.com/example/event-gateway/internal/model"
	configv1 "github.com/example/event-gateway/internal/proto/config/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Router consumes ECST events from Kafka, matches them to subscriptions, maps
// them to CloudEvents, and publishes SendableEvents to the sendable-events topic.
type Router struct {
	consumer       *kafka.Consumer
	producer       *kafka.Producer
	baseMatcher    *matcher.Matcher
	entityMatcher  *matcher.EntityMatcher
	mapper         *mapper.Mapper
	configAddr     string
	logger         *slog.Logger

	mu   sync.RWMutex
	subs map[string]model.Subscription // keyed by subscription ID
}

// NewRouter creates a new Router with all required dependencies.
// resolver may be nil if entity-based routing is not configured.
func NewRouter(
	consumer *kafka.Consumer,
	producer *kafka.Producer,
	configAddr string,
	resolver *entity.Resolver,
	logger *slog.Logger,
) *Router {
	m := matcher.New()
	return &Router{
		consumer:      consumer,
		producer:      producer,
		baseMatcher:   m,
		entityMatcher: matcher.NewEntityMatcher(m, resolver, logger),
		mapper:        mapper.New(),
		configAddr:    configAddr,
		logger:        logger,
		subs:          make(map[string]model.Subscription),
	}
}

// Run starts the config synchronization loop and then begins consuming Kafka messages.
// It blocks until ctx is cancelled.
func (r *Router) Run(ctx context.Context) error {
	go r.configLoop(ctx)

	return r.consumer.Consume(ctx, r.processMessage)
}

// configLoop connects to the config-distributor gRPC stream, processes snapshots
// and deltas, and keeps the in-memory matcher up to date.
func (r *Router) configLoop(ctx context.Context) {
	for {
		if err := r.runConfigStream(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			r.logger.Error("config stream failed, reconnecting in 2s", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	}
}

func (r *Router) runConfigStream(ctx context.Context) error {
	conn, err := grpc.NewClient(r.configAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := configv1.NewConfigDistributorClient(conn)
	stream, err := client.Subscribe(ctx, &configv1.SubscribeRequest{})
	if err != nil {
		return err
	}

	for {
		update, err := stream.Recv()
		if err != nil {
			return err
		}
		r.applyUpdate(update)
	}
}

// applyUpdate processes a ConfigUpdate message and refreshes the matcher.
func (r *Router) applyUpdate(update *configv1.ConfigUpdate) {
	r.mu.Lock()
	defer r.mu.Unlock()

	switch update.GetChangeType() {
	case configv1.ConfigUpdate_SNAPSHOT:
		// Replace all subscriptions with the snapshot.
		r.subs = make(map[string]model.Subscription, len(update.GetSubscriptions()))
		for _, sc := range update.GetSubscriptions() {
			r.subs[sc.GetId()] = protoToSubscription(sc)
		}
		r.logger.Info("loaded config snapshot", "count", len(r.subs), "version", update.GetVersion())

	case configv1.ConfigUpdate_UPSERT:
		for _, sc := range update.GetSubscriptions() {
			r.subs[sc.GetId()] = protoToSubscription(sc)
		}
		r.logger.Info("applied config upsert", "count", len(update.GetSubscriptions()), "version", update.GetVersion())

	case configv1.ConfigUpdate_DELETE:
		for _, sc := range update.GetSubscriptions() {
			delete(r.subs, sc.GetId())
		}
		r.logger.Info("applied config delete", "count", len(update.GetSubscriptions()), "version", update.GetVersion())
	}

	// Rebuild the matcher from the current subscription map.
	all := make([]model.Subscription, 0, len(r.subs))
	for _, sub := range r.subs {
		all = append(all, sub)
	}
	r.baseMatcher.LoadSnapshot(all)
}

// processMessage handles a single Kafka message from the ecst-events topic.
func (r *Router) processMessage(ctx context.Context, key, value []byte) error {
	var event model.ECSTEvent
	if err := json.Unmarshal(value, &event); err != nil {
		r.logger.Error("failed to unmarshal ECST event", "error", err)
		return nil // skip malformed messages
	}

	matches := r.entityMatcher.MatchWithEntities(ctx, event)
	if len(matches) == 0 {
		return nil
	}

	ce, err := r.mapper.Map(event)
	if err != nil {
		r.logger.Error("failed to map event to CloudEvent", "event_id", event.ID, "error", err)
		return nil
	}

	for _, m := range matches {
		sendable := model.SendableEvent{
			SubscriptionID: m.SubscriptionID,
			SubscriberID:   m.SubscriberID,
			Event:          ce,
		}

		data, err := json.Marshal(sendable)
		if err != nil {
			r.logger.Error("failed to marshal sendable event", "subscription_id", m.SubscriptionID, "error", err)
			continue
		}

		if err := r.producer.Publish(ctx, []byte(m.SubscriptionID), data); err != nil {
			r.logger.Error("failed to publish sendable event",
				"subscription_id", m.SubscriptionID,
				"event_id", event.ID,
				"error", err,
			)
			continue
		}
	}

	return nil
}

// protoToSubscription converts a protobuf SubscriptionConfig to the internal model.
func protoToSubscription(sc *configv1.SubscriptionConfig) model.Subscription {
	tenantFilter := make([]model.TenantID, 0, len(sc.GetTenantFilter()))
	for _, t := range sc.GetTenantFilter() {
		tenantFilter = append(tenantFilter, model.TenantID(t))
	}

	return model.Subscription{
		ID:            model.SubscriptionID(sc.GetId()),
		SubscriberID:  model.SubscriberID(sc.GetSubscriberId()),
		EventTypes:    sc.GetEventTypes(),
		SourceFilter:  sc.GetSourceFilter(),
		TenantFilter:  tenantFilter,
		DeliveryMode:  model.DeliveryMode(sc.GetDeliveryMode()),
		MaxBatchSize:  int(sc.GetMaxBatchSize()),
		MaxBatchBytes: int(sc.GetMaxBatchBytes()),
		LingerMS:      int(sc.GetLingerMs()),
		Active:        sc.GetActive(),
	}
}
