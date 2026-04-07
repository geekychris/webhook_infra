package main

import (
	"context"
	"log/slog"
	"sync/atomic"

	configv1 "github.com/example/event-gateway/internal/proto/config/v1"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements configv1.ConfigDistributorServer.
type Server struct {
	configv1.UnimplementedConfigDistributorServer
	pool    *pgxpool.Pool
	watcher *Watcher
	logger  *slog.Logger
	version atomic.Uint64
}

// NewServer creates a new config distributor gRPC server.
func NewServer(pool *pgxpool.Pool, watcher *Watcher, logger *slog.Logger) *Server {
	s := &Server{
		pool:    pool,
		watcher: watcher,
		logger:  logger,
	}
	s.version.Store(0)
	return s
}

// Subscribe implements the server-streaming Subscribe RPC.
func (s *Server) Subscribe(req *configv1.SubscribeRequest, stream grpc.ServerStreamingServer[configv1.ConfigUpdate]) error {
	ctx := stream.Context()
	s.logger.Info("new config subscriber connected", "subscriber_ids_filter", req.GetSubscriberIds())

	// Load the current snapshot of active subscriptions.
	configs, err := LoadSnapshot(ctx, s.pool)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to load subscription snapshot: %v", err)
	}

	// Filter by subscriber IDs if requested.
	if ids := req.GetSubscriberIds(); len(ids) > 0 {
		idSet := make(map[string]struct{}, len(ids))
		for _, id := range ids {
			idSet[id] = struct{}{}
		}
		filtered := make([]*configv1.SubscriptionConfig, 0, len(configs))
		for _, c := range configs {
			if _, ok := idSet[c.GetSubscriberId()]; ok {
				filtered = append(filtered, c)
			}
		}
		configs = filtered
	}

	// Send initial SNAPSHOT.
	ver := s.version.Add(1)
	snapshot := &configv1.ConfigUpdate{
		ChangeType:    configv1.ConfigUpdate_SNAPSHOT,
		Subscriptions: configs,
		Version:       ver,
	}
	if err := stream.Send(snapshot); err != nil {
		return err
	}

	s.logger.Info("sent snapshot", "count", len(configs), "version", ver)

	// Register for change notifications from the watcher.
	changes := s.watcher.Subscribe()
	defer s.watcher.Unsubscribe(changes)

	for {
		select {
		case <-ctx.Done():
			return nil
		case subID, ok := <-changes:
			if !ok {
				return nil
			}
			delta, err := s.buildDelta(ctx, subID)
			if err != nil {
				s.logger.Error("failed to build delta", "subscription_id", subID, "error", err)
				continue
			}
			if err := stream.Send(delta); err != nil {
				return err
			}
		}
	}
}

// buildDelta queries the database for a single subscription and returns an UPSERT
// or DELETE ConfigUpdate depending on whether the subscription is still active.
func (s *Server) buildDelta(ctx context.Context, subID string) (*configv1.ConfigUpdate, error) {
	ver := s.version.Add(1)

	var (
		id, subscriberID, sourceFilter, deliveryMode string
		eventTypes, tenantFilter                     []string
		maxBatchSize, maxBatchBytes, lingerMs        int32
		active                                       bool
	)

	err := s.pool.QueryRow(ctx, `
		SELECT s.id, s.subscriber_id, s.event_types, s.source_filter, s.tenant_filter,
		       s.delivery_mode, s.max_batch_size, s.max_batch_bytes, s.linger_ms, s.active
		FROM subscriptions s
		WHERE s.id = $1
	`, subID).Scan(
		&id, &subscriberID, &eventTypes, &sourceFilter, &tenantFilter,
		&deliveryMode, &maxBatchSize, &maxBatchBytes, &lingerMs, &active,
	)

	if err != nil || !active {
		// Subscription was deleted or deactivated -- send DELETE.
		return &configv1.ConfigUpdate{
			ChangeType: configv1.ConfigUpdate_DELETE,
			Subscriptions: []*configv1.SubscriptionConfig{
				{Id: subID},
			},
			Version: ver,
		}, nil
	}

	return &configv1.ConfigUpdate{
		ChangeType: configv1.ConfigUpdate_UPSERT,
		Subscriptions: []*configv1.SubscriptionConfig{
			{
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
			},
		},
		Version: ver,
	}, nil
}
