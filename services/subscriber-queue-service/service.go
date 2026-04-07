package main

import (
	"context"
	"log/slog"
	"time"

	queuev1 "github.com/example/event-gateway/internal/proto/queue/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Service implements the queuev1.QueueServiceServer interface.
type Service struct {
	queuev1.UnimplementedQueueServiceServer
	store  QueueStore
	leases *LeaseManager
	logger *slog.Logger
}

// NewService creates a new QueueService implementation.
func NewService(store QueueStore, leases *LeaseManager, logger *slog.Logger) *Service {
	return &Service{
		store:  store,
		leases: leases,
		logger: logger,
	}
}

// Append serializes each CloudEventProto and appends to the subscription's queue.
func (s *Service) Append(ctx context.Context, req *queuev1.AppendRequest) (*queuev1.AppendResponse, error) {
	if req.SubscriptionId == "" {
		return nil, status.Error(codes.InvalidArgument, "subscription_id is required")
	}
	if len(req.Events) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one event is required")
	}

	eventBytes := make([][]byte, len(req.Events))
	for i, ev := range req.Events {
		b, err := proto.Marshal(ev)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to marshal event %d: %v", i, err)
		}
		eventBytes[i] = b
	}

	firstPos, err := s.store.Append(req.SubscriptionId, eventBytes)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "append failed: %v", err)
	}

	lastPos := firstPos + uint64(len(req.Events)) - 1
	s.logger.Debug("appended events",
		"subscription_id", req.SubscriptionId,
		"count", len(req.Events),
		"first_pos", firstPos,
		"last_pos", lastPos,
	)

	return &queuev1.AppendResponse{
		FirstPosition: firstPos,
		LastPosition:  lastPos,
	}, nil
}

// Read returns a batch of events from a subscription's queue. Requires a valid lease.
func (s *Service) Read(ctx context.Context, req *queuev1.ReadRequest) (*queuev1.ReadResponse, error) {
	if req.SubscriptionId == "" {
		return nil, status.Error(codes.InvalidArgument, "subscription_id is required")
	}

	if err := s.leases.Validate(req.SubscriptionId, req.LeaseEpoch); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "lease validation failed: %v", err)
	}

	maxCount := int(req.MaxCount)
	if maxCount <= 0 {
		maxCount = 100
	}

	entries, err := s.store.Read(req.SubscriptionId, req.AfterPosition, maxCount)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read failed: %v", err)
	}

	queueEntries := make([]*queuev1.QueueEntry, 0, len(entries))
	for _, e := range entries {
		var ev queuev1.CloudEventProto
		if err := proto.Unmarshal(e.Data, &ev); err != nil {
			s.logger.Error("failed to unmarshal stored event",
				"subscription_id", req.SubscriptionId,
				"position", e.Position,
				"error", err,
			)
			continue
		}
		queueEntries = append(queueEntries, &queuev1.QueueEntry{
			Position:       e.Position,
			SubscriptionId: req.SubscriptionId,
			Event:          &ev,
			EnqueuedAtNanos: e.EnqueuedAt,
		})
	}

	return &queuev1.ReadResponse{
		Entries: queueEntries,
	}, nil
}

// Ack acknowledges events up to the given position and advances the cursor.
func (s *Service) Ack(ctx context.Context, req *queuev1.AckRequest) (*queuev1.AckResponse, error) {
	if req.SubscriptionId == "" {
		return nil, status.Error(codes.InvalidArgument, "subscription_id is required")
	}

	if err := s.leases.Validate(req.SubscriptionId, req.LeaseEpoch); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "lease validation failed: %v", err)
	}

	if err := s.store.SetCursor(req.SubscriptionId, req.Position); err != nil {
		return nil, status.Errorf(codes.Internal, "set cursor failed: %v", err)
	}

	s.logger.Debug("acked",
		"subscription_id", req.SubscriptionId,
		"position", req.Position,
	)

	return &queuev1.AckResponse{}, nil
}

// AcquireLease acquires an exclusive delivery lease for a subscription.
func (s *Service) AcquireLease(ctx context.Context, req *queuev1.AcquireLeaseRequest) (*queuev1.AcquireLeaseResponse, error) {
	if req.SubscriptionId == "" {
		return nil, status.Error(codes.InvalidArgument, "subscription_id is required")
	}
	if req.HolderId == "" {
		return nil, status.Error(codes.InvalidArgument, "holder_id is required")
	}

	ttl := time.Duration(req.TtlSeconds) * time.Second
	if ttl <= 0 {
		ttl = 30 * time.Second
	}

	epoch, expiresAt, err := s.leases.Acquire(req.SubscriptionId, req.HolderId, ttl)
	if err != nil {
		return nil, status.Errorf(codes.AlreadyExists, "acquire lease failed: %v", err)
	}

	s.logger.Debug("lease acquired",
		"subscription_id", req.SubscriptionId,
		"holder_id", req.HolderId,
		"epoch", epoch,
		"expires_at", expiresAt,
	)

	return &queuev1.AcquireLeaseResponse{
		Epoch:         epoch,
		ExpiresAtUnix: expiresAt.Unix(),
	}, nil
}

// RenewLease extends an existing delivery lease.
func (s *Service) RenewLease(ctx context.Context, req *queuev1.RenewLeaseRequest) (*queuev1.RenewLeaseResponse, error) {
	if req.SubscriptionId == "" {
		return nil, status.Error(codes.InvalidArgument, "subscription_id is required")
	}

	ttl := time.Duration(req.TtlSeconds) * time.Second
	if ttl <= 0 {
		ttl = 30 * time.Second
	}

	expiresAt, err := s.leases.Renew(req.SubscriptionId, req.HolderId, req.Epoch, ttl)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "renew lease failed: %v", err)
	}

	return &queuev1.RenewLeaseResponse{
		ExpiresAtUnix: expiresAt.Unix(),
	}, nil
}

// ReleaseLease releases a delivery lease early.
func (s *Service) ReleaseLease(ctx context.Context, req *queuev1.ReleaseLeaseRequest) (*queuev1.ReleaseLeaseResponse, error) {
	if req.SubscriptionId == "" {
		return nil, status.Error(codes.InvalidArgument, "subscription_id is required")
	}

	if err := s.leases.Release(req.SubscriptionId, req.HolderId, req.Epoch); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "release lease failed: %v", err)
	}

	s.logger.Debug("lease released",
		"subscription_id", req.SubscriptionId,
		"holder_id", req.HolderId,
	)

	return &queuev1.ReleaseLeaseResponse{}, nil
}

// ListReady returns subscription IDs that have undelivered events.
func (s *Service) ListReady(ctx context.Context, req *queuev1.ListReadyRequest) (*queuev1.ListReadyResponse, error) {
	subs, err := s.store.ListSubscriptionsWithPending()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list ready failed: %v", err)
	}

	limit := int(req.Limit)
	if limit > 0 && len(subs) > limit {
		subs = subs[:limit]
	}

	return &queuev1.ListReadyResponse{
		SubscriptionIds: subs,
	}, nil
}

// GetState returns the queue state for a subscription.
func (s *Service) GetState(ctx context.Context, req *queuev1.GetStateRequest) (*queuev1.GetStateResponse, error) {
	if req.SubscriptionId == "" {
		return nil, status.Error(codes.InvalidArgument, "subscription_id is required")
	}

	tail, err := s.store.GetTailPosition(req.SubscriptionId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get tail failed: %v", err)
	}

	cursor, err := s.store.GetCursor(req.SubscriptionId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get cursor failed: %v", err)
	}

	state := &queuev1.SubscriptionState{
		SubscriptionId: req.SubscriptionId,
		TailSeq:        tail,
		AckedSeq:       cursor,
	}

	if lease := s.leases.GetLease(req.SubscriptionId); lease != nil {
		state.LeaseEpoch = lease.Epoch
	}

	return &queuev1.GetStateResponse{
		State: state,
	}, nil
}

// ResetCursor sets the acked cursor to the requested position (for replay).
func (s *Service) ResetCursor(ctx context.Context, req *queuev1.ResetCursorRequest) (*queuev1.ResetCursorResponse, error) {
	if req.SubscriptionId == "" {
		return nil, status.Error(codes.InvalidArgument, "subscription_id is required")
	}

	if err := s.store.SetCursor(req.SubscriptionId, req.ToPosition); err != nil {
		return nil, status.Errorf(codes.Internal, "reset cursor failed: %v", err)
	}

	s.logger.Info("cursor reset",
		"subscription_id", req.SubscriptionId,
		"to_position", req.ToPosition,
	)

	return &queuev1.ResetCursorResponse{}, nil
}
