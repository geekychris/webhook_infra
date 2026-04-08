package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/example/event-gateway/internal/readyqueue"
	queuev1 "github.com/example/event-gateway/internal/proto/queue/v1"
	"nhooyr.io/websocket"
)

// AckMessage is sent by the client to acknowledge receipt of events.
type AckMessage struct {
	Type     string `json:"type"`
	Position uint64 `json:"position"`
}

// EventMessage is sent to the client with a queue event.
type EventMessage struct {
	Type     string          `json:"type"`
	Position uint64          `json:"position"`
	Event    json.RawMessage `json:"event"`
}

// Session manages a single WebSocket streaming connection for a subscription.
type Session struct {
	conn        *websocket.Conn
	subID       string
	queueClient queuev1.QueueServiceClient
	readyQ      *readyqueue.Queue
	instanceID  string
	resumeAfter uint64
	leaseEpoch  uint64
	logger      *slog.Logger
}

func NewSession(
	conn *websocket.Conn,
	subID string,
	queueClient queuev1.QueueServiceClient,
	readyQ *readyqueue.Queue,
	instanceID string,
	resumeAfter uint64,
	logger *slog.Logger,
) *Session {
	return &Session{
		conn:        conn,
		subID:       subID,
		queueClient: queueClient,
		readyQ:      readyQ,
		instanceID:  instanceID,
		resumeAfter: resumeAfter,
		logger:      logger.With("subscription_id", subID),
	}
}

// Run starts the streaming session. Blocks until context is cancelled or error.
func (s *Session) Run(ctx context.Context) error {
	defer s.conn.Close(websocket.StatusNormalClosure, "session ended")

	// Acquire delivery lease
	leaseResp, err := s.queueClient.AcquireLease(ctx, &queuev1.AcquireLeaseRequest{
		SubscriptionId: s.subID,
		HolderId:       s.instanceID,
		TtlSeconds:     60,
	})
	if err != nil {
		s.conn.Close(websocket.StatusInternalError, "failed to acquire lease")
		return fmt.Errorf("acquire lease: %w", err)
	}
	s.leaseEpoch = leaseResp.Epoch

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go s.renewLeaseLoop(ctx)

	// Start ACK reader in background
	ackCh := make(chan uint64, 64)
	go s.readAcks(ctx, cancel, ackCh)

	// Subscribe to Redis notifications for this subscription — wakes us
	// instantly when new events are appended. No polling needed.
	var notifyCh <-chan struct{}
	if s.readyQ != nil {
		notifyCh = s.readyQ.Subscribe(ctx, s.subID)
	}

	cursor := s.resumeAfter

	for {
		// Process any pending ACKs first (non-blocking).
		s.drainAcks(ctx, ackCh)

		// Read available events from the queue.
		readResp, err := s.queueClient.Read(ctx, &queuev1.ReadRequest{
			SubscriptionId: s.subID,
			AfterPosition:  cursor,
			MaxCount:       50,
			LeaseEpoch:     s.leaseEpoch,
		})
		if err != nil {
			return fmt.Errorf("read queue: %w", err)
		}

		if len(readResp.Entries) == 0 {
			// No events available — block until notified or timeout.
			if notifyCh != nil {
				select {
				case <-ctx.Done():
					return nil
				case <-notifyCh:
					continue // new events available, read immediately
				case ackPos := <-ackCh:
					s.ack(ctx, ackPos)
					continue
				}
			} else {
				// Fallback: brief poll if Redis not available.
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(100 * time.Millisecond):
					continue
				case ackPos := <-ackCh:
					s.ack(ctx, ackPos)
					continue
				}
			}
		}

		// Stream events to client immediately — no linger for streaming.
		for _, entry := range readResp.Entries {
			eventJSON, err := json.Marshal(entry.GetEvent())
			if err != nil {
				return fmt.Errorf("marshal event proto: %w", err)
			}

			msg := EventMessage{
				Type:     "event",
				Position: entry.Position,
				Event:    eventJSON,
			}

			data, err := json.Marshal(msg)
			if err != nil {
				return fmt.Errorf("marshal event: %w", err)
			}

			if err := s.conn.Write(ctx, websocket.MessageText, data); err != nil {
				return fmt.Errorf("write to websocket: %w", err)
			}

			cursor = entry.Position
		}
	}
}

func (s *Session) drainAcks(ctx context.Context, ackCh <-chan uint64) {
	for {
		select {
		case pos := <-ackCh:
			s.ack(ctx, pos)
		default:
			return
		}
	}
}

func (s *Session) ack(ctx context.Context, pos uint64) {
	if _, err := s.queueClient.Ack(ctx, &queuev1.AckRequest{
		SubscriptionId: s.subID,
		Position:       pos,
		LeaseEpoch:     s.leaseEpoch,
	}); err != nil {
		s.logger.Error("ack failed", "position", pos, "error", err)
	}
}

func (s *Session) renewLeaseLoop(ctx context.Context) {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.queueClient.ReleaseLease(context.Background(), &queuev1.ReleaseLeaseRequest{
				SubscriptionId: s.subID,
				HolderId:       s.instanceID,
				Epoch:          s.leaseEpoch,
			})
			return
		case <-ticker.C:
			_, err := s.queueClient.RenewLease(ctx, &queuev1.RenewLeaseRequest{
				SubscriptionId: s.subID,
				HolderId:       s.instanceID,
				Epoch:          s.leaseEpoch,
				TtlSeconds:     60,
			})
			if err != nil {
				s.logger.Error("lease renewal failed", "error", err)
				return
			}
		}
	}
}

func (s *Session) readAcks(ctx context.Context, cancel context.CancelFunc, ackCh chan<- uint64) {
	defer cancel()
	for {
		_, data, err := s.conn.Read(ctx)
		if err != nil {
			return
		}

		var ack AckMessage
		if err := json.Unmarshal(data, &ack); err != nil {
			s.logger.Warn("invalid ack message", "error", err)
			continue
		}

		if ack.Type == "ack" {
			select {
			case ackCh <- ack.Position:
			case <-ctx.Done():
				return
			}
		}
	}
}
