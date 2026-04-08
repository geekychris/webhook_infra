package main

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/example/event-gateway/internal/kafka"
	"github.com/example/event-gateway/internal/model"
	"github.com/example/event-gateway/internal/readyqueue"
	queuev1 "github.com/example/event-gateway/internal/proto/queue/v1"
)

// Writer consumes SendableEvent messages from Kafka, appends them to the
// subscriber queue service via gRPC, and signals the ready queue in Redis
// so dispatchers are notified immediately.
type Writer struct {
	consumer    *kafka.Consumer
	queueClient queuev1.QueueServiceClient
	readyQ      *readyqueue.Queue
	logger      *slog.Logger
}

// NewWriter creates a new queue writer. readyQ may be nil if Redis is not available.
func NewWriter(consumer *kafka.Consumer, queueClient queuev1.QueueServiceClient, readyQ *readyqueue.Queue, logger *slog.Logger) *Writer {
	return &Writer{
		consumer:    consumer,
		queueClient: queueClient,
		readyQ:      readyQ,
		logger:      logger,
	}
}

func (w *Writer) Run(ctx context.Context) error {
	w.logger.Info("queue-writer starting consumption from sendable-events")
	return w.consumer.Consume(ctx, w.handleMessage)
}

func (w *Writer) handleMessage(ctx context.Context, key, value []byte) error {
	var se model.SendableEvent
	if err := json.Unmarshal(value, &se); err != nil {
		w.logger.Error("failed to unmarshal SendableEvent", "error", err, "key", string(key))
		return err
	}

	ceProto := cloudEventToProto(&se.Event)

	_, err := w.queueClient.Append(ctx, &queuev1.AppendRequest{
		SubscriptionId: string(se.SubscriptionID),
		Events:         []*queuev1.CloudEventProto{ceProto},
	})
	if err != nil {
		w.logger.Error("failed to append to queue service",
			"subscription_id", se.SubscriptionID,
			"event_id", se.Event.ID,
			"error", err,
		)
		return err
	}

	// Signal the ready queue — webhook dispatcher wakes up immediately.
	// Also notify the pub/sub channel — streaming sessions wake up immediately.
	if w.readyQ != nil {
		subIDStr := string(se.SubscriptionID)
		if err := w.readyQ.Signal(ctx, subIDStr); err != nil {
			w.logger.Warn("failed to signal ready queue", "subscription_id", subIDStr, "error", err)
		}
		if err := w.readyQ.Notify(ctx, subIDStr); err != nil {
			w.logger.Warn("failed to notify subscription", "subscription_id", subIDStr, "error", err)
		}
	}

	return nil
}

func cloudEventToProto(ce *model.CloudEvent) *queuev1.CloudEventProto {
	return &queuev1.CloudEventProto{
		SpecVersion:     ce.SpecVersion,
		Id:              ce.ID,
		Type:            ce.Type,
		Source:          ce.Source,
		DataContentType: ce.DataContentType,
		TimeUnixNanos:   ce.Time.UnixNano(),
		Subject:         ce.Subject,
		DataSchema:      ce.DataSchema,
		Data:            ce.Data,
	}
}

func (w *Writer) Close() error {
	return w.consumer.Close()
}
