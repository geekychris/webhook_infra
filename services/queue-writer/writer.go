package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/example/event-gateway/internal/kafka"
	"github.com/example/event-gateway/internal/model"
	queuev1 "github.com/example/event-gateway/internal/proto/queue/v1"
)

// Writer consumes SendableEvent messages from Kafka and appends them to the
// subscriber queue service via gRPC.
type Writer struct {
	consumer    *kafka.Consumer
	queueClient queuev1.QueueServiceClient
	logger      *slog.Logger
}

// NewWriter creates a new queue writer.
func NewWriter(consumer *kafka.Consumer, queueClient queuev1.QueueServiceClient, logger *slog.Logger) *Writer {
	return &Writer{
		consumer:    consumer,
		queueClient: queueClient,
		logger:      logger,
	}
}

// Run starts consuming messages. Blocks until ctx is cancelled.
func (w *Writer) Run(ctx context.Context) error {
	w.logger.Info("queue-writer starting consumption from sendable-events")
	return w.consumer.Consume(ctx, w.handleMessage)
}

// handleMessage processes a single Kafka message. It deserializes the
// SendableEvent, converts the CloudEvent to a protobuf CloudEventProto,
// and appends it to the queue service. Returns nil only on success so
// the consumer commits the offset.
func (w *Writer) handleMessage(ctx context.Context, key, value []byte) error {
	var se model.SendableEvent
	if err := json.Unmarshal(value, &se); err != nil {
		w.logger.Error("failed to unmarshal SendableEvent",
			"error", err,
			"key", string(key),
		)
		// Return error so the offset is not committed.
		return err
	}

	ceProto := cloudEventToProto(&se.Event)

	req := &queuev1.AppendRequest{
		SubscriptionId: string(se.SubscriptionID),
		Events:         []*queuev1.CloudEventProto{ceProto},
	}

	resp, err := w.queueClient.Append(ctx, req)
	if err != nil {
		w.logger.Error("failed to append to queue service",
			"subscription_id", se.SubscriptionID,
			"event_id", se.Event.ID,
			"error", err,
		)
		return err
	}

	w.logger.Debug("appended event to queue",
		"subscription_id", se.SubscriptionID,
		"event_id", se.Event.ID,
		"position", resp.FirstPosition,
	)

	return nil
}

// cloudEventToProto converts a model.CloudEvent to a queuev1.CloudEventProto.
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

// Close shuts down the writer's consumer.
func (w *Writer) Close() error {
	return w.consumer.Close()
}

// retryDelay returns an exponential backoff duration capped at 30 seconds.
// Not used in the current flow (kafka consumer handles retries), but
// available for future use.
func retryDelay(attempt int) time.Duration {
	d := time.Duration(1<<uint(attempt)) * time.Second
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}
