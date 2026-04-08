package kafka

import (
	"context"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// MessageHandler processes a single Kafka message. Return nil to commit the offset.
type MessageHandler func(ctx context.Context, key, value []byte) error

// Consumer wraps a kafka-go Reader for consuming messages from a consumer group.
type Consumer struct {
	reader *kafka.Reader
	logger *slog.Logger
}

// NewConsumer creates a Kafka consumer for the given topic and group.
func NewConsumer(brokers []string, topic, groupID string, logger *slog.Logger) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    topic,
			GroupID:  groupID,
			MinBytes: 1e3,       // 1 KB
			MaxBytes: 10e6,      // 10 MB
			MaxWait:  500 * time.Millisecond,
		}),
		logger: logger,
	}
}

// NewConsumerFromEarliest creates a Kafka consumer that starts from the earliest
// available offset when the consumer group has no committed offsets. This is
// important for topics like entity-mappings where we need the full history on
// first startup to build the complete cache state.
func NewConsumerFromEarliest(brokers []string, topic, groupID string, logger *slog.Logger) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			Topic:       topic,
			GroupID:     groupID,
			MinBytes:    1e3,
			MaxBytes:    10e6,
			MaxWait:     500 * time.Millisecond,
			StartOffset: kafka.FirstOffset,
		}),
		logger: logger,
	}
}

// Consume reads messages in a loop, calling handler for each. It commits offsets
// after successful processing. Blocks until ctx is cancelled.
func (c *Consumer) Consume(ctx context.Context, handler MessageHandler) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		if err := handler(ctx, msg.Key, msg.Value); err != nil {
			c.logger.Error("message handler failed",
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"error", err,
			)
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("commit failed",
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"error", err,
			)
		}
	}
}

// Close closes the underlying reader.
func (c *Consumer) Close() error {
	return c.reader.Close()
}
