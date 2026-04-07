package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// Producer wraps a kafka-go Writer for publishing messages.
type Producer struct {
	writer *kafka.Writer
}

// NewProducer creates a Kafka producer for the given topic with key-based partitioning.
func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
		},
	}
}

// Publish sends a single key-value message to the topic.
func (p *Producer) Publish(ctx context.Context, key, value []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: value,
	})
}

// PublishBatch sends multiple messages to the topic.
func (p *Producer) PublishBatch(ctx context.Context, msgs []kafka.Message) error {
	return p.writer.WriteMessages(ctx, msgs...)
}

// Close closes the underlying writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}
