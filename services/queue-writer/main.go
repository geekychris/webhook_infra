package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/example/event-gateway/internal/config"
	"github.com/example/event-gateway/internal/kafka"
	"github.com/example/event-gateway/internal/observability"
	queuev1 "github.com/example/event-gateway/internal/proto/queue/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	logger := observability.NewLogger("queue-writer")

	brokers := strings.Split(config.Env("KAFKA_BROKERS", "localhost:9092"), ",")
	queueAddr := config.Env("QUEUE_SERVICE_ADDR", "localhost:50052")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Connect to the subscriber queue service via gRPC.
	conn, err := grpc.NewClient(queueAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		logger.Error("failed to connect to queue service", "addr", queueAddr, "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	queueClient := queuev1.NewQueueServiceClient(conn)

	consumer := kafka.NewConsumer(brokers, "sendable-events", "queue-writer", logger)

	writer := NewWriter(consumer, queueClient, logger)
	defer writer.Close()

	logger.Info("queue-writer starting",
		"brokers", brokers,
		"queue_service_addr", queueAddr,
	)

	if err := writer.Run(ctx); err != nil {
		logger.Error("queue-writer stopped with error", "error", err)
		os.Exit(1)
	}

	logger.Info("queue-writer stopped")
}
