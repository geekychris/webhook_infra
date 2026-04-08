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
	"github.com/example/event-gateway/internal/readyqueue"
	queuev1 "github.com/example/event-gateway/internal/proto/queue/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	logger := observability.NewLogger("queue-writer")

	brokers := strings.Split(config.Env("KAFKA_BROKERS", "localhost:9092"), ",")
	queueAddr := config.Env("QUEUE_SERVICE_ADDR", "localhost:50052")
	redisAddr := config.Env("REDIS_ADDR", "localhost:6379")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	conn, err := grpc.NewClient(queueAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("failed to connect to queue service", "addr", queueAddr, "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	queueClient := queuev1.NewQueueServiceClient(conn)

	// Connect to Redis for the ready queue signal.
	var readyQ *readyqueue.Queue
	if redisAddr != "" {
		rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
		if err := rdb.Ping(ctx).Err(); err != nil {
			logger.Warn("redis not available, ready-queue signaling disabled", "error", err)
		} else {
			readyQ = readyqueue.New(rdb)
			logger.Info("redis ready-queue connected", "addr", redisAddr)
		}
	}

	consumer := kafka.NewConsumer(brokers, kafka.TopicSendableEvents, "queue-writer", logger)

	writer := NewWriter(consumer, queueClient, readyQ, logger)
	defer writer.Close()

	logger.Info("queue-writer starting", "brokers", brokers, "queue_service_addr", queueAddr)

	if err := writer.Run(ctx); err != nil {
		logger.Error("queue-writer stopped with error", "error", err)
		os.Exit(1)
	}
}
