package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/example/event-gateway/internal/config"
	"github.com/example/event-gateway/internal/kafka"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})).With("service", "ingestion-router")

	brokersRaw := config.Env("KAFKA_BROKERS", "localhost:9092")
	brokers := strings.Split(brokersRaw, ",")
	configAddr := config.Env("CONFIG_DISTRIBUTOR_ADDR", "localhost:50051")
	groupID := config.Env("KAFKA_GROUP_ID", "ingestion-router")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	consumer := kafka.NewConsumer(brokers, "ecst-events", groupID, logger)
	defer consumer.Close()

	producer := kafka.NewProducer(brokers, "sendable-events")
	defer producer.Close()

	router := NewRouter(consumer, producer, configAddr, logger)

	logger.Info("ingestion-router starting",
		"brokers", brokers,
		"config_addr", configAddr,
	)

	if err := router.Run(ctx); err != nil {
		logger.Error("router stopped with error", "error", err)
		os.Exit(1)
	}
}
