package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"time"

	"github.com/example/event-gateway/internal/config"
	"github.com/example/event-gateway/internal/entity"
	"github.com/example/event-gateway/internal/kafka"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})).With("service", "ingestion-router")

	brokersRaw := config.Env("KAFKA_BROKERS", "localhost:9092")
	brokers := strings.Split(brokersRaw, ",")
	configAddr := config.Env("CONFIG_DISTRIBUTOR_ADDR", "localhost:50051")
	redisAddr := config.Env("REDIS_ADDR", "localhost:6379")
	registryURL := config.Env("REGISTRY_URL", "http://localhost:8081")
	groupID := config.Env("KAFKA_GROUP_ID", "ingestion-router")
	cacheSyncInterval := config.EnvDuration("CACHE_SYNC_INTERVAL", 5*time.Minute)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Set up Redis for entity resolution
	var resolver *entity.Resolver
	if redisAddr != "" {
		rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
		if err := rdb.Ping(ctx).Err(); err != nil {
			logger.Warn("redis not available, entity routing disabled", "addr", redisAddr, "error", err)
		} else {
			resolver = entity.NewResolver(rdb)
			logger.Info("redis connected for entity routing", "addr", redisAddr)
		}
	}

	// Start entity mapping cache management
	if resolver != nil {
		// 1. Warm cache from the registry database on startup (authoritative source)
		warmer := entity.NewCacheWarmer(resolver, registryURL, logger)
		count, err := warmer.WarmOnce(ctx)
		if err != nil {
			logger.Warn("startup cache warm failed, will rely on Kafka replay", "error", err)
		} else {
			logger.Info("entity cache warmed from database", "mappings", count)
		}

		// 2. Start Kafka syncer for real-time incremental updates.
		//    Uses FirstOffset so new consumer groups replay full topic history.
		entitySyncer := entity.NewSyncer(resolver, logger)
		entityConsumer := kafka.NewConsumerFromEarliest(brokers, kafka.TopicEntityMappings, "ingestion-router-entity", logger)
		defer entityConsumer.Close()
		go func() {
			if err := entityConsumer.Consume(ctx, entitySyncer.ProcessMessage); err != nil {
				logger.Error("entity syncer stopped", "error", err)
			}
		}()

		// 3. Periodic full reconciliation to heal any cache drift.
		//    Flushes all entity:* keys and reloads from the database.
		go warmer.RunPeriodicSync(ctx, cacheSyncInterval)
	}

	consumer := kafka.NewConsumer(brokers, kafka.TopicECSTEvents, groupID, logger)
	defer consumer.Close()

	producer := kafka.NewProducer(brokers, kafka.TopicSendableEvents)
	defer producer.Close()

	router := NewRouter(consumer, producer, configAddr, resolver, logger)

	logger.Info("ingestion-router starting",
		"brokers", brokers,
		"config_addr", configAddr,
		"redis_addr", redisAddr,
	)

	if err := router.Run(ctx); err != nil {
		logger.Error("router stopped with error", "error", err)
		os.Exit(1)
	}
}
