package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/example/event-gateway/internal/config"
	"github.com/example/event-gateway/internal/kafka"
	"github.com/example/event-gateway/internal/observability"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := observability.NewLogger("subscriber-registry")

	databaseURL := config.Env("DATABASE_URL", "")
	if databaseURL == "" {
		logger.Error("DATABASE_URL is required")
		os.Exit(1)
	}
	httpPort := config.Env("HTTP_PORT", "8080")
	kafkaBrokers := config.Env("KAFKA_BROKERS", "localhost:9092")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create connection pool.
	poolCfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		logger.Error("parsing database URL", "error", err)
		os.Exit(1)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		logger.Error("creating connection pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Verify connectivity.
	if err := pool.Ping(ctx); err != nil {
		logger.Error("pinging database", "error", err)
		os.Exit(1)
	}
	logger.Info("connected to database")

	// Run migrations.
	store := NewStore(pool)
	if err := store.RunMigrations(ctx); err != nil {
		logger.Error("running migrations", "error", err)
		os.Exit(1)
	}
	logger.Info("migrations complete")

	// Create Kafka producer for entity-mappings topic.
	brokers := strings.Split(kafkaBrokers, ",")
	entityProducer := kafka.NewProducer(brokers, kafka.TopicEntityMappings)
	defer entityProducer.Close()

	// Create entity store.
	entityStore := NewEntityStore(pool, entityProducer)

	// Create and start HTTP server.
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%s", httpPort),
		Handler:      NewServer(store, entityStore, logger),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown on SIGTERM/SIGINT.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig := <-sigCh
		logger.Info("received signal, shutting down", "signal", sig)
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer shutdownCancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("server shutdown error", "error", err)
		}
		cancel()
	}()

	logger.Info("starting HTTP server", "port", httpPort)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("server error", "error", err)
		os.Exit(1)
	}
	logger.Info("server stopped")
}
