package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/event-gateway/internal/config"
	"github.com/example/event-gateway/internal/observability"
)

func main() {
	logger := observability.NewLogger("subscriber-queue-service")

	grpcPort := config.Env("GRPC_PORT", "50052")
	dataDir := config.Env("DATA_DIR", "/data/queue")
	storeBackend := config.Env("STORE_BACKEND", "pebble")
	ttl := config.EnvDuration("QUEUE_TTL", 30*24*time.Hour)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var store QueueStore
	switch storeBackend {
	case "memory":
		logger.Warn("using in-memory queue store — data will not survive restarts")
		store = NewMemoryQueueStore()
	default:
		// Uses Pebble (pure Go) by default. Build with `-tags rocksdb` for RocksDB.
		logger.Info("opening persistent queue store", "path", dataDir, "ttl", ttl, "backend", storeBackend)
		var err error
		store, err = NewPersistentQueueStore(PersistentStoreOptions{
			Path:   dataDir,
			TTL:    ttl,
			Logger: logger,
		})
		if err != nil {
			logger.Error("failed to open persistent store, falling back to memory", "path", dataDir, "error", err)
			store = NewMemoryQueueStore()
		}
	}
	defer store.Close()

	leases := NewLeaseManager(ctx)
	svc := NewService(store, leases, logger)

	srv, err := NewServer(grpcPort, svc, logger)
	if err != nil {
		logger.Error("failed to create server", "error", err)
		os.Exit(1)
	}

	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	logger.Info("subscriber-queue-service starting",
		"port", grpcPort,
		"backend", storeBackend,
		"data_dir", dataDir,
		"ttl", ttl,
	)
	if err := srv.Serve(); err != nil {
		logger.Error("gRPC server failed", "error", err)
		os.Exit(1)
	}
}
