package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/example/event-gateway/internal/config"
	"github.com/example/event-gateway/internal/observability"
)

func main() {
	logger := observability.NewLogger("subscriber-queue-service")

	grpcPort := config.Env("GRPC_PORT", "50052")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	store := NewMemoryQueueStore()
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

	logger.Info("subscriber-queue-service starting", "port", grpcPort)
	if err := srv.Serve(); err != nil {
		logger.Error("gRPC server failed", "error", err)
		os.Exit(1)
	}
}
