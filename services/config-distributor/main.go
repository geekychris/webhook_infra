package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/example/event-gateway/internal/config"
	configv1 "github.com/example/event-gateway/internal/proto/config/v1"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})).With("service", "config-distributor")

	dbURL := config.Env("DATABASE_URL", "postgres://egw:egw@localhost:5432/eventgateway?sslmode=disable")
	grpcPort := config.Env("GRPC_PORT", "50051")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		logger.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	watcher := NewWatcher(pool, logger)
	go func() {
		if err := watcher.Run(ctx); err != nil {
			logger.Error("watcher stopped with error", "error", err)
		}
	}()

	srv := NewServer(pool, watcher, logger)

	grpcServer := grpc.NewServer()
	configv1.RegisterConfigDistributorServer(grpcServer, srv)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", grpcPort))
	if err != nil {
		logger.Error("failed to listen", "port", grpcPort, "error", err)
		os.Exit(1)
	}

	go func() {
		<-ctx.Done()
		logger.Info("shutting down gRPC server")
		grpcServer.GracefulStop()
	}()

	logger.Info("config-distributor starting", "port", grpcPort)
	if err := grpcServer.Serve(lis); err != nil {
		logger.Error("gRPC server failed", "error", err)
		os.Exit(1)
	}
}
