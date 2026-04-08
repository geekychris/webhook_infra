package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/example/event-gateway/internal/config"
	"github.com/example/event-gateway/internal/health"
	"github.com/example/event-gateway/internal/observability"
	"github.com/example/event-gateway/internal/readyqueue"
	queuev1 "github.com/example/event-gateway/internal/proto/queue/v1"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	logger := observability.NewLogger("stream-gateway")

	queueAddr := config.Env("QUEUE_SERVICE_ADDR", "localhost:50052")
	redisAddr := config.Env("REDIS_ADDR", "localhost:6379")
	httpPort := config.Env("HTTP_PORT", "8090")
	instanceID := config.Env("INSTANCE_ID", uuid.New().String())

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	conn, err := grpc.NewClient(queueAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("failed to connect to queue service", "addr", queueAddr, "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	queueClient := queuev1.NewQueueServiceClient(conn)

	// Connect to Redis for event notifications.
	var readyQ *readyqueue.Queue
	if redisAddr != "" {
		rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
		if err := rdb.Ping(ctx).Err(); err != nil {
			logger.Warn("redis not available, streaming will use polling fallback", "error", err)
		} else {
			readyQ = readyqueue.New(rdb)
			logger.Info("redis connected for stream notifications", "addr", redisAddr)
		}
	}

	handler := NewStreamHandler(queueClient, readyQ, instanceID, logger)

	mux := http.NewServeMux()
	mux.Handle("/", health.Handler(nil))
	mux.HandleFunc("GET /v1/stream", handler.HandleUpgrade)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%s", httpPort),
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		logger.Info("shutting down HTTP server")
		server.Shutdown(context.Background())
	}()

	logger.Info("stream-gateway starting", "port", httpPort)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		logger.Error("HTTP server failed", "error", err)
		os.Exit(1)
	}
}
