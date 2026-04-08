package main

import (
	"context"
	"crypto/tls"
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
	registryURL := config.Env("REGISTRY_URL", "http://localhost:8081")
	httpPort := config.Env("HTTP_PORT", "8090")
	instanceID := config.Env("INSTANCE_ID", uuid.New().String())

	// TLS configuration (optional)
	tlsCert := config.Env("TLS_CERT", "") // path to certificate file
	tlsKey := config.Env("TLS_KEY", "")   // path to private key file

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

	// Token authentication against the registry.
	auth := NewTokenAuth(registryURL, logger)

	handler := NewStreamHandler(queueClient, readyQ, auth, instanceID, logger)

	mux := http.NewServeMux()
	mux.Handle("/", health.Handler(nil))
	mux.HandleFunc("GET /v1/stream", handler.HandleUpgrade)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%s", httpPort),
		Handler: mux,
	}

	// Configure TLS if cert and key are provided.
	useTLS := tlsCert != "" && tlsKey != ""
	if useTLS {
		cert, err := tls.LoadX509KeyPair(tlsCert, tlsKey)
		if err != nil {
			logger.Error("failed to load TLS certificate", "cert", tlsCert, "key", tlsKey, "error", err)
			os.Exit(1)
		}
		server.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
	}

	go func() {
		<-ctx.Done()
		logger.Info("shutting down")
		server.Shutdown(context.Background())
	}()

	proto := "ws"
	if useTLS {
		proto = "wss"
		logger.Info("stream-gateway starting with TLS",
			"port", httpPort,
			"protocol", proto,
			"cert", tlsCert,
		)
		if err := server.ListenAndServeTLS(tlsCert, tlsKey); err != http.ErrServerClosed {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	} else {
		logger.Info("stream-gateway starting",
			"port", httpPort,
			"protocol", proto,
			"tls", "disabled (set TLS_CERT and TLS_KEY to enable)",
		)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	}
}
