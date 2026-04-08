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
	queuev1 "github.com/example/event-gateway/internal/proto/queue/v1"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	logger := observability.NewLogger("admin-api")

	dbURL := config.Env("DATABASE_URL", "postgres://egw:egw@localhost:5432/eventgateway?sslmode=disable")
	queueAddr := config.Env("QUEUE_SERVICE_ADDR", "localhost:50052")
	registryURL := config.Env("REGISTRY_URL", "http://localhost:8081")
	httpPort := config.Env("HTTP_PORT", "8085")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		logger.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	conn, err := grpc.NewClient(queueAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("failed to connect to queue service", "addr", queueAddr, "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	queueClient := queuev1.NewQueueServiceClient(conn)

	h := NewAdminHandler(pool, queueClient, logger)

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(middleware.RealIP)
	r.Mount("/", health.Handler(nil))

	r.Get("/", UIHandler().ServeHTTP)

	r.Route("/v1/admin", func(r chi.Router) {
		r.Get("/subscriptions", h.ListSubscriptions)
		r.Get("/subscriptions/{id}/stats", h.GetStats)
		r.Post("/subscriptions/{id}/pause", h.PauseSubscription)
		r.Post("/subscriptions/{id}/resume", h.ResumeSubscription)
		r.Post("/subscriptions/{id}/replay", h.ReplaySubscription)
		r.Get("/health", h.AggregateHealth)
	})

	// Proxy to registry API — same origin, avoids CORS issues.
	// /v1/proxy/v1/subscribers → registry /v1/subscribers
	// /v1/proxy/v1/entity-mappings/all → registry /v1/entity-mappings/all
	r.HandleFunc("/v1/proxy/*", proxyHandler(registryURL))

	server := &http.Server{
		Addr:    fmt.Sprintf(":%s", httpPort),
		Handler: r,
	}

	go func() {
		<-ctx.Done()
		logger.Info("shutting down HTTP server")
		server.Shutdown(context.Background())
	}()

	logger.Info("admin-api starting", "port", httpPort)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		logger.Error("HTTP server failed", "error", err)
		os.Exit(1)
	}
}
