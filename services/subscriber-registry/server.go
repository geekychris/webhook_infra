package main

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/example/event-gateway/internal/health"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// NewServer creates and configures the HTTP server mux with all routes.
func NewServer(store *Store, entityStore *EntityStore, logger *slog.Logger) *chi.Mux {
	r := chi.NewRouter()

	// Middleware
	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(requestLogger(logger))

	// Health checks
	healthHandler := health.Handler(func() error {
		return store.pool.Ping(context.Background())
	})
	r.Mount("/", healthHandler)

	// API routes
	h := NewHandler(store, logger)
	eh := NewEntityHandler(entityStore, logger)

	r.Route("/v1", func(r chi.Router) {
		// Subscribers
		r.Post("/subscribers", h.CreateSubscriber)
		r.Get("/subscribers", h.ListSubscribers)
		r.Get("/subscribers/{id}", h.GetSubscriber)
		r.Put("/subscribers/{id}", h.UpdateSubscriber)
		r.Delete("/subscribers/{id}", h.DeleteSubscriber)

		// Subscriptions (nested under subscriber)
		r.Post("/subscribers/{subscriberID}/subscriptions", h.CreateSubscription)
		r.Get("/subscribers/{subscriberID}/subscriptions", h.ListSubscriptions)

		// Subscriptions (top-level for direct access)
		r.Get("/subscriptions/{id}", h.GetSubscription)
		r.Put("/subscriptions/{id}", h.UpdateSubscription)
		r.Delete("/subscriptions/{id}", h.DeleteSubscription)

		// Entity mappings
		r.Post("/entity-mappings", eh.CreateMapping)
		r.Delete("/entity-mappings", eh.DeleteMapping)
		r.Get("/entity-mappings", eh.ListMappings)
		r.Get("/entity-mappings/all", eh.ListAll)
		r.Get("/subscribers/{id}/entity-mappings", eh.ListBySubscriber)
	})

	return r
}

// requestLogger returns a middleware that logs each request using slog.
func requestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			defer func() {
				logger.Info("request",
					"method", r.Method,
					"path", r.URL.Path,
					"status", ww.Status(),
					"bytes", ww.BytesWritten(),
				)
			}()
			next.ServeHTTP(ww, r)
		})
	}
}
