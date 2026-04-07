package main

import (
	"context"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Watcher listens on the PostgreSQL "subscription_changes" NOTIFY channel and
// fans out change notifications to all registered listeners.
type Watcher struct {
	pool   *pgxpool.Pool
	logger *slog.Logger

	mu        sync.RWMutex
	listeners []chan string
}

// NewWatcher creates a Watcher backed by the given connection pool.
func NewWatcher(pool *pgxpool.Pool, logger *slog.Logger) *Watcher {
	return &Watcher{
		pool:   pool,
		logger: logger,
	}
}

// Subscribe registers a new listener and returns a channel that will receive
// subscription IDs whenever a change notification arrives.
func (w *Watcher) Subscribe() chan string {
	ch := make(chan string, 64)
	w.mu.Lock()
	w.listeners = append(w.listeners, ch)
	w.mu.Unlock()
	return ch
}

// Unsubscribe removes a previously registered listener and closes its channel.
func (w *Watcher) Unsubscribe(ch chan string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i, l := range w.listeners {
		if l == ch {
			w.listeners = append(w.listeners[:i], w.listeners[i+1:]...)
			close(ch)
			return
		}
	}
}

// Run acquires a dedicated connection from the pool, issues LISTEN, and blocks
// until the context is cancelled, fanning out every notification payload to all
// registered listeners.
func (w *Watcher) Run(ctx context.Context) error {
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN subscription_changes"); err != nil {
		return err
	}

	w.logger.Info("listening for subscription_changes notifications")

	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		subID := notification.Payload
		w.logger.Debug("subscription changed", "subscription_id", subID)

		w.mu.RLock()
		for _, ch := range w.listeners {
			select {
			case ch <- subID:
			default:
				w.logger.Warn("listener channel full, dropping notification", "subscription_id", subID)
			}
		}
		w.mu.RUnlock()
	}
}
