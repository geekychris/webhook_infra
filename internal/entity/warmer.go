package entity

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// CacheWarmer populates and periodically reconciles the Redis entity cache
// with the authoritative state from the subscriber-registry database.
type CacheWarmer struct {
	resolver    *Resolver
	registryURL string
	logger      *slog.Logger
}

// NewCacheWarmer creates a warmer that fetches entity mappings from the registry
// and loads them into the resolver's Redis cache.
func NewCacheWarmer(resolver *Resolver, registryURL string, logger *slog.Logger) *CacheWarmer {
	return &CacheWarmer{
		resolver:    resolver,
		registryURL: registryURL,
		logger:      logger,
	}
}

// WarmOnce fetches all entity mappings from the registry and does a full
// flush-and-reload of the Redis cache. Returns the number of mappings loaded.
func (w *CacheWarmer) WarmOnce(ctx context.Context) (int, error) {
	mappings, err := w.fetchAllMappings(ctx)
	if err != nil {
		return 0, fmt.Errorf("fetch mappings: %w", err)
	}

	if err := w.resolver.FlushAndReload(ctx, mappings); err != nil {
		return 0, fmt.Errorf("flush and reload: %w", err)
	}

	return len(mappings), nil
}

// RunPeriodicSync runs WarmOnce on a fixed interval to heal any cache drift.
// Blocks until ctx is cancelled.
func (w *CacheWarmer) RunPeriodicSync(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count, err := w.WarmOnce(ctx)
			if err != nil {
				w.logger.Error("periodic cache sync failed", "error", err)
			} else {
				w.logger.Info("periodic cache sync completed", "mappings", count)
			}
		}
	}
}

// fetchAllMappings calls the registry's ListAll endpoint.
func (w *CacheWarmer) fetchAllMappings(ctx context.Context) ([]EntityMapping, error) {
	url := w.registryURL + "/v1/entity-mappings/all"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("registry returned %d", resp.StatusCode)
	}

	var mappings []EntityMapping
	if err := json.NewDecoder(resp.Body).Decode(&mappings); err != nil {
		return nil, err
	}
	return mappings, nil
}
