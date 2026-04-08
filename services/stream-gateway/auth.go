package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// TokenAuth validates streaming tokens against the subscriber registry.
// It caches validated tokens to avoid hitting the registry on every connection.
type TokenAuth struct {
	registryURL string
	client      *http.Client
	logger      *slog.Logger

	mu    sync.RWMutex
	cache map[string]*authEntry // token → entry
}

type authEntry struct {
	subscriberID string
	validatedAt  time.Time
}

const authCacheTTL = 5 * time.Minute

// NewTokenAuth creates a token authenticator that validates against the registry.
func NewTokenAuth(registryURL string, logger *slog.Logger) *TokenAuth {
	return &TokenAuth{
		registryURL: registryURL,
		client:      &http.Client{Timeout: 5 * time.Second},
		logger:      logger,
		cache:       make(map[string]*authEntry),
	}
}

// Validate checks the token against the subscriber for the given subscription.
// It fetches the subscription to find the subscriber_id, then fetches the
// subscriber to check the stream_auth field matches the token.
//
// Token can be provided via:
//   - Query parameter: ?token=<value>
//   - Authorization header: Bearer <value>
//
// Returns the subscriber ID on success, or an error.
func (a *TokenAuth) Validate(ctx context.Context, subscriptionID, token string) (subscriberID string, err error) {
	if token == "" {
		return "", fmt.Errorf("token is required")
	}

	// Check cache first
	cacheKey := subscriptionID + ":" + token
	a.mu.RLock()
	entry, ok := a.cache[cacheKey]
	a.mu.RUnlock()
	if ok && time.Since(entry.validatedAt) < authCacheTTL {
		return entry.subscriberID, nil
	}

	// Fetch subscription to get subscriber_id
	subResp, err := a.client.Get(a.registryURL + "/v1/subscriptions/" + subscriptionID)
	if err != nil {
		return "", fmt.Errorf("registry unreachable: %w", err)
	}
	defer subResp.Body.Close()
	if subResp.StatusCode == http.StatusNotFound {
		return "", fmt.Errorf("subscription not found")
	}
	if subResp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("registry returned %d for subscription", subResp.StatusCode)
	}

	var subscription struct {
		SubscriberID string `json:"subscriber_id"`
		Active       bool   `json:"active"`
	}
	if err := json.NewDecoder(subResp.Body).Decode(&subscription); err != nil {
		return "", fmt.Errorf("decode subscription: %w", err)
	}
	if !subscription.Active {
		return "", fmt.Errorf("subscription is paused")
	}

	// Fetch subscriber to check stream_auth
	subscriberResp, err := a.client.Get(a.registryURL + "/v1/subscribers/" + subscription.SubscriberID)
	if err != nil {
		return "", fmt.Errorf("registry unreachable: %w", err)
	}
	defer subscriberResp.Body.Close()
	if subscriberResp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("subscriber not found")
	}

	var subscriber struct {
		ID         string `json:"id"`
		StreamAuth string `json:"stream_auth"`
	}
	if err := json.NewDecoder(subscriberResp.Body).Decode(&subscriber); err != nil {
		return "", fmt.Errorf("decode subscriber: %w", err)
	}

	// Validate token
	if subscriber.StreamAuth == "" {
		// No stream_auth configured — allow any token (backward compatible)
		a.logger.Warn("subscriber has no stream_auth configured, allowing connection",
			"subscriber_id", subscriber.ID,
			"subscription_id", subscriptionID,
		)
	} else if subscriber.StreamAuth != token {
		return "", fmt.Errorf("invalid token")
	}

	// Cache successful validation
	a.mu.Lock()
	a.cache[cacheKey] = &authEntry{
		subscriberID: subscriber.ID,
		validatedAt:  time.Now(),
	}
	a.mu.Unlock()

	return subscriber.ID, nil
}

// ExtractToken gets the auth token from query param or Authorization header.
func ExtractToken(r *http.Request) string {
	// Check query parameter first
	if token := r.URL.Query().Get("token"); token != "" {
		return token
	}
	// Check Authorization header: "Bearer <token>"
	auth := r.Header.Get("Authorization")
	if len(auth) > 7 && auth[:7] == "Bearer " {
		return auth[7:]
	}
	return ""
}
