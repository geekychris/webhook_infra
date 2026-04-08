package matcher

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/example/event-gateway/internal/entity"
	"github.com/example/event-gateway/internal/model"
	"github.com/redis/go-redis/v9"
)

func setupEntityMatcher(t *testing.T) (*EntityMatcher, *Matcher, *entity.Resolver, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	resolver := entity.NewResolver(rdb)
	m := New()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	em := NewEntityMatcher(m, resolver, logger)
	return em, m, resolver, mr
}

func makeSub(id, subscriberID string, eventTypes []string) model.Subscription {
	return model.Subscription{
		ID:           model.SubscriptionID(id),
		SubscriberID: model.SubscriberID(subscriberID),
		EventTypes:   eventTypes,
		SourceFilter: "*",
		Active:       true,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
}

func TestStandardMatchingStillWorks(t *testing.T) {
	em, m, _, _ := setupEntityMatcher(t)
	ctx := context.Background()

	s1 := makeSub("sub1", "subscriber-a", []string{"order.created"})
	s2 := makeSub("sub2", "subscriber-b", []string{"order.created"})
	m.LoadSnapshot([]model.Subscription{s1, s2})

	event := model.ECSTEvent{
		ID: "evt1", Type: "order.created", Source: "svc", TenantID: "t1",
		Payload:   json.RawMessage(`{"amount":100}`),
		Timestamp: time.Now(),
	}

	baseResults := m.Match(event)
	entityResults := em.MatchWithEntities(ctx, event)

	if len(entityResults) != len(baseResults) {
		t.Fatalf("expected %d results, got %d", len(baseResults), len(entityResults))
	}

	resultIDs := map[model.SubscriptionID]bool{}
	for _, r := range entityResults {
		resultIDs[r.SubscriptionID] = true
	}
	for _, r := range baseResults {
		if !resultIDs[r.SubscriptionID] {
			t.Errorf("base result %s not found in entity results", r.SubscriptionID)
		}
	}
}

func TestSendToSingleSubscriber(t *testing.T) {
	em, m, _, _ := setupEntityMatcher(t)
	ctx := context.Background()

	s1 := makeSub("sub1", "subscriber-a", []string{"order.created"})
	m.LoadSnapshot([]model.Subscription{s1})

	event := model.ECSTEvent{
		ID: "evt1", Type: "user.updated", Source: "svc", TenantID: "t1",
		Payload:   json.RawMessage(`{"send_to":"subscriber-a","amount":100}`),
		Timestamp: time.Now(),
	}

	results := em.MatchWithEntities(ctx, event)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].SubscriberID != "subscriber-a" {
		t.Errorf("expected subscriber-a, got %s", results[0].SubscriberID)
	}
}

func TestSendToMultipleSubscribers(t *testing.T) {
	em, m, _, _ := setupEntityMatcher(t)
	ctx := context.Background()

	s1 := makeSub("sub1", "subscriber-a", []string{"order.created"})
	s2 := makeSub("sub2", "subscriber-b", []string{"order.created"})
	m.LoadSnapshot([]model.Subscription{s1, s2})

	event := model.ECSTEvent{
		ID: "evt1", Type: "user.updated", Source: "svc", TenantID: "t1",
		Payload:   json.RawMessage(`{"send_to":["subscriber-a","subscriber-b"]}`),
		Timestamp: time.Now(),
	}

	results := em.MatchWithEntities(ctx, event)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	found := map[model.SubscriberID]bool{}
	for _, r := range results {
		found[r.SubscriberID] = true
	}
	if !found["subscriber-a"] || !found["subscriber-b"] {
		t.Errorf("expected both subscriber-a and subscriber-b, got %v", found)
	}
}

func TestSendToUnknownSubscriber(t *testing.T) {
	em, m, _, _ := setupEntityMatcher(t)
	ctx := context.Background()

	s1 := makeSub("sub1", "subscriber-a", []string{"order.created"})
	m.LoadSnapshot([]model.Subscription{s1})

	event := model.ECSTEvent{
		ID: "evt1", Type: "user.updated", Source: "svc", TenantID: "t1",
		Payload:   json.RawMessage(`{"send_to":"unknown-subscriber"}`),
		Timestamp: time.Now(),
	}

	results := em.MatchWithEntities(ctx, event)
	if len(results) != 0 {
		t.Fatalf("expected 0 results for unknown subscriber, got %d", len(results))
	}
}

func TestUserIDRouting(t *testing.T) {
	em, m, resolver, _ := setupEntityMatcher(t)
	ctx := context.Background()

	s1 := makeSub("sub1", "subscriber-a", []string{"order.created"})
	m.LoadSnapshot([]model.Subscription{s1})

	if err := resolver.AddMapping(ctx, "user", "u123", "subscriber-a"); err != nil {
		t.Fatalf("AddMapping failed: %v", err)
	}

	event := model.ECSTEvent{
		ID: "evt2", Type: "notification.sent", Source: "svc", TenantID: "t1",
		Payload:   json.RawMessage(`{"amount":100}`),
		Metadata:  map[string]string{"user_id": "u123"},
		Timestamp: time.Now(),
	}

	results := em.MatchWithEntities(ctx, event)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].SubscriberID != "subscriber-a" {
		t.Errorf("expected subscriber-a, got %s", results[0].SubscriberID)
	}
}

func TestCompanyIDRouting(t *testing.T) {
	em, m, resolver, _ := setupEntityMatcher(t)
	ctx := context.Background()

	s1 := makeSub("sub1", "subscriber-a", []string{"order.created"})
	s2 := makeSub("sub2", "subscriber-b", []string{"order.created"})
	m.LoadSnapshot([]model.Subscription{s1, s2})

	if err := resolver.AddMapping(ctx, "company", "c456", "subscriber-a"); err != nil {
		t.Fatalf("AddMapping failed: %v", err)
	}
	if err := resolver.AddMapping(ctx, "company", "c456", "subscriber-b"); err != nil {
		t.Fatalf("AddMapping failed: %v", err)
	}

	event := model.ECSTEvent{
		ID: "evt3", Type: "notification.sent", Source: "svc", TenantID: "t1",
		Payload:   json.RawMessage(`{"company_id":"c456"}`),
		Timestamp: time.Now(),
	}

	results := em.MatchWithEntities(ctx, event)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	found := map[model.SubscriberID]bool{}
	for _, r := range results {
		found[r.SubscriberID] = true
	}
	if !found["subscriber-a"] || !found["subscriber-b"] {
		t.Errorf("expected both subscriber-a and subscriber-b, got %v", found)
	}
}

func TestUserIDInPayload(t *testing.T) {
	em, m, resolver, _ := setupEntityMatcher(t)
	ctx := context.Background()

	s1 := makeSub("sub1", "subscriber-a", []string{"order.created"})
	m.LoadSnapshot([]model.Subscription{s1})

	if err := resolver.AddMapping(ctx, "user", "u789", "subscriber-a"); err != nil {
		t.Fatalf("AddMapping failed: %v", err)
	}

	event := model.ECSTEvent{
		ID: "evt4", Type: "notification.sent", Source: "svc", TenantID: "t1",
		Payload:   json.RawMessage(`{"user_id":"u789","data":"hello"}`),
		Timestamp: time.Now(),
	}

	results := em.MatchWithEntities(ctx, event)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].SubscriberID != "subscriber-a" {
		t.Errorf("expected subscriber-a, got %s", results[0].SubscriberID)
	}
}

func TestDeduplication(t *testing.T) {
	em, m, resolver, _ := setupEntityMatcher(t)
	ctx := context.Background()

	// subscriber-a has a subscription that matches order.created by type
	s1 := makeSub("sub1", "subscriber-a", []string{"order.created"})
	m.LoadSnapshot([]model.Subscription{s1})

	// Also map user u123 -> subscriber-a
	if err := resolver.AddMapping(ctx, "user", "u123", "subscriber-a"); err != nil {
		t.Fatalf("AddMapping failed: %v", err)
	}

	// Event matches by type AND by user_id -> should appear only once
	event := model.ECSTEvent{
		ID: "evt5", Type: "order.created", Source: "svc", TenantID: "t1",
		Payload:   json.RawMessage(`{"amount":100}`),
		Metadata:  map[string]string{"user_id": "u123"},
		Timestamp: time.Now(),
	}

	results := em.MatchWithEntities(ctx, event)
	if len(results) != 1 {
		t.Fatalf("expected 1 result (deduplicated), got %d", len(results))
	}
	if results[0].SubscriptionID != "sub1" {
		t.Errorf("expected sub1, got %s", results[0].SubscriptionID)
	}
}

func TestNoEntityFields(t *testing.T) {
	em, m, _, _ := setupEntityMatcher(t)
	ctx := context.Background()

	s1 := makeSub("sub1", "subscriber-a", []string{"order.created"})
	m.LoadSnapshot([]model.Subscription{s1})

	event := model.ECSTEvent{
		ID: "evt6", Type: "order.created", Source: "svc", TenantID: "t1",
		Payload:   json.RawMessage(`{"amount":100}`),
		Timestamp: time.Now(),
	}

	results := em.MatchWithEntities(ctx, event)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].SubscriptionID != "sub1" {
		t.Errorf("expected sub1, got %s", results[0].SubscriptionID)
	}
}

func TestNilResolver(t *testing.T) {
	m := New()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	em := NewEntityMatcher(m, nil, logger)
	ctx := context.Background()

	s1 := makeSub("sub1", "subscriber-a", []string{"order.created"})
	m.LoadSnapshot([]model.Subscription{s1})

	// Event with entity fields that would normally trigger resolution
	event := model.ECSTEvent{
		ID: "evt7", Type: "order.created", Source: "svc", TenantID: "t1",
		Payload:   json.RawMessage(`{"send_to":"subscriber-x","user_id":"u123"}`),
		Metadata:  map[string]string{"user_id": "u123"},
		Timestamp: time.Now(),
	}

	results := em.MatchWithEntities(ctx, event)
	// Only standard matching should work; no panic from nil resolver
	if len(results) != 1 {
		t.Fatalf("expected 1 result from standard matching only, got %d", len(results))
	}
	if results[0].SubscriptionID != "sub1" {
		t.Errorf("expected sub1, got %s", results[0].SubscriptionID)
	}
}
