package matcher

import (
	"testing"
	"time"

	"github.com/example/event-gateway/internal/model"
)

func baseSub(id string, eventTypes []string) model.Subscription {
	return model.Subscription{
		ID:           model.SubscriptionID(id),
		SubscriberID: model.SubscriberID("sub-" + id),
		EventTypes:   eventTypes,
		SourceFilter: "*",
		Active:       true,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
}

func baseEvent(typ, source string, tenantID string) model.ECSTEvent {
	return model.ECSTEvent{
		ID:        "evt-1",
		Type:      typ,
		Source:    source,
		TenantID:  model.TenantID(tenantID),
		EntityID:  "entity-1",
		Timestamp: time.Now(),
	}
}

func TestExactTypeMatch(t *testing.T) {
	m := New()
	s := baseSub("s1", []string{"order.created"})
	m.LoadSnapshot([]model.Subscription{s})

	results := m.Match(baseEvent("order.created", "svc-orders", "t1"))
	if len(results) != 1 {
		t.Fatalf("expected 1 match, got %d", len(results))
	}
	if results[0].SubscriptionID != "s1" {
		t.Errorf("expected subscription s1, got %s", results[0].SubscriptionID)
	}
}

func TestNoMatch(t *testing.T) {
	m := New()
	s := baseSub("s1", []string{"order.created"})
	m.LoadSnapshot([]model.Subscription{s})

	results := m.Match(baseEvent("user.deleted", "svc-users", "t1"))
	if len(results) != 0 {
		t.Fatalf("expected 0 matches, got %d", len(results))
	}
}

func TestSourceFilterGlob(t *testing.T) {
	m := New()
	s := baseSub("s1", []string{"order.created"})
	s.SourceFilter = "svc-*"
	m.LoadSnapshot([]model.Subscription{s})

	// Should match
	results := m.Match(baseEvent("order.created", "svc-orders", "t1"))
	if len(results) != 1 {
		t.Fatalf("expected 1 match, got %d", len(results))
	}

	// Should not match
	results = m.Match(baseEvent("order.created", "api-gateway", "t1"))
	if len(results) != 0 {
		t.Fatalf("expected 0 matches, got %d", len(results))
	}
}

func TestSourceFilterEmpty(t *testing.T) {
	m := New()
	s := baseSub("s1", []string{"order.created"})
	s.SourceFilter = ""
	m.LoadSnapshot([]model.Subscription{s})

	results := m.Match(baseEvent("order.created", "anything", "t1"))
	if len(results) != 1 {
		t.Fatalf("expected 1 match with empty source filter, got %d", len(results))
	}
}

func TestTenantFilter(t *testing.T) {
	m := New()
	s := baseSub("s1", []string{"order.created"})
	s.TenantFilter = []model.TenantID{"tenant-a", "tenant-b"}
	m.LoadSnapshot([]model.Subscription{s})

	// Should match
	results := m.Match(baseEvent("order.created", "svc-orders", "tenant-a"))
	if len(results) != 1 {
		t.Fatalf("expected 1 match for tenant-a, got %d", len(results))
	}

	// Should not match
	results = m.Match(baseEvent("order.created", "svc-orders", "tenant-c"))
	if len(results) != 0 {
		t.Fatalf("expected 0 matches for tenant-c, got %d", len(results))
	}
}

func TestTenantFilterEmpty(t *testing.T) {
	m := New()
	s := baseSub("s1", []string{"order.created"})
	s.TenantFilter = nil
	m.LoadSnapshot([]model.Subscription{s})

	results := m.Match(baseEvent("order.created", "svc-orders", "any-tenant"))
	if len(results) != 1 {
		t.Fatalf("expected 1 match with empty tenant filter, got %d", len(results))
	}
}

func TestMultipleSubscriptionsMatch(t *testing.T) {
	m := New()
	s1 := baseSub("s1", []string{"order.created"})
	s2 := baseSub("s2", []string{"order.created", "order.updated"})
	m.LoadSnapshot([]model.Subscription{s1, s2})

	results := m.Match(baseEvent("order.created", "svc-orders", "t1"))
	if len(results) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(results))
	}
}

func TestInactiveSubscriptionSkipped(t *testing.T) {
	m := New()
	s := baseSub("s1", []string{"order.created"})
	s.Active = false
	m.LoadSnapshot([]model.Subscription{s})

	results := m.Match(baseEvent("order.created", "svc-orders", "t1"))
	if len(results) != 0 {
		t.Fatalf("expected 0 matches for inactive subscription, got %d", len(results))
	}
}

func TestLoadSnapshotReplaces(t *testing.T) {
	m := New()
	m.LoadSnapshot([]model.Subscription{baseSub("s1", []string{"order.created"})})
	m.LoadSnapshot([]model.Subscription{baseSub("s2", []string{"user.created"})})

	results := m.Match(baseEvent("order.created", "svc", "t1"))
	if len(results) != 0 {
		t.Fatalf("expected old subscriptions to be replaced, got %d matches", len(results))
	}
	results = m.Match(baseEvent("user.created", "svc", "t1"))
	if len(results) != 1 {
		t.Fatalf("expected 1 match after reload, got %d", len(results))
	}
}
