package entity

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/example/event-gateway/internal/model"
)

func newTestSyncer(t *testing.T) (*Syncer, *Resolver) {
	t.Helper()
	r, _ := setupTestResolver(t)
	logger := slog.Default()
	return NewSyncer(r, logger), r
}

func TestSyncerAddAction(t *testing.T) {
	syncer, resolver := newTestSyncer(t)
	ctx := context.Background()

	evt := EntityMappingEvent{
		Action:       "add",
		EntityType:   EntityTypeUser,
		EntityID:     "user-99",
		SubscriberID: model.SubscriberID("sub-abc"),
	}
	value, _ := json.Marshal(evt)

	if err := syncer.ProcessMessage(ctx, nil, value); err != nil {
		t.Fatalf("ProcessMessage returned error: %v", err)
	}

	ids, err := resolver.Resolve(ctx, EntityTypeUser, "user-99")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(ids) != 1 || ids[0] != "sub-abc" {
		t.Fatalf("expected [sub-abc], got %v", ids)
	}
}

func TestSyncerRemoveAction(t *testing.T) {
	syncer, resolver := newTestSyncer(t)
	ctx := context.Background()

	// Pre-populate a mapping.
	if err := resolver.AddMapping(ctx, EntityTypeUser, "user-99", "sub-abc"); err != nil {
		t.Fatalf("AddMapping failed: %v", err)
	}

	evt := EntityMappingEvent{
		Action:       "remove",
		EntityType:   EntityTypeUser,
		EntityID:     "user-99",
		SubscriberID: model.SubscriberID("sub-abc"),
	}
	value, _ := json.Marshal(evt)

	if err := syncer.ProcessMessage(ctx, nil, value); err != nil {
		t.Fatalf("ProcessMessage returned error: %v", err)
	}

	ids, err := resolver.Resolve(ctx, EntityTypeUser, "user-99")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected empty slice after remove, got %v", ids)
	}
}

func TestSyncerInvalidJSON(t *testing.T) {
	syncer, _ := newTestSyncer(t)
	ctx := context.Background()

	err := syncer.ProcessMessage(ctx, nil, []byte("not json at all!!!"))
	if err != nil {
		t.Fatalf("expected nil error for invalid JSON, got: %v", err)
	}
}

func TestSyncerUnknownAction(t *testing.T) {
	syncer, resolver := newTestSyncer(t)
	ctx := context.Background()

	evt := EntityMappingEvent{
		Action:       "unknown",
		EntityType:   EntityTypeUser,
		EntityID:     "user-5",
		SubscriberID: model.SubscriberID("sub-x"),
	}
	value, _ := json.Marshal(evt)

	if err := syncer.ProcessMessage(ctx, nil, value); err != nil {
		t.Fatalf("expected nil error for unknown action, got: %v", err)
	}

	// Verify nothing was written.
	ids, err := resolver.Resolve(ctx, EntityTypeUser, "user-5")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected no mappings for unknown action, got %v", ids)
	}
}
