package entity

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/example/event-gateway/internal/model"
	"github.com/redis/go-redis/v9"
)

func setupTestResolver(t *testing.T) (*Resolver, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return NewResolver(rdb), mr
}

func TestResolveNoMapping(t *testing.T) {
	r, _ := setupTestResolver(t)
	ctx := context.Background()

	ids, err := r.Resolve(ctx, EntityTypeUser, "unknown-123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected empty slice, got %v", ids)
	}
}

func TestAddAndResolve(t *testing.T) {
	r, _ := setupTestResolver(t)
	ctx := context.Background()

	sub := model.SubscriberID("sub-1")
	if err := r.AddMapping(ctx, EntityTypeUser, "user-42", sub); err != nil {
		t.Fatalf("AddMapping failed: %v", err)
	}

	ids, err := r.Resolve(ctx, EntityTypeUser, "user-42")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(ids) != 1 || ids[0] != sub {
		t.Fatalf("expected [%s], got %v", sub, ids)
	}
}

func TestAddMultipleMappings(t *testing.T) {
	r, _ := setupTestResolver(t)
	ctx := context.Background()

	subs := []model.SubscriberID{"sub-a", "sub-b", "sub-c"}
	for _, s := range subs {
		if err := r.AddMapping(ctx, EntityTypeCompany, "corp-1", s); err != nil {
			t.Fatalf("AddMapping failed: %v", err)
		}
	}

	ids, err := r.Resolve(ctx, EntityTypeCompany, "corp-1")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3 subscriber IDs, got %d", len(ids))
	}

	got := make([]string, len(ids))
	for i, id := range ids {
		got[i] = string(id)
	}
	sort.Strings(got)

	want := []string{"sub-a", "sub-b", "sub-c"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("mismatch at index %d: got %s, want %s", i, got[i], want[i])
		}
	}
}

func TestRemoveMapping(t *testing.T) {
	r, _ := setupTestResolver(t)
	ctx := context.Background()

	sub := model.SubscriberID("sub-1")
	if err := r.AddMapping(ctx, EntityTypeUser, "user-7", sub); err != nil {
		t.Fatalf("AddMapping failed: %v", err)
	}
	if err := r.RemoveMapping(ctx, EntityTypeUser, "user-7", sub); err != nil {
		t.Fatalf("RemoveMapping failed: %v", err)
	}

	ids, err := r.Resolve(ctx, EntityTypeUser, "user-7")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected empty slice after removal, got %v", ids)
	}
}

func TestWarmCache(t *testing.T) {
	r, _ := setupTestResolver(t)
	ctx := context.Background()

	mappings := []EntityMapping{
		{EntityType: EntityTypeUser, EntityID: "u1", SubscriberID: "sub-1", CreatedAt: time.Now()},
		{EntityType: EntityTypeUser, EntityID: "u1", SubscriberID: "sub-2", CreatedAt: time.Now()},
		{EntityType: EntityTypeCompany, EntityID: "c1", SubscriberID: "sub-3", CreatedAt: time.Now()},
	}

	if err := r.WarmCache(ctx, mappings); err != nil {
		t.Fatalf("WarmCache failed: %v", err)
	}

	ids, err := r.Resolve(ctx, EntityTypeUser, "u1")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 subscriber IDs for user u1, got %d", len(ids))
	}

	ids, err = r.Resolve(ctx, EntityTypeCompany, "c1")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(ids) != 1 || ids[0] != "sub-3" {
		t.Fatalf("expected [sub-3] for company c1, got %v", ids)
	}
}

func TestFlushAndReload(t *testing.T) {
	r, _ := setupTestResolver(t)
	ctx := context.Background()

	// Populate with initial data
	r.AddMapping(ctx, EntityTypeUser, "u1", "sub-old-1")
	r.AddMapping(ctx, EntityTypeUser, "u1", "sub-old-2")
	r.AddMapping(ctx, EntityTypeCompany, "c1", "sub-old-3")

	// Verify initial state
	ids, _ := r.Resolve(ctx, EntityTypeUser, "u1")
	if len(ids) != 2 {
		t.Fatalf("expected 2 before flush, got %d", len(ids))
	}

	// Flush and reload with different data (simulates DB reconciliation)
	newMappings := []EntityMapping{
		{EntityType: EntityTypeUser, EntityID: "u1", SubscriberID: "sub-new-1"},
		{EntityType: EntityTypeUser, EntityID: "u2", SubscriberID: "sub-new-2"},
	}
	if err := r.FlushAndReload(ctx, newMappings); err != nil {
		t.Fatalf("FlushAndReload failed: %v", err)
	}

	// u1 should now have only sub-new-1 (sub-old-1 and sub-old-2 are gone)
	ids, _ = r.Resolve(ctx, EntityTypeUser, "u1")
	if len(ids) != 1 || ids[0] != "sub-new-1" {
		t.Fatalf("expected [sub-new-1] after reload, got %v", ids)
	}

	// u2 should exist now
	ids, _ = r.Resolve(ctx, EntityTypeUser, "u2")
	if len(ids) != 1 || ids[0] != "sub-new-2" {
		t.Fatalf("expected [sub-new-2], got %v", ids)
	}

	// c1 should be gone (it was not in the new mappings)
	ids, _ = r.Resolve(ctx, EntityTypeCompany, "c1")
	if len(ids) != 0 {
		t.Fatalf("expected empty for c1 after flush, got %v", ids)
	}
}

func TestFlushAndReloadEmpty(t *testing.T) {
	r, _ := setupTestResolver(t)
	ctx := context.Background()

	r.AddMapping(ctx, EntityTypeUser, "u1", "sub-1")

	// Flush and reload with no mappings — should clear everything
	if err := r.FlushAndReload(ctx, nil); err != nil {
		t.Fatalf("FlushAndReload failed: %v", err)
	}

	ids, _ := r.Resolve(ctx, EntityTypeUser, "u1")
	if len(ids) != 0 {
		t.Fatalf("expected empty after flush with nil, got %v", ids)
	}
}

func TestResolveDoesNotCrossPollute(t *testing.T) {
	r, _ := setupTestResolver(t)
	ctx := context.Background()

	if err := r.AddMapping(ctx, EntityTypeUser, "id-1", "sub-user"); err != nil {
		t.Fatalf("AddMapping failed: %v", err)
	}
	if err := r.AddMapping(ctx, EntityTypeCompany, "id-1", "sub-company"); err != nil {
		t.Fatalf("AddMapping failed: %v", err)
	}
	if err := r.AddMapping(ctx, EntityTypeUser, "id-2", "sub-other"); err != nil {
		t.Fatalf("AddMapping failed: %v", err)
	}

	// user:id-1 should only have sub-user
	ids, err := r.Resolve(ctx, EntityTypeUser, "id-1")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(ids) != 1 || ids[0] != "sub-user" {
		t.Fatalf("expected [sub-user], got %v", ids)
	}

	// company:id-1 should only have sub-company
	ids, err = r.Resolve(ctx, EntityTypeCompany, "id-1")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(ids) != 1 || ids[0] != "sub-company" {
		t.Fatalf("expected [sub-company], got %v", ids)
	}

	// user:id-2 should only have sub-other
	ids, err = r.Resolve(ctx, EntityTypeUser, "id-2")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(ids) != 1 || ids[0] != "sub-other" {
		t.Fatalf("expected [sub-other], got %v", ids)
	}
}
