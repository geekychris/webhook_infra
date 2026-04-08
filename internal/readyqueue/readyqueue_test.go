package readyqueue

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func setup(t *testing.T) *Queue {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return New(rdb)
}

func TestSignalAndDrain(t *testing.T) {
	q := setup(t)
	ctx := context.Background()

	q.Signal(ctx, "sub-1")
	q.Signal(ctx, "sub-2")
	q.Signal(ctx, "sub-3")

	ids, err := q.Drain(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3, got %d", len(ids))
	}

	// After drain, set should be empty
	n, _ := q.Len(ctx)
	if n != 0 {
		t.Errorf("expected 0 after drain, got %d", n)
	}
}

func TestSignalNXPreservesOrder(t *testing.T) {
	q := setup(t)
	ctx := context.Background()

	// Signal sub-1 first, then sub-2
	q.Signal(ctx, "sub-1")
	time.Sleep(2 * time.Millisecond)
	q.Signal(ctx, "sub-2")

	// Signal sub-1 again — should NOT bump its score
	q.Signal(ctx, "sub-1")

	// Drain should return sub-1 first (oldest score)
	ids, _ := q.Drain(ctx, 10)
	if len(ids) != 2 {
		t.Fatalf("expected 2, got %d", len(ids))
	}
	if ids[0] != "sub-1" {
		t.Errorf("expected sub-1 first (oldest), got %s", ids[0])
	}
	if ids[1] != "sub-2" {
		t.Errorf("expected sub-2 second, got %s", ids[1])
	}
}

func TestDrainEmpty(t *testing.T) {
	q := setup(t)
	ctx := context.Background()

	ids, err := q.Drain(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Errorf("expected empty, got %v", ids)
	}
}

func TestRemove(t *testing.T) {
	q := setup(t)
	ctx := context.Background()

	q.Signal(ctx, "sub-1")
	q.Signal(ctx, "sub-2")
	q.Remove(ctx, "sub-1")

	ids, _ := q.Drain(ctx, 10)
	if len(ids) != 1 || ids[0] != "sub-2" {
		t.Errorf("expected [sub-2], got %v", ids)
	}
}

func TestRequeueAfterDrain(t *testing.T) {
	q := setup(t)
	ctx := context.Background()

	q.Signal(ctx, "sub-1")
	ids, _ := q.Drain(ctx, 10)
	if len(ids) != 1 {
		t.Fatal("expected 1")
	}

	// Requeue after drain
	q.Requeue(ctx, "sub-1")
	ids, _ = q.Drain(ctx, 10)
	if len(ids) != 1 || ids[0] != "sub-1" {
		t.Errorf("expected [sub-1] after requeue, got %v", ids)
	}
}

func TestDrainWithLimit(t *testing.T) {
	q := setup(t)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		q.Signal(ctx, "sub-"+string(rune('a'+i)))
	}

	ids, _ := q.Drain(ctx, 3)
	if len(ids) != 3 {
		t.Fatalf("expected 3, got %d", len(ids))
	}

	// 7 should remain
	n, _ := q.Len(ctx)
	if n != 7 {
		t.Errorf("expected 7 remaining, got %d", n)
	}
}
