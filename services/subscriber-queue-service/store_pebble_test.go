//go:build !rocksdb

package main

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func setupPersistentStore(t *testing.T) *PersistentQueueStore {
	t.Helper()
	return setupPersistentStoreWithTTL(t, DefaultTTL)
}

func setupPersistentStoreWithTTL(t *testing.T, ttl time.Duration) *PersistentQueueStore {
	t.Helper()
	dir, err := os.MkdirTemp("", "pebble-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	store, err := NewPersistentQueueStore(PersistentStoreOptions{Path: dir, TTL: ttl})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestPersistentAppendAndRead(t *testing.T) {
	s := setupPersistentStore(t)
	first, err := s.Append("sub-1", [][]byte{[]byte("a"), []byte("b"), []byte("c")})
	if err != nil {
		t.Fatal(err)
	}
	if first != 1 {
		t.Fatalf("expected first=1, got %d", first)
	}
	entries, _ := s.Read("sub-1", 0, 10)
	if len(entries) != 3 {
		t.Fatalf("expected 3, got %d", len(entries))
	}
	if string(entries[0].Data) != "a" || string(entries[2].Data) != "c" {
		t.Error("data mismatch")
	}
}

func TestPersistentTimestamp(t *testing.T) {
	s := setupPersistentStore(t)
	before := time.Now().UnixNano()
	s.Append("sub-1", [][]byte{[]byte("x")})
	after := time.Now().UnixNano()
	entries, _ := s.Read("sub-1", 0, 1)
	if entries[0].EnqueuedAt < before || entries[0].EnqueuedAt > after {
		t.Errorf("timestamp %d not in [%d, %d]", entries[0].EnqueuedAt, before, after)
	}
}

func TestPersistentCursorAndTail(t *testing.T) {
	s := setupPersistentStore(t)
	s.Append("sub-1", [][]byte{[]byte("a"), []byte("b"), []byte("c")})
	tail, _ := s.GetTailPosition("sub-1")
	if tail != 3 {
		t.Fatalf("tail: expected 3, got %d", tail)
	}
	s.SetCursor("sub-1", 2)
	cursor, _ := s.GetCursor("sub-1")
	if cursor != 2 {
		t.Fatalf("cursor: expected 2, got %d", cursor)
	}
}

func TestPersistentIsolation(t *testing.T) {
	s := setupPersistentStore(t)
	s.Append("sub-A", [][]byte{[]byte("alpha")})
	s.Append("sub-B", [][]byte{[]byte("beta")})
	a, _ := s.Read("sub-A", 0, 10)
	b, _ := s.Read("sub-B", 0, 10)
	if len(a) != 1 || string(a[0].Data) != "alpha" {
		t.Error("A mismatch")
	}
	if len(b) != 1 || string(b[0].Data) != "beta" {
		t.Error("B mismatch")
	}
}

func TestPersistentCompaction(t *testing.T) {
	s := setupPersistentStore(t)
	s.Append("sub-1", [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")})
	s.SetCursor("sub-1", 3)
	s.runCompaction()
	entries, _ := s.Read("sub-1", 0, 10)
	if len(entries) != 2 || entries[0].Position != 4 {
		t.Errorf("expected [4,5], got %v", entries)
	}
}

func TestPersistentTTLCompaction(t *testing.T) {
	s := setupPersistentStoreWithTTL(t, 50*time.Millisecond)
	s.Append("sub-1", [][]byte{[]byte("old-a"), []byte("old-b")})
	time.Sleep(100 * time.Millisecond) // ensure old entries are past TTL
	s.Append("sub-1", [][]byte{[]byte("new-c")})
	s.runCompaction()
	entries, _ := s.Read("sub-1", 0, 10)
	if len(entries) != 1 || string(entries[0].Data) != "new-c" {
		t.Fatalf("expected [new-c], got %d entries", len(entries))
	}
	cursor, _ := s.GetCursor("sub-1")
	if cursor != 2 {
		t.Errorf("cursor: expected 2, got %d", cursor)
	}
}

func TestPersistentSurvivesRestart(t *testing.T) {
	dir, _ := os.MkdirTemp("", "pebble-persist-*")
	defer os.RemoveAll(dir)

	s1, _ := NewPersistentQueueStore(PersistentStoreOptions{Path: dir})
	s1.Append("sub-1", [][]byte{[]byte("hello"), []byte("world")})
	s1.SetCursor("sub-1", 1)
	s1.Close()

	s2, _ := NewPersistentQueueStore(PersistentStoreOptions{Path: dir})
	defer s2.Close()
	tail, _ := s2.GetTailPosition("sub-1")
	cursor, _ := s2.GetCursor("sub-1")
	if tail != 2 || cursor != 1 {
		t.Fatalf("after restart: tail=%d cursor=%d", tail, cursor)
	}
	entries, _ := s2.Read("sub-1", 1, 10)
	if len(entries) != 1 || string(entries[0].Data) != "world" {
		t.Error("data not persisted")
	}
}

func TestPersistentManySubscriptions(t *testing.T) {
	s := setupPersistentStore(t)
	for i := 0; i < 100; i++ {
		events := make([][]byte, 10)
		for j := range events {
			events[j] = []byte(fmt.Sprintf("e-%d-%d", i, j))
		}
		s.Append(fmt.Sprintf("sub-%03d", i), events)
	}
	for i := 0; i < 100; i += 2 {
		s.SetCursor(fmt.Sprintf("sub-%03d", i), 5)
	}
	pending, _ := s.ListSubscriptionsWithPending()
	if len(pending) != 100 {
		t.Errorf("expected 100 pending, got %d", len(pending))
	}
}
