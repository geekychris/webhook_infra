//go:build cgo && rocksdb

package main

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func setupRocksDBStore(t *testing.T) *RocksDBQueueStore {
	t.Helper()
	return setupRocksDBStoreWithTTL(t, DefaultTTL)
}

func setupRocksDBStoreWithTTL(t *testing.T, ttl time.Duration) *RocksDBQueueStore {
	t.Helper()
	dir, err := os.MkdirTemp("", "rocksdb-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	store, err := NewRocksDBQueueStore(RocksDBOptions{Path: dir, TTL: ttl})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestRocksDBAppendAndRead(t *testing.T) {
	s := setupRocksDBStore(t)

	first, err := s.Append("sub-1", [][]byte{[]byte("event-a"), []byte("event-b"), []byte("event-c")})
	if err != nil {
		t.Fatal(err)
	}
	if first != 1 {
		t.Fatalf("expected first position 1, got %d", first)
	}

	entries, err := s.Read("sub-1", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	if string(entries[0].Data) != "event-a" || entries[0].Position != 1 {
		t.Errorf("entry 0: got pos=%d data=%q", entries[0].Position, entries[0].Data)
	}
	if string(entries[2].Data) != "event-c" || entries[2].Position != 3 {
		t.Errorf("entry 2: got pos=%d data=%q", entries[2].Position, entries[2].Data)
	}
}

func TestRocksDBEnqueueTimestampStored(t *testing.T) {
	s := setupRocksDBStore(t)

	before := time.Now().UnixNano()
	s.Append("sub-1", [][]byte{[]byte("hello")})
	after := time.Now().UnixNano()

	entries, _ := s.Read("sub-1", 0, 1)
	if len(entries) != 1 {
		t.Fatal("expected 1 entry")
	}
	if entries[0].EnqueuedAt < before || entries[0].EnqueuedAt > after {
		t.Errorf("EnqueuedAt %d not in [%d, %d]", entries[0].EnqueuedAt, before, after)
	}
}

func TestRocksDBReadAfterPosition(t *testing.T) {
	s := setupRocksDBStore(t)

	s.Append("sub-1", [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")})

	entries, err := s.Read("sub-1", 2, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries after pos 2, got %d", len(entries))
	}
	if entries[0].Position != 3 || entries[1].Position != 4 {
		t.Errorf("expected positions [3,4], got [%d,%d]", entries[0].Position, entries[1].Position)
	}
}

func TestRocksDBCursorAndTail(t *testing.T) {
	s := setupRocksDBStore(t)

	s.Append("sub-1", [][]byte{[]byte("a"), []byte("b"), []byte("c")})

	tail, _ := s.GetTailPosition("sub-1")
	if tail != 3 {
		t.Fatalf("expected tail 3, got %d", tail)
	}

	cursor, _ := s.GetCursor("sub-1")
	if cursor != 0 {
		t.Fatalf("expected cursor 0, got %d", cursor)
	}

	s.SetCursor("sub-1", 2)
	cursor, _ = s.GetCursor("sub-1")
	if cursor != 2 {
		t.Fatalf("expected cursor 2, got %d", cursor)
	}
}

func TestRocksDBListPending(t *testing.T) {
	s := setupRocksDBStore(t)

	s.Append("sub-1", [][]byte{[]byte("a")})
	s.Append("sub-2", [][]byte{[]byte("b")})
	s.Append("sub-3", [][]byte{[]byte("c")})
	s.SetCursor("sub-2", 1)

	pending, err := s.ListSubscriptionsWithPending()
	if err != nil {
		t.Fatal(err)
	}

	pendingSet := map[string]bool{}
	for _, id := range pending {
		pendingSet[id] = true
	}

	if !pendingSet["sub-1"] || !pendingSet["sub-3"] {
		t.Errorf("expected sub-1 and sub-3 in pending, got %v", pending)
	}
	if pendingSet["sub-2"] {
		t.Error("sub-2 should not be pending")
	}
}

func TestRocksDBIsolation(t *testing.T) {
	s := setupRocksDBStore(t)

	s.Append("sub-A", [][]byte{[]byte("alpha")})
	s.Append("sub-B", [][]byte{[]byte("beta")})

	entriesA, _ := s.Read("sub-A", 0, 10)
	entriesB, _ := s.Read("sub-B", 0, 10)

	if len(entriesA) != 1 || string(entriesA[0].Data) != "alpha" {
		t.Errorf("sub-A: expected [alpha], got %v", entriesA)
	}
	if len(entriesB) != 1 || string(entriesB[0].Data) != "beta" {
		t.Errorf("sub-B: expected [beta], got %v", entriesB)
	}
}

func TestRocksDBCursorCompaction(t *testing.T) {
	s := setupRocksDBStore(t)

	s.Append("sub-1", [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")})
	s.SetCursor("sub-1", 3)

	s.runCompaction()

	entries, _ := s.Read("sub-1", 0, 10)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries after compaction, got %d", len(entries))
	}
	if entries[0].Position != 4 || entries[1].Position != 5 {
		t.Errorf("expected positions [4,5], got [%d,%d]", entries[0].Position, entries[1].Position)
	}

	tail, _ := s.GetTailPosition("sub-1")
	if tail != 5 {
		t.Errorf("tail should still be 5, got %d", tail)
	}
}

func TestRocksDBTTLCompaction(t *testing.T) {
	// Use a very short TTL so we can test expiration
	s := setupRocksDBStoreWithTTL(t, 1*time.Millisecond)

	s.Append("sub-1", [][]byte{[]byte("old-a"), []byte("old-b")})

	// Wait for entries to expire
	time.Sleep(10 * time.Millisecond)

	// Append fresh entries
	s.Append("sub-1", [][]byte{[]byte("new-c")})

	// Run TTL compaction
	s.runCompaction()

	// Old entries should be gone, new one should remain
	entries, _ := s.Read("sub-1", 0, 10)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after TTL expiration, got %d", len(entries))
	}
	if string(entries[0].Data) != "new-c" {
		t.Errorf("expected new-c, got %q", entries[0].Data)
	}

	// Cursor should have been advanced past the expired entries
	cursor, _ := s.GetCursor("sub-1")
	if cursor != 2 {
		t.Errorf("expected cursor advanced to 2, got %d", cursor)
	}
}

func TestRocksDBTTLDoesNotDeleteFreshEntries(t *testing.T) {
	s := setupRocksDBStoreWithTTL(t, 1*time.Hour)

	s.Append("sub-1", [][]byte{[]byte("a"), []byte("b"), []byte("c")})

	s.runCompaction()

	entries, _ := s.Read("sub-1", 0, 10)
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries (all fresh), got %d", len(entries))
	}
}

func TestRocksDBSurvivesRestart(t *testing.T) {
	dir, err := os.MkdirTemp("", "rocksdb-persist-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s1, err := NewRocksDBQueueStore(RocksDBOptions{Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	s1.Append("sub-1", [][]byte{[]byte("hello"), []byte("world")})
	s1.SetCursor("sub-1", 1)
	s1.Close()

	s2, err := NewRocksDBQueueStore(RocksDBOptions{Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	tail, _ := s2.GetTailPosition("sub-1")
	if tail != 2 {
		t.Fatalf("expected tail 2 after reopen, got %d", tail)
	}

	cursor, _ := s2.GetCursor("sub-1")
	if cursor != 1 {
		t.Fatalf("expected cursor 1 after reopen, got %d", cursor)
	}

	entries, _ := s2.Read("sub-1", 1, 10)
	if len(entries) != 1 || string(entries[0].Data) != "world" {
		t.Errorf("expected [world] at pos 2, got %v", entries)
	}
}

func TestRocksDBManySubscriptions(t *testing.T) {
	s := setupRocksDBStore(t)

	for i := 0; i < 100; i++ {
		subID := fmt.Sprintf("sub-%03d", i)
		events := make([][]byte, 10)
		for j := range events {
			events[j] = []byte(fmt.Sprintf("event-%d-%d", i, j))
		}
		if _, err := s.Append(subID, events); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 100; i += 2 {
		s.SetCursor(fmt.Sprintf("sub-%03d", i), 5)
	}

	pending, _ := s.ListSubscriptionsWithPending()
	if len(pending) != 100 {
		t.Errorf("expected 100 pending, got %d", len(pending))
	}

	s.SetCursor("sub-000", 10)
	pending, _ = s.ListSubscriptionsWithPending()
	if len(pending) != 99 {
		t.Errorf("expected 99 pending, got %d", len(pending))
	}
}
