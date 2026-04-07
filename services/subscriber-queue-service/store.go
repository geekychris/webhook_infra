package main

import (
	"fmt"
	"sync"
	"time"
)

// QueueStore is the storage interface for per-subscription event queues.
// This in-memory implementation is suitable for development and testing.
// For production, swap in a RocksDB-backed implementation.
type QueueStore interface {
	Append(subID string, events [][]byte) (firstPos uint64, err error)
	Read(subID string, afterPos uint64, maxCount int) ([]StoreEntry, error)
	GetCursor(subID string) (uint64, error)
	SetCursor(subID string, pos uint64) error
	GetTailPosition(subID string) (uint64, error)
	ListSubscriptionsWithPending() ([]string, error)
	Close() error
}

// StoreEntry is a single event in the queue.
type StoreEntry struct {
	Position   uint64
	Data       []byte
	EnqueuedAt int64 // unix nanos
}

// subscriptionQueue holds the append-only log and acked cursor for one subscription.
type subscriptionQueue struct {
	mu      sync.RWMutex
	entries []StoreEntry
	cursor  uint64 // last acked position (0 = nothing acked)
}

// MemoryQueueStore is an in-memory implementation of QueueStore.
type MemoryQueueStore struct {
	mu     sync.RWMutex
	queues map[string]*subscriptionQueue
}

// NewMemoryQueueStore creates a new in-memory queue store.
func NewMemoryQueueStore() *MemoryQueueStore {
	return &MemoryQueueStore{
		queues: make(map[string]*subscriptionQueue),
	}
}

func (s *MemoryQueueStore) getOrCreate(subID string) *subscriptionQueue {
	s.mu.RLock()
	q, ok := s.queues[subID]
	s.mu.RUnlock()
	if ok {
		return q
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// Double-check after acquiring write lock.
	if q, ok = s.queues[subID]; ok {
		return q
	}
	q = &subscriptionQueue{}
	s.queues[subID] = q
	return q
}

// Append adds events to a subscription's queue and returns the first assigned position.
// Positions are 1-based and monotonically increasing per subscription.
func (s *MemoryQueueStore) Append(subID string, events [][]byte) (uint64, error) {
	if len(events) == 0 {
		return 0, fmt.Errorf("no events to append")
	}

	q := s.getOrCreate(subID)
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now().UnixNano()
	var nextPos uint64 = 1
	if len(q.entries) > 0 {
		nextPos = q.entries[len(q.entries)-1].Position + 1
	}

	firstPos := nextPos
	for _, data := range events {
		q.entries = append(q.entries, StoreEntry{
			Position:   nextPos,
			Data:       data,
			EnqueuedAt: now,
		})
		nextPos++
	}
	return firstPos, nil
}

// Read returns up to maxCount entries with position > afterPos.
func (s *MemoryQueueStore) Read(subID string, afterPos uint64, maxCount int) ([]StoreEntry, error) {
	q := s.getOrCreate(subID)
	q.mu.RLock()
	defer q.mu.RUnlock()

	if len(q.entries) == 0 {
		return nil, nil
	}

	// Binary search for the first entry with position > afterPos.
	startIdx := 0
	if afterPos > 0 {
		// Positions are 1-based and contiguous, so we can compute the index directly.
		firstPos := q.entries[0].Position
		if afterPos >= q.entries[len(q.entries)-1].Position {
			return nil, nil
		}
		if afterPos >= firstPos {
			startIdx = int(afterPos - firstPos + 1)
		}
	}

	end := startIdx + maxCount
	if end > len(q.entries) {
		end = len(q.entries)
	}

	if startIdx >= len(q.entries) {
		return nil, nil
	}

	// Return copies to avoid data races.
	result := make([]StoreEntry, end-startIdx)
	copy(result, q.entries[startIdx:end])
	return result, nil
}

// GetCursor returns the last acked position for a subscription.
func (s *MemoryQueueStore) GetCursor(subID string) (uint64, error) {
	q := s.getOrCreate(subID)
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.cursor, nil
}

// SetCursor sets the acked cursor for a subscription.
func (s *MemoryQueueStore) SetCursor(subID string, pos uint64) error {
	q := s.getOrCreate(subID)
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.entries) > 0 && pos > q.entries[len(q.entries)-1].Position {
		return fmt.Errorf("cursor position %d exceeds tail %d", pos, q.entries[len(q.entries)-1].Position)
	}

	q.cursor = pos
	return nil
}

// GetTailPosition returns the position of the last entry in the subscription's queue.
func (s *MemoryQueueStore) GetTailPosition(subID string) (uint64, error) {
	q := s.getOrCreate(subID)
	q.mu.RLock()
	defer q.mu.RUnlock()

	if len(q.entries) == 0 {
		return 0, nil
	}
	return q.entries[len(q.entries)-1].Position, nil
}

// ListSubscriptionsWithPending returns subscription IDs that have unacked events.
func (s *MemoryQueueStore) ListSubscriptionsWithPending() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []string
	for subID, q := range s.queues {
		q.mu.RLock()
		hasPending := len(q.entries) > 0 && q.cursor < q.entries[len(q.entries)-1].Position
		q.mu.RUnlock()
		if hasPending {
			result = append(result, subID)
		}
	}
	return result, nil
}

// Close is a no-op for the in-memory store.
func (s *MemoryQueueStore) Close() error {
	return nil
}
