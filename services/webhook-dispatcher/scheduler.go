package main

import (
	"log/slog"
	"sync"
	"time"
)

// ScheduleEntry tracks the delivery state for a subscription in the scheduler.
type ScheduleEntry struct {
	SubscriptionID string
	SubscriberID   string
	WebhookURL     string
	SigningKey      []byte
	MaxBatchSize   int
	PendingCount   uint64
	BackoffUntil   time.Time
	FailCount      int
}

// Scheduler implements fair round-robin scheduling across subscriptions.
type Scheduler struct {
	mu      sync.Mutex
	entries map[string]*ScheduleEntry
	order   []string // subscription IDs in round-robin order
	index   int
	logger  *slog.Logger
}

// NewScheduler creates a fair scheduler.
func NewScheduler(logger *slog.Logger) *Scheduler {
	return &Scheduler{
		entries: make(map[string]*ScheduleEntry),
		logger:  logger,
	}
}

// UpdateEntries replaces the set of known subscriptions.
func (s *Scheduler) UpdateEntries(entries []*ScheduleEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	newEntries := make(map[string]*ScheduleEntry, len(entries))
	newOrder := make([]string, 0, len(entries))

	for _, e := range entries {
		// Preserve backoff state from existing entry
		if existing, ok := s.entries[e.SubscriptionID]; ok {
			e.BackoffUntil = existing.BackoffUntil
			e.FailCount = existing.FailCount
		}
		newEntries[e.SubscriptionID] = e
		newOrder = append(newOrder, e.SubscriptionID)
	}

	s.entries = newEntries
	s.order = newOrder
	if s.index >= len(s.order) {
		s.index = 0
	}
}

// Next returns the next subscription ready for delivery, or nil if none are ready.
// Implements fair round-robin: cycles through all subscriptions before revisiting.
func (s *Scheduler) Next() *ScheduleEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.order) == 0 {
		return nil
	}

	now := time.Now()
	checked := 0
	for checked < len(s.order) {
		id := s.order[s.index]
		s.index = (s.index + 1) % len(s.order)
		checked++

		entry := s.entries[id]
		if entry == nil {
			continue
		}
		if now.Before(entry.BackoffUntil) {
			continue
		}
		return entry
	}

	return nil
}

// Len returns the number of known subscriptions.
func (s *Scheduler) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.order)
}

// RecordSuccess resets failure tracking for a subscription.
func (s *Scheduler) RecordSuccess(subID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.entries[subID]; ok {
		e.FailCount = 0
		e.BackoffUntil = time.Time{}
	}
}

// RecordFailure records a delivery failure and sets backoff.
func (s *Scheduler) RecordFailure(subID string, backoffDuration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.entries[subID]; ok {
		e.FailCount++
		e.BackoffUntil = time.Now().Add(backoffDuration)
		s.logger.Warn("subscription backing off",
			"subscription_id", subID,
			"fail_count", e.FailCount,
			"backoff_until", e.BackoffUntil,
		)
	}
}
