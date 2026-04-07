package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// leaseEntry tracks an exclusive delivery lease for a subscription.
type leaseEntry struct {
	HolderID  string
	Epoch     uint64
	ExpiresAt time.Time
}

// LeaseManager manages in-memory delivery leases with epoch-based fencing.
type LeaseManager struct {
	mu     sync.Mutex
	leases map[string]*leaseEntry
}

// NewLeaseManager creates a new lease manager and starts a background
// goroutine that expires stale leases. Cancel the context to stop it.
func NewLeaseManager(ctx context.Context) *LeaseManager {
	lm := &LeaseManager{
		leases: make(map[string]*leaseEntry),
	}
	go lm.reapLoop(ctx)
	return lm
}

// Acquire grants a lease for subID to holderID with the given TTL.
// If no lease exists or the current lease has expired, a new lease is created
// with an incremented epoch. Returns an error if the lease is held by another holder.
func (lm *LeaseManager) Acquire(subID, holderID string, ttl time.Duration) (epoch uint64, expiresAt time.Time, err error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	now := time.Now()
	existing, ok := lm.leases[subID]

	if ok && existing.ExpiresAt.After(now) && existing.HolderID != holderID {
		return 0, time.Time{}, fmt.Errorf("lease for %s held by %s until %s",
			subID, existing.HolderID, existing.ExpiresAt.Format(time.RFC3339))
	}

	var nextEpoch uint64 = 1
	if ok {
		nextEpoch = existing.Epoch + 1
	}

	expires := now.Add(ttl)
	lm.leases[subID] = &leaseEntry{
		HolderID:  holderID,
		Epoch:     nextEpoch,
		ExpiresAt: expires,
	}
	return nextEpoch, expires, nil
}

// Renew extends an existing lease. The caller must present the correct epoch
// and holder ID. Returns the new expiration time.
func (lm *LeaseManager) Renew(subID, holderID string, epoch uint64, ttl time.Duration) (time.Time, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	existing, ok := lm.leases[subID]
	if !ok {
		return time.Time{}, fmt.Errorf("no lease for subscription %s", subID)
	}
	if existing.Epoch != epoch {
		return time.Time{}, fmt.Errorf("epoch mismatch: expected %d, got %d", existing.Epoch, epoch)
	}
	if existing.HolderID != holderID {
		return time.Time{}, fmt.Errorf("holder mismatch: lease held by %s", existing.HolderID)
	}

	expires := time.Now().Add(ttl)
	existing.ExpiresAt = expires
	return expires, nil
}

// Release explicitly releases a lease. The caller must present the correct
// epoch and holder ID.
func (lm *LeaseManager) Release(subID, holderID string, epoch uint64) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	existing, ok := lm.leases[subID]
	if !ok {
		return nil // already released
	}
	if existing.Epoch != epoch {
		return fmt.Errorf("epoch mismatch: expected %d, got %d", existing.Epoch, epoch)
	}
	if existing.HolderID != holderID {
		return fmt.Errorf("holder mismatch: lease held by %s", existing.HolderID)
	}

	delete(lm.leases, subID)
	return nil
}

// Validate checks that the given epoch matches the current lease for subID.
func (lm *LeaseManager) Validate(subID string, epoch uint64) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	existing, ok := lm.leases[subID]
	if !ok {
		return fmt.Errorf("no lease for subscription %s", subID)
	}
	if existing.Epoch != epoch {
		return fmt.Errorf("epoch mismatch: expected %d, got %d", existing.Epoch, epoch)
	}
	if time.Now().After(existing.ExpiresAt) {
		return fmt.Errorf("lease for %s has expired", subID)
	}
	return nil
}

// GetLease returns the current lease entry for a subscription, or nil if none.
func (lm *LeaseManager) GetLease(subID string) *leaseEntry {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	entry, ok := lm.leases[subID]
	if !ok {
		return nil
	}
	// Return a copy.
	copy := *entry
	return &copy
}

// reapLoop periodically removes expired leases.
func (lm *LeaseManager) reapLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lm.mu.Lock()
			now := time.Now()
			for subID, entry := range lm.leases {
				if now.After(entry.ExpiresAt) {
					delete(lm.leases, subID)
				}
			}
			lm.mu.Unlock()
		}
	}
}
