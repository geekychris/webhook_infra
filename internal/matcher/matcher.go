package matcher

import (
	"path/filepath"
	"sync"

	"github.com/example/event-gateway/internal/model"
)

// MatchResult holds the result of matching an event to a subscription.
type MatchResult struct {
	SubscriptionID model.SubscriptionID
	SubscriberID   model.SubscriberID
}

// Matcher holds subscriptions in memory and matches incoming ECSTEvents.
type Matcher struct {
	mu           sync.RWMutex
	byType       map[string][]*model.Subscription
	bySubscriber map[model.SubscriberID][]*model.Subscription
}

// New creates a new empty Matcher.
func New() *Matcher {
	return &Matcher{
		byType:       make(map[string][]*model.Subscription),
		bySubscriber: make(map[model.SubscriberID][]*model.Subscription),
	}
}

// LoadSnapshot replaces all subscriptions with the provided snapshot.
func (m *Matcher) LoadSnapshot(subs []model.Subscription) {
	index := make(map[string][]*model.Subscription)
	for i := range subs {
		sub := &subs[i]
		for _, et := range sub.EventTypes {
			index[et] = append(index[et], sub)
		}
	}

	subIdx := make(map[model.SubscriberID][]*model.Subscription)
	for i := range subs {
		s := &subs[i]
		if !s.Active {
			continue
		}
		subIdx[s.SubscriberID] = append(subIdx[s.SubscriberID], s)
	}

	m.mu.Lock()
	m.byType = index
	m.bySubscriber = subIdx
	m.mu.Unlock()
}

// Match returns all subscriptions that match the given event.
func (m *Matcher) Match(event model.ECSTEvent) []*MatchResult {
	m.mu.RLock()
	candidates := m.byType[event.Type]
	m.mu.RUnlock()

	var results []*MatchResult
	for _, sub := range candidates {
		if !sub.Active {
			continue
		}
		if !matchSource(sub.SourceFilter, event.Source) {
			continue
		}
		if !matchTenant(sub.TenantFilter, event.TenantID) {
			continue
		}
		results = append(results, &MatchResult{
			SubscriptionID: sub.ID,
			SubscriberID:   sub.SubscriberID,
		})
	}
	return results
}

// SubscriptionsForSubscriber returns all active subscriptions for a given subscriber ID.
func (m *Matcher) SubscriptionsForSubscriber(subscriberID model.SubscriberID) []*model.Subscription {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.bySubscriber[subscriberID]
}

// matchSource checks whether the event source matches the subscription's glob filter.
// An empty filter or "*" matches everything.
func matchSource(filter, source string) bool {
	if filter == "" || filter == "*" {
		return true
	}
	matched, err := filepath.Match(filter, source)
	if err != nil {
		return false
	}
	return matched
}

// matchTenant checks whether the event tenant is in the subscription's tenant filter.
// An empty filter matches all tenants.
func matchTenant(filter []model.TenantID, tenantID model.TenantID) bool {
	if len(filter) == 0 {
		return true
	}
	for _, t := range filter {
		if t == tenantID {
			return true
		}
	}
	return false
}
