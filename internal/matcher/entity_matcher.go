package matcher

import (
	"context"
	"log/slog"

	"github.com/example/event-gateway/internal/entity"
	"github.com/example/event-gateway/internal/model"
)

// EntityMatcher extends Matcher with entity-based routing via Redis lookups
// and a pluggable per-event-type extraction registry.
type EntityMatcher struct {
	matcher   *Matcher
	resolver  *entity.Resolver
	extractor *EntityExtractor
	logger    *slog.Logger
}

// NewEntityMatcher creates an EntityMatcher that wraps the given Matcher and Resolver.
// If resolver is nil, entity-based routing is disabled and only standard matching is used.
func NewEntityMatcher(matcher *Matcher, resolver *entity.Resolver, logger *slog.Logger) *EntityMatcher {
	return &EntityMatcher{
		matcher:   matcher,
		resolver:  resolver,
		extractor: NewEntityExtractor(),
		logger:    logger,
	}
}

// Extractor returns the entity extractor so callers can register custom
// per-event-type extraction functions.
func (em *EntityMatcher) Extractor() *EntityExtractor {
	return em.extractor
}

// MatchWithEntities performs full matching: type/source/tenant + entity routing.
//
// The flow is:
//  1. Standard Match() for type/source/tenant hits
//  2. Run the entity extractor for this event type (default or custom-registered)
//  3. For each direct subscriber ID (send_to), find their active subscriptions
//  4. For each entity ref, resolve via Redis and find active subscriptions
//  5. Deduplicate all results
func (em *EntityMatcher) MatchWithEntities(ctx context.Context, event model.ECSTEvent) []*MatchResult {
	// Step 1: standard type/source/tenant matches
	results := em.matcher.Match(event)

	// Build dedup set from standard results
	seen := make(map[model.SubscriptionID]struct{}, len(results))
	for _, r := range results {
		seen[r.SubscriptionID] = struct{}{}
	}

	// If resolver is nil, skip entity-based routing
	if em.resolver == nil {
		return results
	}

	// Step 2: extract entity refs and direct subscribers using the pluggable extractor
	entityRefs, directSubscribers := em.extractor.Extract(event)

	// Step 3: direct subscriber routing (send_to)
	for _, subID := range directSubscribers {
		subs := em.matcher.SubscriptionsForSubscriber(subID)
		for _, sub := range subs {
			if _, exists := seen[sub.ID]; !exists {
				seen[sub.ID] = struct{}{}
				results = append(results, &MatchResult{
					SubscriptionID: sub.ID,
					SubscriberID:   sub.SubscriberID,
				})
			}
		}
	}

	// Step 4: entity-based routing via Redis
	for _, ref := range entityRefs {
		subscriberIDs, err := em.resolver.Resolve(ctx, ref.Type, ref.ID)
		if err != nil {
			em.logger.Warn("entity resolution failed",
				"error", err,
				"entity_type", ref.Type,
				"entity_id", ref.ID,
			)
			continue
		}
		for _, subID := range subscriberIDs {
			subs := em.matcher.SubscriptionsForSubscriber(subID)
			for _, sub := range subs {
				if _, exists := seen[sub.ID]; !exists {
					seen[sub.ID] = struct{}{}
					results = append(results, &MatchResult{
						SubscriptionID: sub.ID,
						SubscriberID:   sub.SubscriberID,
					})
				}
			}
		}
	}

	return results
}
