package matcher

import (
	"encoding/json"
	"sync"

	"github.com/example/event-gateway/internal/model"
)

// EntityRef identifies an entity to resolve for routing.
type EntityRef struct {
	Type string // "user", "company", or any custom entity type
	ID   string // the entity ID value
}

// ExtractFunc extracts entity references from an ECST event. The function
// receives the raw event and its parsed payload (may be nil if payload is not
// valid JSON). It returns zero or more EntityRefs to resolve via the entity
// mapping cache, plus zero or more direct subscriber IDs (for send_to style
// routing).
type ExtractFunc func(event model.ECSTEvent, payload map[string]interface{}) (entities []EntityRef, directSubscribers []model.SubscriberID)

// EntityExtractor manages per-event-type entity extraction functions. It
// provides a default extractor that handles user_id, company_id, and send_to,
// and allows registering custom extractors per event type.
type EntityExtractor struct {
	mu       sync.RWMutex
	hooks    map[string]ExtractFunc // keyed by event type
	fallback ExtractFunc            // default extractor
}

// NewEntityExtractor creates an extractor with the built-in default logic.
func NewEntityExtractor() *EntityExtractor {
	return &EntityExtractor{
		hooks:    make(map[string]ExtractFunc),
		fallback: defaultExtract,
	}
}

// Register adds a custom extraction function for an event type. This completely
// replaces the default extraction for that event type. If you want the default
// behaviour plus custom logic, call DefaultExtract inside your function.
//
// Example — an event type where user ID is in "owner_id" instead of "user_id":
//
//	extractor.Register("task.assigned", func(event model.ECSTEvent, payload map[string]interface{}) ([]EntityRef, []model.SubscriberID) {
//	    var entities []EntityRef
//	    if ownerID, ok := payload["owner_id"].(string); ok {
//	        entities = append(entities, EntityRef{Type: "user", ID: ownerID})
//	    }
//	    if teamID, ok := payload["team_id"].(string); ok {
//	        entities = append(entities, EntityRef{Type: "team", ID: teamID})
//	    }
//	    return entities, nil
//	})
func (e *EntityExtractor) Register(eventType string, fn ExtractFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.hooks[eventType] = fn
}

// Extract runs the appropriate extraction function for the event. If a custom
// function is registered for the event type, it is used. Otherwise, the default
// extractor runs.
func (e *EntityExtractor) Extract(event model.ECSTEvent) (entities []EntityRef, directSubscribers []model.SubscriberID) {
	var payload map[string]interface{}
	if len(event.Payload) > 0 {
		json.Unmarshal(event.Payload, &payload)
	}

	e.mu.RLock()
	fn, ok := e.hooks[event.Type]
	e.mu.RUnlock()

	if ok {
		return fn(event, payload)
	}
	return e.fallback(event, payload)
}

// DefaultExtract is the built-in extraction logic. It checks for send_to,
// user_id, and company_id in metadata and payload. Custom ExtractFunc
// implementations can call this to get the default behaviour and then add
// their own entities on top.
func DefaultExtract(event model.ECSTEvent, payload map[string]interface{}) (entities []EntityRef, directSubscribers []model.SubscriberID) {
	return defaultExtract(event, payload)
}

func defaultExtract(event model.ECSTEvent, payload map[string]interface{}) (entities []EntityRef, directSubscribers []model.SubscriberID) {
	// send_to — direct subscriber routing
	if payload != nil {
		if sendTo, ok := payload["send_to"]; ok {
			switch v := sendTo.(type) {
			case string:
				directSubscribers = append(directSubscribers, model.SubscriberID(v))
			case []interface{}:
				for _, item := range v {
					if s, ok := item.(string); ok {
						directSubscribers = append(directSubscribers, model.SubscriberID(s))
					}
				}
			}
		}
	}

	// user_id — metadata first, then payload
	userID := event.Metadata["user_id"]
	if userID == "" && payload != nil {
		if v, ok := payload["user_id"].(string); ok {
			userID = v
		}
	}
	if userID != "" {
		entities = append(entities, EntityRef{Type: "user", ID: userID})
	}

	// company_id — metadata first, then payload
	companyID := event.Metadata["company_id"]
	if companyID == "" && payload != nil {
		if v, ok := payload["company_id"].(string); ok {
			companyID = v
		}
	}
	if companyID != "" {
		entities = append(entities, EntityRef{Type: "company", ID: companyID})
	}

	return entities, directSubscribers
}
