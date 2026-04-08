package mapper

import (
	"errors"
	"sync"

	"github.com/example/event-gateway/internal/model"
)

// MapFunc is a per-event-type mapping function. It receives the raw ECST event
// and the default CloudEvent produced by the base mapper. It may modify the
// CloudEvent in place (e.g. project, filter, or enrich payload fields) and
// return it. Returning an error skips the event.
type MapFunc func(event model.ECSTEvent, ce model.CloudEvent) (model.CloudEvent, error)

// Mapper converts ECSTEvents to CloudEvents v1.0, with support for
// per-event-type custom mapping functions.
type Mapper struct {
	mu    sync.RWMutex
	hooks map[string]MapFunc // keyed by event type; "*" = wildcard for all types
}

// New creates a new Mapper with no custom hooks.
func New() *Mapper {
	return &Mapper{
		hooks: make(map[string]MapFunc),
	}
}

// Register adds a custom mapping function for an event type. The function is
// called after the base ECST→CloudEvent conversion. Use "*" to register a
// wildcard hook that runs for all event types (after the type-specific hook).
//
// Example:
//
//	m.Register("invoice.paid", func(event model.ECSTEvent, ce model.CloudEvent) (model.CloudEvent, error) {
//	    // Project only the fields subscribers need
//	    var p map[string]interface{}
//	    json.Unmarshal(event.Payload, &p)
//	    projected, _ := json.Marshal(map[string]interface{}{
//	        "invoice_id": p["invoice_id"],
//	        "amount":     p["amount"],
//	        "currency":   p["currency"],
//	    })
//	    ce.Data = projected
//	    return ce, nil
//	})
func (m *Mapper) Register(eventType string, fn MapFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hooks[eventType] = fn
}

// Map converts an ECSTEvent to a CloudEvent v1.0. If a custom mapping function
// is registered for the event type, it is called after the base conversion. If
// a wildcard ("*") hook is registered, it runs after the type-specific hook.
func (m *Mapper) Map(event model.ECSTEvent) (model.CloudEvent, error) {
	if event.ID == "" {
		return model.CloudEvent{}, errors.New("missing required field: ID")
	}
	if event.Type == "" {
		return model.CloudEvent{}, errors.New("missing required field: Type")
	}
	if event.Source == "" {
		return model.CloudEvent{}, errors.New("missing required field: Source")
	}

	ce := model.CloudEvent{
		SpecVersion:     "1.0",
		ID:              event.ID,
		Type:            event.Type,
		Source:          event.Source,
		Time:            event.Timestamp,
		Subject:         event.EntityID,
		DataContentType: "application/json",
		Data:            event.Payload,
	}

	m.mu.RLock()
	typeHook := m.hooks[event.Type]
	wildcardHook := m.hooks["*"]
	m.mu.RUnlock()

	// Apply type-specific hook first
	if typeHook != nil {
		var err error
		ce, err = typeHook(event, ce)
		if err != nil {
			return model.CloudEvent{}, err
		}
	}

	// Then apply wildcard hook
	if wildcardHook != nil {
		var err error
		ce, err = wildcardHook(event, ce)
		if err != nil {
			return model.CloudEvent{}, err
		}
	}

	return ce, nil
}
