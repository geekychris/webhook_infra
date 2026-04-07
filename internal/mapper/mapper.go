package mapper

import (
	"errors"

	"github.com/example/event-gateway/internal/model"
)

// Mapper converts ECSTEvents to CloudEvents v1.0.
type Mapper struct{}

// New creates a new Mapper.
func New() *Mapper {
	return &Mapper{}
}

// Map converts an ECSTEvent to a CloudEvent v1.0.
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

	return model.CloudEvent{
		SpecVersion:     "1.0",
		ID:              event.ID,
		Type:            event.Type,
		Source:          event.Source,
		Time:            event.Timestamp,
		Subject:         event.EntityID,
		DataContentType: "application/json",
		Data:            event.Payload,
	}, nil
}
