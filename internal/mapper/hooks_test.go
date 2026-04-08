package mapper

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/example/event-gateway/internal/model"
)

func baseEvent() model.ECSTEvent {
	return model.ECSTEvent{
		ID:        "evt-1",
		Type:      "invoice.paid",
		Source:    "billing-service",
		TenantID:  "tenant-1",
		EntityID:  "inv-100",
		Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Payload:   json.RawMessage(`{"invoice_id":"inv-100","amount":99.99,"currency":"USD","internal_code":"X123"}`),
	}
}

func TestMapperWithNoHooksUnchanged(t *testing.T) {
	m := New()
	ce, err := m.Map(baseEvent())
	if err != nil {
		t.Fatal(err)
	}
	// Default mapping passes payload through untouched
	var data map[string]interface{}
	json.Unmarshal(ce.Data, &data)
	if data["internal_code"] != "X123" {
		t.Error("expected internal_code in default mapping")
	}
}

func TestMapperTypeSpecificHook(t *testing.T) {
	m := New()

	// Register a hook that strips internal_code from invoice.paid events
	m.Register("invoice.paid", func(event model.ECSTEvent, ce model.CloudEvent) (model.CloudEvent, error) {
		var p map[string]interface{}
		json.Unmarshal(event.Payload, &p)
		delete(p, "internal_code")
		ce.Data, _ = json.Marshal(p)
		return ce, nil
	})

	ce, err := m.Map(baseEvent())
	if err != nil {
		t.Fatal(err)
	}

	var data map[string]interface{}
	json.Unmarshal(ce.Data, &data)
	if _, exists := data["internal_code"]; exists {
		t.Error("internal_code should have been stripped by hook")
	}
	if data["amount"] != 99.99 {
		t.Error("amount should still be present")
	}
}

func TestMapperHookDoesNotAffectOtherTypes(t *testing.T) {
	m := New()
	m.Register("invoice.paid", func(event model.ECSTEvent, ce model.CloudEvent) (model.CloudEvent, error) {
		ce.Data = json.RawMessage(`{"stripped":true}`)
		return ce, nil
	})

	// Different event type should NOT be affected
	event := baseEvent()
	event.Type = "order.created"
	ce, err := m.Map(event)
	if err != nil {
		t.Fatal(err)
	}

	var data map[string]interface{}
	json.Unmarshal(ce.Data, &data)
	if _, exists := data["stripped"]; exists {
		t.Error("order.created should not be affected by invoice.paid hook")
	}
}

func TestMapperWildcardHook(t *testing.T) {
	m := New()

	// Wildcard hook that adds a custom extension to all events
	m.Register("*", func(event model.ECSTEvent, ce model.CloudEvent) (model.CloudEvent, error) {
		ce.DataSchema = "https://schemas.example.com/" + event.Type
		return ce, nil
	})

	ce, err := m.Map(baseEvent())
	if err != nil {
		t.Fatal(err)
	}
	if ce.DataSchema != "https://schemas.example.com/invoice.paid" {
		t.Errorf("expected schema URL, got %q", ce.DataSchema)
	}
}

func TestMapperTypeHookRunsBeforeWildcard(t *testing.T) {
	m := New()

	var order []string

	m.Register("invoice.paid", func(event model.ECSTEvent, ce model.CloudEvent) (model.CloudEvent, error) {
		order = append(order, "type")
		return ce, nil
	})
	m.Register("*", func(event model.ECSTEvent, ce model.CloudEvent) (model.CloudEvent, error) {
		order = append(order, "wildcard")
		return ce, nil
	})

	m.Map(baseEvent())
	if len(order) != 2 || order[0] != "type" || order[1] != "wildcard" {
		t.Errorf("expected [type, wildcard], got %v", order)
	}
}

func TestMapperHookCanRejectEvent(t *testing.T) {
	m := New()
	m.Register("invoice.paid", func(event model.ECSTEvent, ce model.CloudEvent) (model.CloudEvent, error) {
		return ce, errors.New("event filtered out by policy")
	})

	_, err := m.Map(baseEvent())
	if err == nil {
		t.Fatal("expected error from rejecting hook")
	}
}
