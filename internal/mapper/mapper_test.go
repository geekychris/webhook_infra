package mapper

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/example/event-gateway/internal/model"
)

func TestMapValidEvent(t *testing.T) {
	m := New()
	ts := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)
	payload := json.RawMessage(`{"amount":99.99}`)

	event := model.ECSTEvent{
		ID:        "evt-123",
		Type:      "order.created",
		Source:    "svc-orders",
		TenantID:  "tenant-1",
		EntityID:  "order-456",
		Timestamp: ts,
		Version:   "1.0",
		Payload:   payload,
	}

	ce, err := m.Map(event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if ce.SpecVersion != "1.0" {
		t.Errorf("specversion = %q, want %q", ce.SpecVersion, "1.0")
	}
	if ce.ID != "evt-123" {
		t.Errorf("id = %q, want %q", ce.ID, "evt-123")
	}
	if ce.Type != "order.created" {
		t.Errorf("type = %q, want %q", ce.Type, "order.created")
	}
	if ce.Source != "svc-orders" {
		t.Errorf("source = %q, want %q", ce.Source, "svc-orders")
	}
	if !ce.Time.Equal(ts) {
		t.Errorf("time = %v, want %v", ce.Time, ts)
	}
	if ce.Subject != "order-456" {
		t.Errorf("subject = %q, want %q", ce.Subject, "order-456")
	}
	if ce.DataContentType != "application/json" {
		t.Errorf("datacontenttype = %q, want %q", ce.DataContentType, "application/json")
	}
	if string(ce.Data) != string(payload) {
		t.Errorf("data = %s, want %s", ce.Data, payload)
	}
}

func TestMapMissingID(t *testing.T) {
	m := New()
	_, err := m.Map(model.ECSTEvent{
		Type:   "order.created",
		Source: "svc-orders",
	})
	if err == nil {
		t.Fatal("expected error for missing ID")
	}
}

func TestMapMissingType(t *testing.T) {
	m := New()
	_, err := m.Map(model.ECSTEvent{
		ID:     "evt-1",
		Source: "svc-orders",
	})
	if err == nil {
		t.Fatal("expected error for missing Type")
	}
}

func TestMapMissingSource(t *testing.T) {
	m := New()
	_, err := m.Map(model.ECSTEvent{
		ID:   "evt-1",
		Type: "order.created",
	})
	if err == nil {
		t.Fatal("expected error for missing Source")
	}
}
