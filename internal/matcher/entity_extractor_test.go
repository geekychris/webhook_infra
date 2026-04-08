package matcher

import (
	"encoding/json"
	"testing"

	"github.com/example/event-gateway/internal/model"
)

func TestDefaultExtractUserIDFromMetadata(t *testing.T) {
	ext := NewEntityExtractor()
	event := model.ECSTEvent{
		ID: "e1", Type: "order.created", Source: "svc",
		Metadata: map[string]string{"user_id": "u123"},
		Payload:  json.RawMessage(`{}`),
	}

	refs, direct := ext.Extract(event)
	if len(direct) != 0 {
		t.Errorf("expected no direct subscribers, got %v", direct)
	}
	if len(refs) != 1 || refs[0].Type != "user" || refs[0].ID != "u123" {
		t.Errorf("expected [{user u123}], got %v", refs)
	}
}

func TestDefaultExtractCompanyIDFromPayload(t *testing.T) {
	ext := NewEntityExtractor()
	event := model.ECSTEvent{
		ID: "e1", Type: "order.created", Source: "svc",
		Payload: json.RawMessage(`{"company_id":"c_acme","amount":100}`),
	}

	refs, _ := ext.Extract(event)
	found := false
	for _, r := range refs {
		if r.Type == "company" && r.ID == "c_acme" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected company ref for c_acme, got %v", refs)
	}
}

func TestDefaultExtractSendTo(t *testing.T) {
	ext := NewEntityExtractor()
	event := model.ECSTEvent{
		ID: "e1", Type: "notification.send", Source: "svc",
		Payload: json.RawMessage(`{"send_to":["sub-1","sub-2"],"msg":"hi"}`),
	}

	_, direct := ext.Extract(event)
	if len(direct) != 2 {
		t.Fatalf("expected 2 direct subscribers, got %d", len(direct))
	}
	if direct[0] != "sub-1" || direct[1] != "sub-2" {
		t.Errorf("expected [sub-1 sub-2], got %v", direct)
	}
}

func TestCustomExtractorOverridesDefault(t *testing.T) {
	ext := NewEntityExtractor()

	// task.assigned stores user ID in "owner_id" and team in "team_id"
	ext.Register("task.assigned", func(event model.ECSTEvent, payload map[string]interface{}) ([]EntityRef, []model.SubscriberID) {
		var refs []EntityRef
		if ownerID, ok := payload["owner_id"].(string); ok {
			refs = append(refs, EntityRef{Type: "user", ID: ownerID})
		}
		if teamID, ok := payload["team_id"].(string); ok {
			refs = append(refs, EntityRef{Type: "team", ID: teamID})
		}
		return refs, nil
	})

	event := model.ECSTEvent{
		ID: "e1", Type: "task.assigned", Source: "svc",
		Payload: json.RawMessage(`{"owner_id":"u999","team_id":"t42","user_id":"u_ignored"}`),
	}

	refs, direct := ext.Extract(event)
	if len(direct) != 0 {
		t.Errorf("expected no direct, got %v", direct)
	}
	if len(refs) != 2 {
		t.Fatalf("expected 2 refs, got %d: %v", len(refs), refs)
	}

	// Should have user:u999 and team:t42, NOT user:u_ignored (custom replaces default)
	types := map[string]string{}
	for _, r := range refs {
		types[r.Type] = r.ID
	}
	if types["user"] != "u999" {
		t.Errorf("expected user:u999, got user:%s", types["user"])
	}
	if types["team"] != "t42" {
		t.Errorf("expected team:t42, got team:%s", types["team"])
	}
}

func TestCustomExtractorDoesNotAffectOtherTypes(t *testing.T) {
	ext := NewEntityExtractor()

	ext.Register("task.assigned", func(event model.ECSTEvent, payload map[string]interface{}) ([]EntityRef, []model.SubscriberID) {
		return []EntityRef{{Type: "custom", ID: "xyz"}}, nil
	})

	// Different event type uses default extractor
	event := model.ECSTEvent{
		ID: "e1", Type: "order.created", Source: "svc",
		Metadata: map[string]string{"user_id": "u123"},
		Payload:  json.RawMessage(`{}`),
	}

	refs, _ := ext.Extract(event)
	if len(refs) != 1 || refs[0].Type != "user" {
		t.Errorf("expected default extraction for order.created, got %v", refs)
	}
}

func TestCustomExtractorWithDefaultFallback(t *testing.T) {
	ext := NewEntityExtractor()

	// Custom extractor that adds a custom entity on top of the defaults
	ext.Register("payment.received", func(event model.ECSTEvent, payload map[string]interface{}) ([]EntityRef, []model.SubscriberID) {
		// Get default entities first
		refs, direct := DefaultExtract(event, payload)
		// Add a custom "merchant" entity
		if merchantID, ok := payload["merchant_id"].(string); ok {
			refs = append(refs, EntityRef{Type: "merchant", ID: merchantID})
		}
		return refs, direct
	})

	event := model.ECSTEvent{
		ID: "e1", Type: "payment.received", Source: "svc",
		Metadata: map[string]string{"user_id": "u1"},
		Payload:  json.RawMessage(`{"merchant_id":"m_stripe"}`),
	}

	refs, _ := ext.Extract(event)
	types := map[string]string{}
	for _, r := range refs {
		types[r.Type] = r.ID
	}
	if types["user"] != "u1" {
		t.Errorf("expected user:u1 from default, got %v", types)
	}
	if types["merchant"] != "m_stripe" {
		t.Errorf("expected merchant:m_stripe from custom, got %v", types)
	}
}

func TestExtractNoPayload(t *testing.T) {
	ext := NewEntityExtractor()
	event := model.ECSTEvent{
		ID: "e1", Type: "ping", Source: "svc",
	}
	refs, direct := ext.Extract(event)
	if len(refs) != 0 || len(direct) != 0 {
		t.Errorf("expected nothing, got refs=%v direct=%v", refs, direct)
	}
}
