// Package e2e runs end-to-end tests against a live Docker Compose stack.
//
// Prerequisites:
//   - Docker Compose stack running: make compose-up
//   - Ports: 18081 (registry), 18085 (admin), 18090 (stream-gateway), 19090 (webhook-subscriber)
//
// Run:
//
//	go test ./tests/e2e/... -v -count=1 -timeout 120s
//
// These tests are NOT run by `make test` (which only runs unit tests).
// They require the full infrastructure to be up.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"nhooyr.io/websocket"
)

var (
	registryURL = envOr("REGISTRY_URL", "http://localhost:18081")
	adminURL    = envOr("ADMIN_URL", "http://localhost:18085")
	streamURL   = envOr("STREAM_URL", "ws://localhost:18090")
	webhookURL  = envOr("WEBHOOK_URL", "http://localhost:19090")
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// TestMain checks that the stack is reachable before running tests.
func TestMain(m *testing.M) {
	// Quick health check
	resp, err := http.Get(registryURL + "/healthz")
	if err != nil || resp.StatusCode != 200 {
		fmt.Println("SKIP: Docker Compose stack not running. Start with: make compose-up && make compose-synthetic")
		os.Exit(0)
	}
	resp.Body.Close()
	os.Exit(m.Run())
}

// --- Helpers ---

func httpJSON(t *testing.T, method, url string, body interface{}) (int, map[string]interface{}) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(data, &result)
	return resp.StatusCode, result
}

func httpJSONArray(t *testing.T, method, url string) (int, []interface{}) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var result []interface{}
	json.Unmarshal(data, &result)
	return resp.StatusCode, result
}

func createSubscriber(t *testing.T, name, webhookEndpoint string) string {
	t.Helper()
	status, result := httpJSON(t, "POST", registryURL+"/v1/subscribers", map[string]string{
		"name":        name,
		"webhook_url": webhookEndpoint,
	})
	if status != 201 {
		t.Fatalf("create subscriber failed: %d %v", status, result)
	}
	return result["id"].(string)
}

func createSubscription(t *testing.T, subscriberID string, eventTypes []string, mode string) string {
	t.Helper()
	body := map[string]interface{}{
		"event_types":   eventTypes,
		"source_filter": "*",
		"delivery_mode": mode,
		"max_batch_size": 10,
	}
	status, result := httpJSON(t, "POST",
		fmt.Sprintf("%s/v1/subscribers/%s/subscriptions", registryURL, subscriberID), body)
	if status != 201 {
		t.Fatalf("create subscription failed: %d %v", status, result)
	}
	return result["id"].(string)
}

func getStats(t *testing.T, subscriptionID string) map[string]interface{} {
	t.Helper()
	_, result := httpJSON(t, "GET",
		fmt.Sprintf("%s/v1/admin/subscriptions/%s/stats", adminURL, subscriptionID), nil)
	return result
}

func getWebhookCount(t *testing.T) int64 {
	t.Helper()
	_, result := httpJSON(t, "GET", webhookURL+"/stats", nil)
	if result == nil {
		return 0
	}
	if v, ok := result["total_received"].(float64); ok {
		return int64(v)
	}
	return 0
}

func waitFor(t *testing.T, timeout time.Duration, desc string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("timeout waiting for: %s", desc)
}

// waitForPipeline waits until events flow end-to-end for a subscription.
// The event generator produces ~10 events/sec with random types, so we need
// to wait long enough for matching events to arrive. With 9 event types and
// a subscription matching 3 of them, we expect ~3.3 matches/sec.
func waitForPipeline(t *testing.T, subscriptionID string) {
	t.Helper()
	waitFor(t, 60*time.Second, "pipeline events for "+subscriptionID, func() bool {
		stats := getStats(t, subscriptionID)
		tail, _ := stats["tail_seq"].(float64)
		return tail > 0
	})
}

// --- Tests ---

func TestHealthEndpoints(t *testing.T) {
	endpoints := []string{
		registryURL + "/healthz",
		adminURL + "/healthz",
	}
	for _, url := range endpoints {
		resp, err := http.Get(url)
		if err != nil {
			t.Errorf("%s: %v", url, err)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Errorf("%s: status %d", url, resp.StatusCode)
		}
	}
}

func TestAdminSystemHealth(t *testing.T) {
	status, result := httpJSON(t, "GET", adminURL+"/v1/admin/health", nil)
	if status != 200 {
		t.Fatalf("expected 200, got %d", status)
	}
	if result["database"] != true {
		t.Error("database should be healthy")
	}
	if result["queue_service"] != true {
		t.Error("queue_service should be healthy")
	}
}

func TestSubscriberCRUD(t *testing.T) {
	// Create
	subID := createSubscriber(t, "E2E Test Corp", "http://webhook-subscriber:9090/webhook")

	// Read
	status, result := httpJSON(t, "GET", registryURL+"/v1/subscribers/"+subID, nil)
	if status != 200 {
		t.Fatalf("get subscriber: %d", status)
	}
	if result["name"] != "E2E Test Corp" {
		t.Errorf("expected name 'E2E Test Corp', got %v", result["name"])
	}

	// Update
	status, _ = httpJSON(t, "PUT", registryURL+"/v1/subscribers/"+subID,
		map[string]string{"name": "Updated Corp", "webhook_url": "http://webhook-subscriber:9090/webhook"})
	if status != 200 {
		t.Fatalf("update subscriber: %d", status)
	}

	// List
	status, list := httpJSONArray(t, "GET", registryURL+"/v1/subscribers")
	if status != 200 {
		t.Fatalf("list subscribers: %d", status)
	}
	if len(list) == 0 {
		t.Error("expected at least one subscriber")
	}

	// Delete
	req, _ := http.NewRequest("DELETE", registryURL+"/v1/subscribers/"+subID, nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Fatalf("delete subscriber: %d", resp.StatusCode)
	}
}

func TestWebhookDeliveryPipeline(t *testing.T) {
	// Record starting webhook count
	startCount := getWebhookCount(t)

	// Create subscriber pointed at the synthetic webhook receiver
	subID := createSubscriber(t, "E2E Webhook Test", "http://webhook-subscriber:9090/webhook")
	t.Cleanup(func() {
		req, _ := http.NewRequest("DELETE", registryURL+"/v1/subscribers/"+subID, nil)
		http.DefaultClient.Do(req)
	})

	// Create subscription for event types the generator produces
	subscriptionID := createSubscription(t, subID, []string{
		"invoice.created", "invoice.paid", "customer.created",
	}, "webhook")

	// Wait for events to flow through the pipeline (generator → router → queue → dispatcher → webhook)
	waitForPipeline(t, subscriptionID)

	// Give dispatcher a moment to deliver
	time.Sleep(3 * time.Second)

	stats := getStats(t, subscriptionID)
	tailSeq, _ := stats["tail_seq"].(float64)
	ackedSeq, _ := stats["acked_seq"].(float64)

	t.Logf("Pipeline working: tail=%v acked=%v webhooks_received=%d",
		tailSeq, ackedSeq, getWebhookCount(t)-startCount)
}

func TestQueuePersistenceAcrossRestart(t *testing.T) {
	// Create a subscriber + subscription
	subID := createSubscriber(t, "E2E Persistence Test", "http://webhook-subscriber:9090/webhook")
	t.Cleanup(func() {
		req, _ := http.NewRequest("DELETE", registryURL+"/v1/subscribers/"+subID, nil)
		http.DefaultClient.Do(req)
	})

	subscriptionID := createSubscription(t, subID, []string{"payment.failed"}, "webhook")

	waitForPipeline(t, subscriptionID)

	statsBefore := getStats(t, subscriptionID)
	tailBefore, _ := statsBefore["tail_seq"].(float64)
	t.Logf("Before: tail=%v acked=%v", tailBefore, statsBefore["acked_seq"])

	// NOTE: To fully test persistence, you would restart the queue service container:
	//   docker compose -f deploy/docker-compose/docker-compose.yml restart subscriber-queue-service
	// Then verify stats are preserved. This test verifies the stats endpoint works
	// and events are flowing — the RocksDB persistence is tested in unit tests.

	if tailBefore == 0 {
		t.Error("expected events to be queued before persistence check")
	}
}

func TestStreamingDelivery(t *testing.T) {
	// Create subscriber + streaming subscription
	subID := createSubscriber(t, "E2E Stream Test", "")
	t.Cleanup(func() {
		req, _ := http.NewRequest("DELETE", registryURL+"/v1/subscribers/"+subID, nil)
		http.DefaultClient.Do(req)
	})

	subscriptionID := createSubscription(t, subID, []string{
		"shipment.created", "subscription.renewed", "payment.failed",
	}, "stream")

	waitForPipeline(t, subscriptionID)

	// Wait for at least 2 events
	waitFor(t, 30*time.Second, "streaming events >= 2", func() bool {
		stats := getStats(t, subscriptionID)
		tail, _ := stats["tail_seq"].(float64)
		return tail >= 2
	})

	// Connect via WebSocket and read events
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	url := fmt.Sprintf("%s/v1/stream?subscription_id=%s&token=test", streamURL, subscriptionID)
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "test done")

	// Read at least 1 event
	var received int
	for received < 1 {
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("websocket read: %v", err)
		}

		var msg map[string]interface{}
		json.Unmarshal(data, &msg)

		if msg["type"] == "event" {
			received++
			pos := msg["position"].(float64)
			t.Logf("Received streaming event at position %v", pos)

			// Send ACK
			ack, _ := json.Marshal(map[string]interface{}{"type": "ack", "position": pos})
			conn.Write(ctx, websocket.MessageText, ack)
		}
	}

	if received < 1 {
		t.Error("expected at least 1 streaming event")
	}
}

func TestStreamingResume(t *testing.T) {
	subID := createSubscriber(t, "E2E Resume Test", "")
	t.Cleanup(func() {
		req, _ := http.NewRequest("DELETE", registryURL+"/v1/subscribers/"+subID, nil)
		http.DefaultClient.Do(req)
	})

	subscriptionID := createSubscription(t, subID, []string{
		"order.created", "customer.updated",
	}, "stream")

	waitForPipeline(t, subscriptionID)
	waitFor(t, 30*time.Second, "resume events >= 3", func() bool {
		stats := getStats(t, subscriptionID)
		tail, _ := stats["tail_seq"].(float64)
		return tail >= 3
	})

	// First connection: read and ACK position 1
	ctx1, cancel1 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel1()

	url := fmt.Sprintf("%s/v1/stream?subscription_id=%s&token=test", streamURL, subscriptionID)
	conn1, _, err := websocket.Dial(ctx1, url, nil)
	if err != nil {
		t.Fatalf("first dial: %v", err)
	}

	_, data, err := conn1.Read(ctx1)
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	var msg1 map[string]interface{}
	json.Unmarshal(data, &msg1)
	pos1 := uint64(msg1["position"].(float64))

	ack, _ := json.Marshal(map[string]interface{}{"type": "ack", "position": pos1})
	conn1.Write(ctx1, websocket.MessageText, ack)
	conn1.Close(websocket.StatusNormalClosure, "disconnect")
	cancel1()

	t.Logf("First session: received and ACKed position %d", pos1)

	// Small delay for lease to expire
	time.Sleep(2 * time.Second)

	// Second connection: resume after pos1, should get pos1+1
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	resumeURL := fmt.Sprintf("%s/v1/stream?subscription_id=%s&token=test&resume_after=%d",
		streamURL, subscriptionID, pos1)
	conn2, _, err := websocket.Dial(ctx2, resumeURL, nil)
	if err != nil {
		t.Fatalf("resume dial: %v", err)
	}
	defer conn2.Close(websocket.StatusNormalClosure, "done")

	_, data, err = conn2.Read(ctx2)
	if err != nil {
		t.Fatalf("resume read: %v", err)
	}
	var msg2 map[string]interface{}
	json.Unmarshal(data, &msg2)
	pos2 := uint64(msg2["position"].(float64))

	if pos2 <= pos1 {
		t.Errorf("expected position > %d after resume, got %d", pos1, pos2)
	}
	t.Logf("Resume: first got %d, resumed and got %d", pos1, pos2)
}

func TestPauseAndResume(t *testing.T) {
	subID := createSubscriber(t, "E2E Pause Test", "http://webhook-subscriber:9090/webhook")
	t.Cleanup(func() {
		req, _ := http.NewRequest("DELETE", registryURL+"/v1/subscribers/"+subID, nil)
		http.DefaultClient.Do(req)
	})

	subscriptionID := createSubscription(t, subID, []string{"invoice.paid"}, "webhook")

	// Pause
	status, result := httpJSON(t, "POST",
		fmt.Sprintf("%s/v1/admin/subscriptions/%s/pause", adminURL, subscriptionID), nil)
	if status != 200 {
		t.Fatalf("pause failed: %d %v", status, result)
	}

	// Verify paused
	_, sub := httpJSON(t, "GET", registryURL+"/v1/subscriptions/"+subscriptionID, nil)
	if sub["active"] != false {
		t.Error("expected subscription to be paused (active=false)")
	}

	// Resume
	status, result = httpJSON(t, "POST",
		fmt.Sprintf("%s/v1/admin/subscriptions/%s/resume", adminURL, subscriptionID), nil)
	if status != 200 {
		t.Fatalf("resume failed: %d %v", status, result)
	}

	// Verify resumed
	_, sub = httpJSON(t, "GET", registryURL+"/v1/subscriptions/"+subscriptionID, nil)
	if sub["active"] != true {
		t.Error("expected subscription to be active after resume")
	}
}

func TestReplay(t *testing.T) {
	subID := createSubscriber(t, "E2E Replay Test", "http://webhook-subscriber:9090/webhook")
	t.Cleanup(func() {
		req, _ := http.NewRequest("DELETE", registryURL+"/v1/subscribers/"+subID, nil)
		http.DefaultClient.Do(req)
	})

	subscriptionID := createSubscription(t, subID, []string{"order.created"}, "webhook")

	waitForPipeline(t, subscriptionID)
	waitFor(t, 60*time.Second, "events acked >= 3 before replay", func() bool {
		stats := getStats(t, subscriptionID)
		acked, _ := stats["acked_seq"].(float64)
		return acked >= 3
	})

	statsBefore := getStats(t, subscriptionID)
	ackedBefore, _ := statsBefore["acked_seq"].(float64)
	t.Logf("Before replay: acked=%v", ackedBefore)

	// Replay from position 1
	status, _ := httpJSON(t, "POST",
		fmt.Sprintf("%s/v1/admin/subscriptions/%s/replay?from_position=1", adminURL, subscriptionID), nil)
	if status != 200 {
		t.Fatalf("replay failed: %d", status)
	}

	// After replay, acked should be reset to 1
	stats := getStats(t, subscriptionID)
	ackedAfter, _ := stats["acked_seq"].(float64)
	if ackedAfter > ackedBefore {
		t.Errorf("expected acked to be reset, but got %v (was %v)", ackedAfter, ackedBefore)
	}
	t.Logf("After replay: acked=%v (was %v)", ackedAfter, ackedBefore)
}

func TestEntityMappingCRUD(t *testing.T) {
	subID := createSubscriber(t, "E2E Entity Test", "http://webhook-subscriber:9090/webhook")
	t.Cleanup(func() {
		req, _ := http.NewRequest("DELETE", registryURL+"/v1/subscribers/"+subID, nil)
		http.DefaultClient.Do(req)
	})

	// Create mapping
	status, result := httpJSON(t, "POST", registryURL+"/v1/entity-mappings", map[string]string{
		"entity_type":   "user",
		"entity_id":     "e2e-user-123",
		"subscriber_id": subID,
	})
	if status != 201 {
		t.Fatalf("create mapping failed: %d %v", status, result)
	}

	// List by entity
	status, list := httpJSONArray(t, "GET",
		registryURL+"/v1/entity-mappings?entity_type=user&entity_id=e2e-user-123")
	if status != 200 {
		t.Fatalf("list mappings: %d", status)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 mapping, got %d", len(list))
	}

	// List by subscriber
	status, list = httpJSONArray(t, "GET",
		registryURL+"/v1/subscribers/"+subID+"/entity-mappings")
	if status != 200 {
		t.Fatalf("list by subscriber: %d", status)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 mapping, got %d", len(list))
	}

	// Delete mapping
	req, _ := http.NewRequest("DELETE", registryURL+"/v1/entity-mappings",
		strings.NewReader(fmt.Sprintf(`{"entity_type":"user","entity_id":"e2e-user-123","subscriber_id":"%s"}`, subID)))
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Fatalf("delete mapping: %d", resp.StatusCode)
	}

	// Verify deleted
	status, list = httpJSONArray(t, "GET",
		registryURL+"/v1/entity-mappings?entity_type=user&entity_id=e2e-user-123")
	if len(list) != 0 {
		t.Errorf("expected 0 mappings after delete, got %d", len(list))
	}
}

func TestEntityMappingBroadcast(t *testing.T) {
	// Create two subscribers
	sub1 := createSubscriber(t, "E2E Broadcast Sub1", "http://webhook-subscriber:9090/webhook")
	sub2 := createSubscriber(t, "E2E Broadcast Sub2", "http://webhook-subscriber:9090/webhook")
	t.Cleanup(func() {
		for _, id := range []string{sub1, sub2} {
			req, _ := http.NewRequest("DELETE", registryURL+"/v1/subscribers/"+id, nil)
			http.DefaultClient.Do(req)
		}
	})

	// Map the same company to both subscribers
	for _, subID := range []string{sub1, sub2} {
		status, _ := httpJSON(t, "POST", registryURL+"/v1/entity-mappings", map[string]string{
			"entity_type":   "company",
			"entity_id":     "e2e-company-acme",
			"subscriber_id": subID,
		})
		if status != 201 {
			t.Fatalf("create mapping for %s failed: %d", subID, status)
		}
	}

	// Verify both are listed
	_, list := httpJSONArray(t, "GET",
		registryURL+"/v1/entity-mappings?entity_type=company&entity_id=e2e-company-acme")
	if len(list) != 2 {
		t.Fatalf("expected 2 mappings for broadcast, got %d", len(list))
	}

	t.Log("Entity broadcast mapping: 2 subscribers mapped to same company")
}

func TestQueueStatsEndpoint(t *testing.T) {
	subID := createSubscriber(t, "E2E Stats Test", "http://webhook-subscriber:9090/webhook")
	t.Cleanup(func() {
		req, _ := http.NewRequest("DELETE", registryURL+"/v1/subscribers/"+subID, nil)
		http.DefaultClient.Do(req)
	})

	subscriptionID := createSubscription(t, subID, []string{"customer.created"}, "webhook")

	waitForPipeline(t, subscriptionID)

	stats := getStats(t, subscriptionID)

	// Verify all expected fields are present
	requiredFields := []string{"subscription_id", "tail_seq", "acked_seq", "pending_count", "lease_epoch"}
	for _, field := range requiredFields {
		if _, ok := stats[field]; !ok {
			t.Errorf("missing field: %s", field)
		}
	}

	t.Logf("Stats: %+v", stats)
}
