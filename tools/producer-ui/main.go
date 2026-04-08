package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/example/event-gateway/internal/kafka"
	"github.com/example/event-gateway/internal/model"
	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
)

var publishCount atomic.Int64

var knownEventTypes = []string{
	"invoice.created",
	"invoice.paid",
	"payment.failed",
	"order.created",
	"order.updated",
	"customer.created",
	"customer.updated",
	"shipment.created",
	"subscription.renewed",
}

func main() {
	port := os.Getenv("HTTP_PORT")
	if port == "" {
		port = "8070"
	}

	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "localhost:9092"
	}
	brokers := strings.Split(brokersEnv, ",")

	producer := kafka.NewProducer(brokers, kafka.TopicECSTEvents)
	defer producer.Close()

	mux := http.NewServeMux()

	// Serve embedded UI at root.
	mux.Handle("/", UIHandler())

	// POST /api/publish — publish a single ECSTEvent.
	mux.HandleFunc("/api/publish", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var evt model.ECSTEvent
		if err := json.NewDecoder(r.Body).Decode(&evt); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
			return
		}

		// Auto-fill defaults.
		if evt.ID == "" {
			evt.ID = uuid.New().String()
		}
		if evt.Timestamp.IsZero() {
			evt.Timestamp = time.Now().UTC()
		}
		if evt.Version == "" {
			evt.Version = "1"
		}

		data, err := json.Marshal(evt)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "marshal error: " + err.Error()})
			return
		}

		key := []byte(string(evt.TenantID))
		if err := producer.Publish(context.Background(), key, data); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "kafka publish failed: " + err.Error()})
			return
		}

		publishCount.Add(1)
		writeJSON(w, http.StatusOK, evt)
	})

	// POST /api/publish-batch — publish multiple ECSTEvents in one request.
	mux.HandleFunc("/api/publish-batch", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var events []model.ECSTEvent
		if err := json.NewDecoder(r.Body).Decode(&events); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
			return
		}

		// Build Kafka messages for batch write
		msgs := make([]kafkago.Message, 0, len(events))
		for i := range events {
			evt := &events[i]
			if evt.ID == "" {
				evt.ID = uuid.New().String()
			}
			if evt.Timestamp.IsZero() {
				evt.Timestamp = time.Now().UTC()
			}
			if evt.Version == "" {
				evt.Version = "1"
			}

			data, err := json.Marshal(evt)
			if err != nil {
				continue
			}
			msgs = append(msgs, kafkago.Message{
				Key:   []byte(string(evt.TenantID)),
				Value: data,
			})
		}

		if err := producer.PublishBatch(context.Background(), msgs); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "batch publish failed: " + err.Error()})
			return
		}

		publishCount.Add(int64(len(msgs)))
		writeJSON(w, http.StatusOK, map[string]any{
			"published": len(msgs),
			"total":     len(events),
			"events":    events,
		})
	})

	// GET /api/targets — returns active subscriptions with subscriber info so
	// the UI can show which event types will reach which consumers.
	mux.HandleFunc("/api/targets", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		registryURL := os.Getenv("REGISTRY_URL")
		if registryURL == "" {
			registryURL = "http://localhost:18081"
		}
		adminURL := os.Getenv("ADMIN_URL")
		if adminURL == "" {
			adminURL = "http://localhost:18085"
		}

		// Fetch subscriptions from admin API
		subsResp, err := http.Get(adminURL + "/v1/admin/subscriptions")
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		defer subsResp.Body.Close()
		var subs []map[string]interface{}
		json.NewDecoder(subsResp.Body).Decode(&subs)

		// Fetch subscribers from registry
		subrsResp, err := http.Get(registryURL + "/v1/subscribers")
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		defer subrsResp.Body.Close()
		var subscribers []map[string]interface{}
		json.NewDecoder(subrsResp.Body).Decode(&subscribers)

		// Build subscriber lookup
		subMap := map[string]map[string]interface{}{}
		for _, s := range subscribers {
			if id, ok := s["id"].(string); ok {
				subMap[id] = s
			}
		}

		// Fetch entity mappings
		mappingsResp, err := http.Get(registryURL + "/v1/entity-mappings/all")
		var mappings []map[string]interface{}
		if err == nil {
			defer mappingsResp.Body.Close()
			json.NewDecoder(mappingsResp.Body).Decode(&mappings)
		}

		// Enrich subscriptions with subscriber info
		type target struct {
			SubscriptionID string                 `json:"subscription_id"`
			SubscriberID   string                 `json:"subscriber_id"`
			SubscriberName string                 `json:"subscriber_name"`
			WebhookURL     string                 `json:"webhook_url"`
			DeliveryMode   string                 `json:"delivery_mode"`
			EventTypes     interface{}            `json:"event_types"`
			Active         interface{}            `json:"active"`
			EntityMappings []map[string]interface{} `json:"entity_mappings"`
		}

		targets := make([]target, 0, len(subs))
		for _, s := range subs {
			subID, _ := s["subscriber_id"].(string)
			subscriber := subMap[subID]

			name := ""
			webhookURL := ""
			if subscriber != nil {
				name, _ = subscriber["name"].(string)
				webhookURL, _ = subscriber["webhook_url"].(string)
			}

			// Find entity mappings for this subscriber
			var subMappings []map[string]interface{}
			for _, m := range mappings {
				if mSubID, ok := m["subscriber_id"].(string); ok && mSubID == subID {
					subMappings = append(subMappings, m)
				}
			}

			targets = append(targets, target{
				SubscriptionID: s["id"].(string),
				SubscriberID:   subID,
				SubscriberName: name,
				WebhookURL:     webhookURL,
				DeliveryMode:   s["delivery_mode"].(string),
				EventTypes:     s["event_types"],
				Active:         s["active"],
				EntityMappings: subMappings,
			})
		}

		writeJSON(w, http.StatusOK, targets)
	})

	// GET /api/event-types — return known event types.
	mux.HandleFunc("/api/event-types", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, http.StatusOK, knownEventTypes)
	})

	// GET /api/stats — return session publish count.
	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"published": publishCount.Load(),
			"brokers":   brokersEnv,
			"topic":     kafka.TopicECSTEvents,
		})
	})

	addr := fmt.Sprintf(":%s", port)
	log.Printf("Producer UI listening on %s (brokers: %s)", addr, brokersEnv)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
