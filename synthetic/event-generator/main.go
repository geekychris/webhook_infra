package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/event-gateway/internal/config"
	"github.com/example/event-gateway/internal/kafka"
	"github.com/example/event-gateway/internal/model"
	"github.com/example/event-gateway/internal/observability"
	"github.com/google/uuid"
)

var eventTypes = []string{
	"customer.created",
	"customer.updated",
	"invoice.created",
	"invoice.paid",
	"payment.failed",
	"order.created",
	"order.updated",
	"shipment.created",
	"subscription.renewed",
}

var sources = []string{
	"billing-service",
	"customer-service",
	"order-service",
	"payment-service",
	"shipping-service",
}

var tenantIDs = []model.TenantID{
	"tenant_1", "tenant_2", "tenant_3", "tenant_4", "tenant_5",
	"tenant_10", "tenant_20", "tenant_42",
}

func main() {
	logger := observability.NewLogger("event-generator")

	brokers := config.EnvSlice("KAFKA_BROKERS", []string{"localhost:9092"})
	eventsPerSecond := config.EnvInt("EVENTS_PER_SECOND", 10)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	producer := kafka.NewProducer(brokers, kafka.TopicECSTEvents)
	defer producer.Close()

	interval := time.Second / time.Duration(eventsPerSecond)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logger.Info("event generator starting",
		"brokers", brokers,
		"events_per_second", eventsPerSecond,
	)

	count := 0
	for {
		select {
		case <-ctx.Done():
			logger.Info("generator shutting down", "total_events", count)
			return
		case <-ticker.C:
			event := generateEvent()
			data, err := json.Marshal(event)
			if err != nil {
				logger.Error("failed to marshal event", "error", err)
				continue
			}

			if err := producer.Publish(ctx, []byte(event.TenantID), data); err != nil {
				logger.Error("failed to publish event", "error", err)
				continue
			}

			count++
			if count%100 == 0 {
				logger.Info("events published", "count", count)
			}
		}
	}
}

func generateEvent() model.ECSTEvent {
	evtType := eventTypes[rand.Intn(len(eventTypes))]
	source := sources[rand.Intn(len(sources))]
	tenant := tenantIDs[rand.Intn(len(tenantIDs))]
	entityID := fmt.Sprintf("entity_%d", rand.Intn(10000))

	payload := map[string]interface{}{
		"entity_id": entityID,
		"amount":    rand.Float64() * 1000,
		"currency":  "USD",
		"status":    "completed",
	}
	payloadBytes, _ := json.Marshal(payload)

	return model.ECSTEvent{
		ID:        uuid.New().String(),
		Type:      evtType,
		Source:    source,
		TenantID:  tenant,
		EntityID:  entityID,
		Timestamp: time.Now().UTC(),
		Version:   "1",
		Payload:   payloadBytes,
	}
}
