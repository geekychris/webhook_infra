package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/example/event-gateway/internal/config"
	"github.com/example/event-gateway/internal/health"
	"github.com/example/event-gateway/internal/observability"
	queuev1 "github.com/example/event-gateway/internal/proto/queue/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	logger := observability.NewLogger("webhook-dispatcher")

	queueAddr := config.Env("QUEUE_SERVICE_ADDR", "localhost:50052")
	registryURL := config.Env("REGISTRY_URL", "http://localhost:8081")
	healthPort := config.Env("HEALTH_PORT", "8082")
	instanceID := config.Env("INSTANCE_ID", uuid.New().String())

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	conn, err := grpc.NewClient(queueAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("failed to connect to queue service", "addr", queueAddr, "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	queueClient := queuev1.NewQueueServiceClient(conn)

	registry := NewRegistryClient(registryURL)
	sender := NewSender(logger)
	scheduler := NewScheduler(logger)

	dispatcher := NewDispatcher(queueClient, registry, sender, scheduler, instanceID, logger)

	// Health endpoint
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/", health.Handler(nil))
		logger.Info("health server starting", "port", healthPort)
		http.ListenAndServe(fmt.Sprintf(":%s", healthPort), mux)
	}()

	logger.Info("webhook-dispatcher starting", "instance_id", instanceID)
	dispatcher.Run(ctx)
}

// RegistryClient fetches subscriber info from the registry service.
type RegistryClient struct {
	baseURL string
	client  *http.Client
}

func NewRegistryClient(baseURL string) *RegistryClient {
	return &RegistryClient{
		baseURL: baseURL,
		client:  &http.Client{},
	}
}

type subscriberInfo struct {
	ID         string `json:"id"`
	WebhookURL string `json:"webhook_url"`
	SigningKey  []byte `json:"signing_key"`
}

type subscriptionInfo struct {
	ID           string `json:"id"`
	SubscriberID string `json:"subscriber_id"`
	MaxBatchSize int    `json:"max_batch_size"`
	Active       bool   `json:"active"`
}

func (rc *RegistryClient) GetSubscriber(ctx context.Context, id string) (*subscriberInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/v1/subscribers/%s", rc.baseURL, id), nil)
	if err != nil {
		return nil, err
	}
	resp, err := rc.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("registry returned %d", resp.StatusCode)
	}
	var info subscriberInfo
	return &info, json.NewDecoder(resp.Body).Decode(&info)
}
