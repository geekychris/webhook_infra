package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/example/event-gateway/internal/backoff"
	"github.com/example/event-gateway/internal/model"
	queuev1 "github.com/example/event-gateway/internal/proto/queue/v1"
)

// Dispatcher orchestrates webhook delivery for all owned subscriptions.
type Dispatcher struct {
	queueClient queuev1.QueueServiceClient
	registry    *RegistryClient
	sender      *Sender
	scheduler   *Scheduler
	instanceID  string
	logger      *slog.Logger
	backoff     backoff.Policy
	leaseTTL    int64
}

// NewDispatcher creates a webhook dispatcher.
func NewDispatcher(
	queueClient queuev1.QueueServiceClient,
	registry *RegistryClient,
	sender *Sender,
	scheduler *Scheduler,
	instanceID string,
	logger *slog.Logger,
) *Dispatcher {
	return &Dispatcher{
		queueClient: queueClient,
		registry:    registry,
		sender:      sender,
		scheduler:   scheduler,
		instanceID:  instanceID,
		logger:      logger,
		backoff:     backoff.DefaultPolicy(),
		leaseTTL:    30,
	}
}

// Run starts the dispatch loop. Blocks until ctx is cancelled.
func (d *Dispatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	refreshTicker := time.NewTicker(5 * time.Second)
	defer refreshTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.logger.Info("dispatcher shutting down")
			return

		case <-refreshTicker.C:
			d.refreshReadySubscriptions(ctx)

		case <-ticker.C:
			d.dispatchOnce(ctx)
		}
	}
}

func (d *Dispatcher) refreshReadySubscriptions(ctx context.Context) {
	resp, err := d.queueClient.ListReady(ctx, &queuev1.ListReadyRequest{Limit: 100})
	if err != nil {
		d.logger.Error("failed to list ready subscriptions", "error", err)
		return
	}

	entries := make([]*ScheduleEntry, 0, len(resp.SubscriptionIds))
	for _, subID := range resp.SubscriptionIds {
		entries = append(entries, &ScheduleEntry{
			SubscriptionID: subID,
			MaxBatchSize:   100, // default
		})
	}

	d.scheduler.UpdateEntries(entries)
}

func (d *Dispatcher) dispatchOnce(ctx context.Context) {
	entry := d.scheduler.Next()
	if entry == nil {
		return
	}

	if err := d.deliverSubscription(ctx, entry); err != nil {
		dur := d.backoff.Duration(entry.FailCount)
		d.scheduler.RecordFailure(entry.SubscriptionID, dur)
		d.logger.Error("delivery failed",
			"subscription_id", entry.SubscriptionID,
			"error", err,
			"backoff", dur,
		)
	} else {
		d.scheduler.RecordSuccess(entry.SubscriptionID)
	}
}

func (d *Dispatcher) deliverSubscription(ctx context.Context, entry *ScheduleEntry) error {
	// Acquire or renew lease
	leaseResp, err := d.queueClient.AcquireLease(ctx, &queuev1.AcquireLeaseRequest{
		SubscriptionId: entry.SubscriptionID,
		HolderId:       d.instanceID,
		TtlSeconds:     d.leaseTTL,
	})
	if err != nil {
		return err
	}

	epoch := leaseResp.Epoch

	// Get subscription state to know where to read from
	stateResp, err := d.queueClient.GetState(ctx, &queuev1.GetStateRequest{
		SubscriptionId: entry.SubscriptionID,
	})
	if err != nil {
		return err
	}

	ackedSeq := stateResp.GetState().GetAckedSeq()
	batchSize := int32(entry.MaxBatchSize)
	if batchSize <= 0 {
		batchSize = 100
	}

	// Read events from queue
	readResp, err := d.queueClient.Read(ctx, &queuev1.ReadRequest{
		SubscriptionId: entry.SubscriptionID,
		AfterPosition:  ackedSeq,
		MaxCount:        batchSize,
		LeaseEpoch:     epoch,
	})
	if err != nil {
		return err
	}

	if len(readResp.Entries) == 0 {
		return nil
	}

	// Convert proto CloudEvents to model CloudEvents
	events := make([]model.CloudEvent, 0, len(readResp.Entries))
	var lastPos uint64
	for _, e := range readResp.Entries {
		ev := e.GetEvent()
		if ev == nil {
			continue
		}
		ce := protoToCloudEvent(ev)
		events = append(events, ce)
		lastPos = e.Position
	}

	if len(events) == 0 {
		return nil
	}

	// Get subscriber info for webhook URL and signing key
	// For now, get it from the queue state or use cached info
	webhookURL := entry.WebhookURL
	signingKey := entry.SigningKey

	if webhookURL == "" {
		// Try to get from registry
		sub, err := d.registry.GetSubscriber(ctx, entry.SubscriberID)
		if err != nil {
			d.logger.Warn("could not get subscriber info from registry, using defaults",
				"subscriber_id", entry.SubscriberID,
				"error", err,
			)
		} else {
			webhookURL = sub.WebhookURL
			signingKey = sub.SigningKey
			entry.WebhookURL = webhookURL
			entry.SigningKey = signingKey
		}
	}

	if webhookURL == "" {
		d.logger.Warn("no webhook URL for subscription, skipping", "subscription_id", entry.SubscriptionID)
		return nil
	}

	// Send batch
	if err := d.sender.SendBatch(ctx, webhookURL, signingKey, events); err != nil {
		return err
	}

	// Ack on success
	_, err = d.queueClient.Ack(ctx, &queuev1.AckRequest{
		SubscriptionId: entry.SubscriptionID,
		Position:       lastPos,
		LeaseEpoch:     epoch,
	})
	if err != nil {
		d.logger.Error("ack failed after successful delivery",
			"subscription_id", entry.SubscriptionID,
			"position", lastPos,
			"error", err,
		)
		return err
	}

	d.logger.Info("batch delivered",
		"subscription_id", entry.SubscriptionID,
		"events", len(events),
		"last_position", lastPos,
	)
	return nil
}

// protoToCloudEvent converts a protobuf CloudEventProto to a model CloudEvent.
func protoToCloudEvent(p *queuev1.CloudEventProto) model.CloudEvent {
	return model.CloudEvent{
		SpecVersion:     p.GetSpecVersion(),
		ID:              p.GetId(),
		Type:            p.GetType(),
		Source:          p.GetSource(),
		DataContentType: p.GetDataContentType(),
		Time:            time.Unix(0, p.GetTimeUnixNanos()),
		Subject:         p.GetSubject(),
		DataSchema:      p.GetDataSchema(),
		Data:            p.GetData(),
	}
}
