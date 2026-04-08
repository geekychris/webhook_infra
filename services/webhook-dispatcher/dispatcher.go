package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/example/event-gateway/internal/backoff"
	"github.com/example/event-gateway/internal/model"
	"github.com/example/event-gateway/internal/readyqueue"
	queuev1 "github.com/example/event-gateway/internal/proto/queue/v1"
)

// subMeta caches subscription + subscriber metadata.
type subMeta struct {
	SubscriberID string
	DeliveryMode string
	MaxBatchSize int
	WebhookURL   string
	SigningKey    []byte
}

// Dispatcher delivers webhook events using a blocking ready-queue pattern:
//
//  1. Queue-writer signals Redis when events are appended to a subscription.
//  2. Dispatcher blocks on Redis BZPOPMIN — wakes instantly when work arrives.
//  3. Delivers a batch of events to the subscriber's webhook endpoint.
//  4. If more events remain, re-signals the ready queue for another round.
//
// No polling. No busy-spinning. No RocksDB scanning.
// Scales to tens of thousands of subscriptions.
type Dispatcher struct {
	queueClient queuev1.QueueServiceClient
	registry    *RegistryClient
	sender      *Sender
	readyQ      *readyqueue.Queue
	instanceID  string
	logger      *slog.Logger
	backoff     backoff.Policy
	leaseTTL    int64

	metaMu    sync.RWMutex
	metaCache map[string]*subMeta

	backoffMu sync.Mutex
	backoffs  map[string]time.Time

	// lingerDuration is how long to wait after the first ready signal before
	// delivering. This lets multiple events accumulate in the queue so they
	// can be sent as a single HTTP batch. Default 50ms — tunable via env.
	lingerDuration time.Duration
}

func NewDispatcher(
	queueClient queuev1.QueueServiceClient,
	registry *RegistryClient,
	sender *Sender,
	readyQ *readyqueue.Queue,
	instanceID string,
	logger *slog.Logger,
) *Dispatcher {
	return &Dispatcher{
		queueClient: queueClient,
		registry:    registry,
		sender:      sender,
		readyQ:      readyQ,
		instanceID:  instanceID,
		logger:      logger,
		backoff:        backoff.DefaultPolicy(),
		leaseTTL:       30,
		metaCache:      make(map[string]*subMeta),
		backoffs:       make(map[string]time.Time),
		lingerDuration: 5 * time.Millisecond,
	}
}

// Run starts the dispatcher. It blocks on Redis waiting for ready subscriptions
// and delivers events as soon as they arrive. No polling loops.
func (d *Dispatcher) Run(ctx context.Context) {
	// Background: refresh cached metadata every 30s.
	go d.metaRefreshLoop(ctx)

	// Main delivery loop — blocks on Redis, wakes on work.
	for {
		select {
		case <-ctx.Done():
			d.logger.Info("dispatcher shutting down")
			return
		default:
		}

		// Block until a subscription is ready.
		subID, err := d.readyQ.Wait(ctx, 1*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			d.logger.Error("ready queue wait failed", "error", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if subID == "" {
			continue // timeout, no work
		}

		// Drain all other subscriptions that are already ready (no blocking).
		batch := []string{subID}
		drained, _ := d.readyQ.Drain(ctx, 100)
		batch = append(batch, drained...)

		// If we only got one subscription and it likely has just one event,
		// linger a few milliseconds to let a burst of events accumulate in
		// the queue. This is only useful for webhook batching — saves HTTP
		// round trips when a burst of events arrives in rapid succession.
		// If multiple subscriptions are already ready, skip the linger
		// entirely — there's plenty of work to do.
		if len(batch) == 1 && d.lingerDuration > 0 {
			time.Sleep(d.lingerDuration)
			// Check if more subscriptions became ready during the linger.
			extra, _ := d.readyQ.Drain(ctx, 100)
			batch = append(batch, extra...)
		}

		// Deduplicate (a subscription could appear in both Wait and Drain).
		seen := make(map[string]bool, len(batch))
		unique := batch[:0]
		for _, id := range batch {
			if !seen[id] {
				seen[id] = true
				unique = append(unique, id)
			}
		}

		// Deliver each subscription's pending events (fair round-robin).
		for _, id := range unique {
			if d.isBackingOff(id) {
				d.readyQ.Requeue(ctx, id)
				continue
			}
			d.deliverAndRequeue(ctx, id)
		}
	}
}

func (d *Dispatcher) deliverAndRequeue(ctx context.Context, subID string) {
	meta := d.getMeta(subID)
	if meta == nil {
		meta = d.fetchAndCacheMeta(ctx, subID)
	}
	if meta == nil {
		return // unknown subscription
	}
	if meta.DeliveryMode == "stream" {
		return // handled by stream-gateway
	}

	err := d.deliverBatch(ctx, subID, meta)
	if err != nil {
		failCount := d.recordFailure(subID)
		dur := d.backoff.Duration(failCount)
		d.setBackoff(subID, dur)
		d.logger.Error("delivery failed",
			"subscription_id", subID,
			"error", err,
			"backoff", dur,
		)
		// Requeue so it gets retried after backoff expires.
		d.readyQ.Requeue(ctx, subID)
		return
	}

	d.clearBackoff(subID)

	// Check if more events remain — if so, requeue immediately.
	stateResp, err := d.queueClient.GetState(ctx, &queuev1.GetStateRequest{
		SubscriptionId: subID,
	})
	if err == nil {
		st := stateResp.GetState()
		if st.GetTailSeq() > st.GetAckedSeq() {
			d.readyQ.Requeue(ctx, subID)
		}
	}
}

func (d *Dispatcher) deliverBatch(ctx context.Context, subID string, meta *subMeta) error {
	leaseResp, err := d.queueClient.AcquireLease(ctx, &queuev1.AcquireLeaseRequest{
		SubscriptionId: subID,
		HolderId:       d.instanceID,
		TtlSeconds:     d.leaseTTL,
	})
	if err != nil {
		return err
	}
	epoch := leaseResp.Epoch

	stateResp, err := d.queueClient.GetState(ctx, &queuev1.GetStateRequest{
		SubscriptionId: subID,
	})
	if err != nil {
		return err
	}

	ackedSeq := stateResp.GetState().GetAckedSeq()
	batchSize := int32(meta.MaxBatchSize)
	if batchSize <= 0 {
		batchSize = 100
	}

	readResp, err := d.queueClient.Read(ctx, &queuev1.ReadRequest{
		SubscriptionId: subID,
		AfterPosition:  ackedSeq,
		MaxCount:       batchSize,
		LeaseEpoch:     epoch,
	})
	if err != nil {
		return err
	}
	if len(readResp.Entries) == 0 {
		return nil
	}

	events := make([]model.CloudEvent, 0, len(readResp.Entries))
	var lastPos uint64
	for _, e := range readResp.Entries {
		ev := e.GetEvent()
		if ev == nil {
			continue
		}
		events = append(events, protoToCloudEvent(ev))
		lastPos = e.Position
	}
	if len(events) == 0 {
		return nil
	}

	if meta.WebhookURL == "" {
		d.logger.Warn("no webhook URL", "subscription_id", subID)
		return nil
	}

	if err := d.sender.SendBatch(ctx, meta.WebhookURL, meta.SigningKey, events); err != nil {
		return err
	}

	if _, err := d.queueClient.Ack(ctx, &queuev1.AckRequest{
		SubscriptionId: subID,
		Position:       lastPos,
		LeaseEpoch:     epoch,
	}); err != nil {
		return err
	}

	d.logger.Info("batch delivered",
		"subscription_id", subID,
		"events", len(events),
		"last_position", lastPos,
	)
	return nil
}

// --- Backoff tracking ---

func (d *Dispatcher) isBackingOff(subID string) bool {
	d.backoffMu.Lock()
	defer d.backoffMu.Unlock()
	until, ok := d.backoffs[subID]
	return ok && time.Now().Before(until)
}

func (d *Dispatcher) setBackoff(subID string, dur time.Duration) {
	d.backoffMu.Lock()
	d.backoffs[subID] = time.Now().Add(dur)
	d.backoffMu.Unlock()
}

func (d *Dispatcher) clearBackoff(subID string) {
	d.backoffMu.Lock()
	delete(d.backoffs, subID)
	d.backoffMu.Unlock()
}

// failCount increments and returns the consecutive failure count.
var failCounts sync.Map

func (d *Dispatcher) recordFailure(subID string) int {
	val, _ := failCounts.LoadOrStore(subID, new(int))
	p := val.(*int)
	*p++
	return *p
}

// --- Metadata cache ---

func (d *Dispatcher) getMeta(subID string) *subMeta {
	d.metaMu.RLock()
	defer d.metaMu.RUnlock()
	return d.metaCache[subID]
}

func (d *Dispatcher) fetchAndCacheMeta(ctx context.Context, subID string) *subMeta {
	subInfo, err := d.registry.GetSubscription(ctx, subID)
	if err != nil || subInfo == nil {
		return nil
	}

	meta := &subMeta{
		SubscriberID: subInfo.SubscriberID,
		DeliveryMode: subInfo.DeliveryMode,
		MaxBatchSize: subInfo.MaxBatchSize,
	}
	if meta.MaxBatchSize <= 0 {
		meta.MaxBatchSize = 100
	}

	sub, err := d.registry.GetSubscriber(ctx, subInfo.SubscriberID)
	if err == nil && sub != nil {
		meta.WebhookURL = sub.WebhookURL
		meta.SigningKey = sub.SigningKey
	}

	d.metaMu.Lock()
	d.metaCache[subID] = meta
	d.metaMu.Unlock()

	return meta
}

func (d *Dispatcher) metaRefreshLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.metaMu.RLock()
			ids := make([]string, 0, len(d.metaCache))
			for id := range d.metaCache {
				ids = append(ids, id)
			}
			d.metaMu.RUnlock()
			for _, id := range ids {
				d.fetchAndCacheMeta(ctx, id)
			}
		}
	}
}

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
