# Enterprise Outbound Event Gateway — Architecture & Operations Guide

## 1. Overview

The Event Gateway is an enterprise-grade outbound event delivery platform. It consumes rich internal **ECST events** from Apache Kafka, determines which third-party **subscribers** should receive each event, translates matching events into the **CloudEvents v1.0** format, and delivers them via:

- **Webhook batches** over HTTPS (with HMAC-SHA256 signatures)
- **Real-time streaming** over secure WebSockets

### Core Guarantees

| Property | Guarantee |
|----------|-----------|
| Delivery | **At-least-once** — duplicates are possible after crashes/restarts |
| Ordering | **Strict per-subscription** — events are delivered in queue order within each subscription |
| Durability | **RocksDB-backed** — queues survive pod restarts; configurable TTL (default 30 days) |
| Isolation | One slow/failing subscriber does not block others |
| Scalability | Horizontal — add pods for ingestion, queue writing, dispatching, streaming |

### Module

```
github.com/example/event-gateway
```

---

## 2. Architecture

### 2.1 System Diagram

```
┌─────────────────┐
│ Upstream Services│
└────────┬────────┘
         │ ECST events
         ▼
┌─────────────────┐     ┌────────────────────┐
│  Kafka           │     │  PostgreSQL          │
│  ecst-events     │     │  subscribers         │
│  sendable-events │     │  subscriptions       │
│  entity-mappings │     │  entity_mappings     │
└────────┬────────┘     └──────────┬───────────┘
         │                         │
         │              ┌──────────▼───────────┐
         │              │  subscriber-registry   │ ◄── REST API (port 8081)
         │              │  (CRUD + migrations)   │
         │              └──────────┬───────────┘
         │                   PG NOTIFY │
         │              ┌──────────▼───────────┐
         │              │  config-distributor    │ ◄── gRPC stream (port 50051)
         │              └──────────┬───────────┘
         │                  gRPC   │ stream
         ▼                         ▼
┌─────────────────────────────────────────────┐
│              ingestion-router                │
│  Kafka consumer (ecst-events)                │
│  In-memory Matcher (type/source/tenant)      │
│  Entity Matcher (send_to/user_id/company_id) │
│  Redis/Valkey resolver for entity lookups    │
│  CloudEvent Mapper                           │
│  Kafka producer (sendable-events)            │
└────────────────────┬────────────────────────┘
                     │ sendable-events
                     ▼
┌─────────────────────────────────────────────┐
│              queue-writer                    │
│  Kafka consumer → gRPC Append               │
└────────────────────┬────────────────────────┘
                     │ gRPC
                     ▼
┌─────────────────────────────────────────────┐
│       subscriber-queue-service               │
│  RocksDB-backed per-subscription queues      │
│  Delivery lease manager (epoch fencing)      │
│  Background compaction (cursor + TTL)        │
│  gRPC API (port 50052)                       │
└──────────┬──────────────────┬───────────────┘
           │                  │
    gRPC   │                  │ gRPC
           ▼                  ▼
┌──────────────────┐  ┌──────────────────────┐
│ webhook-dispatcher│  │   stream-gateway      │
│ Fair scheduler    │  │   WebSocket server    │
│ HMAC signing      │  │   Resume/replay       │
│ Retry + backoff   │  │   ACK protocol        │
└────────┬─────────┘  └──────────┬───────────┘
         │                       │
         ▼                       ▼
┌──────────────────┐  ┌──────────────────────┐
│  Webhook          │  │  Streaming            │
│  Subscribers      │  │  Subscribers          │
│  (HTTPS POST)     │  │  (WSS)               │
└──────────────────┘  └──────────────────────┘

┌─────────────────────────────────────────────┐
│              admin-api                       │
│  Dashboard UI + REST (port 8085)             │
│  Pause/resume, replay, stats                 │
└─────────────────────────────────────────────┘

┌──────────────┐
│   Valkey      │ ◄── Redis-compatible cache
│  (port 6379)  │     Entity mappings for fast lookup
└──────────────┘
```

### 2.2 Service Inventory

| Service | Internal Port | External Port | Protocol | Role |
|---------|--------------|---------------|----------|------|
| subscriber-registry | 8081 | 18081 | HTTP/REST | CRUD for subscribers, subscriptions, entity mappings |
| config-distributor | 50051 | — | gRPC streaming | Streams subscription config snapshots/deltas |
| ingestion-router | — | — | Kafka + gRPC | Matches events, maps to CloudEvents, publishes sendable-events |
| queue-writer | — | — | Kafka + gRPC | Writes sendable-events to per-subscription queues |
| subscriber-queue-service | 50052 | — | gRPC | RocksDB-backed durable queues with leases and TTL |
| webhook-dispatcher | — | — | gRPC + HTTP client | Delivers CloudEvents batches to webhook endpoints |
| stream-gateway | 8090 | 18090 | WebSocket | Real-time streaming delivery with resume |
| admin-api | 8085 | 18085 | HTTP/REST | Admin dashboard, pause/resume/replay, stats |
| valkey | 6379 | — | Redis protocol | Entity mapping cache for fast subscriber lookup |
| postgres | 5432 | — | PostgreSQL | Persistent storage for subscribers, subscriptions, entity mappings |
| kafka | 19092 | — | Kafka | Event transport (ecst-events, sendable-events, entity-mappings) |

### 2.3 Kafka Topics

| Topic | Key | Value | Purpose |
|-------|-----|-------|---------|
| `ecst-events` | tenant_id | ECSTEvent JSON | Raw platform events from upstream services |
| `sendable-events` | subscription_id | SendableEvent JSON | Matched events targeted at specific subscriptions |
| `entity-mappings` | entity_type:entity_id | EntityMappingEvent JSON | Entity-to-subscriber mapping changes |

---

## 3. Event Flow

### 3.1 ECST Event Format

Events from upstream services arrive on the `ecst-events` Kafka topic:

```json
{
  "event_id": "evt_100001",
  "event_type": "invoice.paid",
  "source_service": "billing-service",
  "tenant_id": "tenant_42",
  "entity_id": "invoice_9001",
  "time": "2026-04-07T18:15:00Z",
  "version": "1",
  "payload": {
    "invoice_id": "invoice_9001",
    "customer_id": "cust_100",
    "amount": 125.00,
    "currency": "USD",
    "status": "paid",
    "user_id": "u_5678",
    "company_id": "c_acme"
  },
  "metadata": {
    "user_id": "u_5678",
    "trace_id": "abc123"
  }
}
```

### 3.2 CloudEvent Output

After mapping, the subscriber receives:

```json
{
  "specversion": "1.0",
  "id": "evt_100001",
  "source": "billing-service",
  "type": "invoice.paid",
  "time": "2026-04-07T18:15:00Z",
  "subject": "invoice_9001",
  "datacontenttype": "application/json",
  "data": {
    "invoice_id": "invoice_9001",
    "customer_id": "cust_100",
    "amount": 125.00,
    "currency": "USD",
    "status": "paid",
    "user_id": "u_5678",
    "company_id": "c_acme"
  }
}
```

### 3.3 Pipeline Stages

```
 1. ECST event arrives on Kafka
 2. Ingestion router consumes and deserializes
 3. Subscription matching (multi-dimensional — see Section 6)
 4. ECST → CloudEvent mapping (with optional per-type hooks — see Section 6.6)
 5. SendableEvent published to sendable-events (keyed by subscription_id)
 6. Queue writer appends to RocksDB-backed per-subscription durable queue
 7. Webhook dispatcher or stream gateway delivers to subscriber
 8. On 2xx ACK: queue cursor advances
 9. On failure: exponential backoff, retry later
10. Background compaction removes: acked entries + entries past TTL (default 30 days)
```

---

## 4. Building and Running

### 4.1 Prerequisites

- Go 1.23+
- Docker and Docker Compose
- RocksDB 10.x with development headers (for the queue service — `brew install rocksdb` on macOS)
- `protoc` with Go plugins (for proto regeneration only)

### 4.2 Build

```bash
# Build all service binaries (most are pure Go, queue-service requires CGO)
make build

# Build the queue service locally (CGO_ENABLED=1 required for RocksDB)
export CGO_CFLAGS="$(pkg-config --cflags rocksdb)"
export CGO_LDFLAGS="$(pkg-config --libs rocksdb) -L/opt/homebrew/lib -lzstd -llz4 -lz -lsnappy -lbz2"
CGO_ENABLED=1 go build ./services/subscriber-queue-service/...

# Run unit tests
make test

# Run queue service tests (requires CGO)
CGO_ENABLED=1 go test ./services/subscriber-queue-service/... -v

# Regenerate protobuf (only if .proto files change)
make proto
```

### 4.3 Run with Docker Compose

```bash
# Start the full platform (8 services + Kafka + PostgreSQL + Valkey)
make compose-up

# Also start synthetic test tools (event generator + test subscribers)
make compose-synthetic

# View logs
docker compose -f deploy/docker-compose/docker-compose.yml logs -f

# Shut down and clean up
make compose-down
```

### 4.4 Service Ports (Docker Compose)

| Service | URL |
|---------|-----|
| Admin UI & API | http://localhost:18085/ |
| Subscriber Registry API | http://localhost:18081/v1/ |
| Stream Gateway (WebSocket) | ws://localhost:18090/v1/stream |
| Synthetic Webhook Subscriber | http://localhost:19090/ |

### 4.5 Local Development

Run individual services locally with environment variables:

```bash
# Start infrastructure
docker compose -f deploy/docker-compose/docker-compose.yml up -d postgres kafka valkey

# Run a service
DATABASE_URL="postgres://egw:egw@localhost:5432/eventgateway?sslmode=disable" \
HTTP_PORT=8081 \
go run ./services/subscriber-registry
```

Key environment variables per service:

| Variable | Used By | Default |
|----------|---------|---------|
| `DATABASE_URL` | registry, config-distributor, admin-api | — (required) |
| `HTTP_PORT` | registry, admin-api, stream-gateway | 8080/8085/8090 |
| `GRPC_PORT` | config-distributor, queue-service | 50051/50052 |
| `KAFKA_BROKERS` | ingestion-router, queue-writer, event-generator, registry | localhost:9092 |
| `CONFIG_DISTRIBUTOR_ADDR` | ingestion-router | localhost:50051 |
| `QUEUE_SERVICE_ADDR` | queue-writer, dispatcher, stream-gateway, admin-api | localhost:50052 |
| `REGISTRY_URL` | dispatcher, stream-gateway, ingestion-router | http://localhost:8081 |
| `REDIS_ADDR` | ingestion-router | localhost:6379 |
| `CACHE_SYNC_INTERVAL` | ingestion-router | 5m |
| `INSTANCE_ID` | dispatcher, stream-gateway | auto-generated UUID |
| `DATA_DIR` | queue-service | /data/queue |
| `STORE_BACKEND` | queue-service | rocksdb |
| `QUEUE_TTL` | queue-service | 720h (30 days) |

---

## 5. Administration Guide

### 5.1 Managing Subscribers

Subscribers represent external organizations that receive events.

**Create a subscriber:**

```bash
curl -X POST http://localhost:18081/v1/subscribers \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "Acme Corp",
    "webhook_url": "https://acme.example.com/webhooks/events"
  }'
```

Response includes an auto-generated `signing_key` (32-byte random, used for HMAC signatures).

**List subscribers:**

```bash
curl http://localhost:18081/v1/subscribers?offset=0&limit=50
```

**Update / Delete:**

```bash
curl -X PUT http://localhost:18081/v1/subscribers/{id} \
  -H 'Content-Type: application/json' \
  -d '{"name": "Acme Corp Updated", "webhook_url": "https://new-url.example.com/events"}'

curl -X DELETE http://localhost:18081/v1/subscribers/{id}
```

### 5.2 Managing Subscriptions

Subscriptions define what events a subscriber receives and how.

**Create a webhook subscription:**

```bash
curl -X POST http://localhost:18081/v1/subscribers/{subscriber_id}/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{
    "event_types": ["invoice.created", "invoice.paid", "payment.failed"],
    "source_filter": "billing-*",
    "tenant_filter": ["tenant_42"],
    "delivery_mode": "webhook",
    "max_batch_size": 50
  }'
```

**Create a streaming subscription:**

```bash
curl -X POST http://localhost:18081/v1/subscribers/{subscriber_id}/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{
    "event_types": ["order.created", "order.updated"],
    "source_filter": "*",
    "delivery_mode": "stream"
  }'
```

**Subscription fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `event_types` | string[] | required | Event types to match (exact match) |
| `source_filter` | string | `*` | Glob pattern for source service (`*` = all) |
| `tenant_filter` | string[] | `[]` | Tenant IDs to match (empty = all) |
| `delivery_mode` | string | `webhook` | `webhook` or `stream` |
| `max_batch_size` | int | 100 | Max events per webhook batch |
| `max_batch_bytes` | int | 1048576 | Max bytes per batch (1 MB) |
| `linger_ms` | int | 100 | Wait time before sending partial batch |
| `active` | bool | true | Enable/disable the subscription |

### 5.3 Admin API

The admin API at port 18085 provides operational controls:

```bash
# View all subscriptions with status
curl http://localhost:18085/v1/admin/subscriptions

# Get queue stats for a subscription
curl http://localhost:18085/v1/admin/subscriptions/{id}/stats
# → {"tail_seq": 1200, "acked_seq": 1195, "pending_count": 5, "lease_epoch": 42}

# Pause delivery
curl -X POST http://localhost:18085/v1/admin/subscriptions/{id}/pause

# Resume delivery
curl -X POST http://localhost:18085/v1/admin/subscriptions/{id}/resume

# Replay from a specific position
curl -X POST "http://localhost:18085/v1/admin/subscriptions/{id}/replay?from_position=1000"

# System health check
curl http://localhost:18085/v1/admin/health
# → {"database": true, "queue_service": true}
```

### 5.4 Admin Dashboard

Open http://localhost:18085/ in a browser for a web-based admin dashboard with:

- **Dashboard** — summary cards (subscribers, subscriptions, health), auto-refreshes
- **Subscribers** — table with create/edit functionality
- **Subscriptions** — table with pause/resume controls
- **Queue Stats** — real-time per-subscription delivery metrics with replay controls

---

## 6. Subscription Matching — How Events Reach Subscribers

The ingestion router determines which subscribers receive each event using a multi-layered matching system. Matching is designed to be **fast** (in-memory indexes + Redis) and **extensible** (new matching dimensions can be added).

### 6.1 Standard Matching (In-Memory)

Standard matching uses three dimensions evaluated in order:

```
Event Type (indexed) → Source Filter (glob) → Tenant Filter (membership)
```

**Event Type** — Primary index. Subscriptions are stored in a `map[string][]*Subscription` keyed by each event type they subscribe to. Lookup is O(1).

**Source Filter** — Glob pattern matching against `event.Source` using Go's `filepath.Match`. The pattern `*` matches all sources. Example: `billing-*` matches `billing-service`, `billing-worker`.

**Tenant Filter** — Membership check. If the subscription's `tenant_filter` is empty, all tenants match. Otherwise, `event.TenantID` must be in the list.

**Active Status** — Only subscriptions with `active = true` are considered.

### 6.2 Entity-Based Routing (Redis-Backed)

Beyond standard matching, events can be routed based on entity relationships:

#### 6.2.1 Direct Routing (`send_to`)

If an event's payload contains a `send_to` field, the event is routed directly to the named subscriber(s):

```json
{
  "event_type": "notification.custom",
  "payload": {
    "send_to": "subscriber-uuid-here",
    "message": "Hello"
  }
}
```

The `send_to` field accepts a single subscriber ID (string) or an array of subscriber IDs.

#### 6.2.2 User/Company ID Routing

Events with `user_id` or `company_id` are routed to all subscribers associated with that entity via the **entity mapping** system:

```json
{
  "event_type": "order.created",
  "metadata": {"user_id": "u_5678"},
  "payload": {
    "company_id": "c_acme",
    "amount": 100
  }
}
```

The router checks `event.Metadata["user_id"]` and `event.Metadata["company_id"]` first, then falls back to parsing the same fields from `event.Payload`.

**Lookup Flow:**

```
user_id "u_5678"
    → Redis SMEMBERS entity:user:u_5678
    → [subscriber_A, subscriber_B]
    → Find active subscriptions for each subscriber
    → Add to match results

company_id "c_acme"
    → Redis SMEMBERS entity:company:c_acme
    → [subscriber_A, subscriber_C]
    → Find active subscriptions for each subscriber
    → Add to match results
```

#### 6.2.3 Result Deduplication

All matching dimensions are combined (union). If a subscriber is matched by both event type AND entity routing, they receive the event only once. Deduplication is by subscription ID.

### 6.3 Managing Entity Mappings

Entity mappings associate users/companies with subscribers. Multiple subscribers can be mapped to the same entity (broadcasting).

**REST API** (on subscriber-registry, port 18081):

```bash
# Map user u_5678 to subscriber
curl -X POST http://localhost:18081/v1/entity-mappings \
  -H 'Content-Type: application/json' \
  -d '{
    "entity_type": "user",
    "entity_id": "u_5678",
    "subscriber_id": "subscriber-uuid"
  }'

# Map company c_acme to multiple subscribers (one call each)
curl -X POST http://localhost:18081/v1/entity-mappings \
  -d '{"entity_type": "company", "entity_id": "c_acme", "subscriber_id": "sub-uuid-1"}'
curl -X POST http://localhost:18081/v1/entity-mappings \
  -d '{"entity_type": "company", "entity_id": "c_acme", "subscriber_id": "sub-uuid-2"}'

# List subscribers for an entity
curl "http://localhost:18081/v1/entity-mappings?entity_type=user&entity_id=u_5678"

# List entity mappings for a subscriber
curl http://localhost:18081/v1/subscribers/{id}/entity-mappings

# Remove a mapping
curl -X DELETE http://localhost:18081/v1/entity-mappings \
  -d '{"entity_type": "user", "entity_id": "u_5678", "subscriber_id": "subscriber-uuid"}'
```

**Kafka Sync Channel:**

Every mapping change is also published to the `entity-mappings` Kafka topic:

```json
{"action": "add", "entity_type": "user", "entity_id": "u_5678", "subscriber_id": "sub-uuid"}
{"action": "remove", "entity_type": "company", "entity_id": "c_acme", "subscriber_id": "sub-uuid"}
```

The ingestion-router consumes this topic to keep its Redis cache synchronized in real-time. This provides **eventual consistency** — mapping changes propagate to the routing layer within seconds.

### 6.4 Redis Cache Architecture

Entity mappings are cached in **Valkey** (Redis-compatible) for sub-millisecond lookups:

```
Key: entity:user:u_5678       → SET { "sub-uuid-1", "sub-uuid-2" }
Key: entity:company:c_acme    → SET { "sub-uuid-1", "sub-uuid-3" }
```

The cache is kept in sync through **three layers**:

**Layer 1 — Startup warm (authoritative, immediate):**
On boot, the ingestion-router fetches all mappings from the registry's `GET /v1/entity-mappings/all` endpoint and does a `FlushAndReload` into Redis — deletes all `entity:*` keys and repopulates from the database. This gives the router a complete, accurate cache from second one.

**Layer 2 — Real-time Kafka sync (incremental, continuous):**
When the REST API creates or deletes a mapping, an `EntityMappingEvent` is published to the `entity-mappings` Kafka topic. The ingestion-router's syncer consumer reads these messages and calls `SADD`/`SREM` on the specific Redis key. The Kafka consumer uses `StartOffset: FirstOffset` so a brand-new consumer group replays the full topic history.

**Layer 3 — Periodic full reconciliation (healing, configurable):**
A background goroutine runs `FlushAndReload` on a configurable interval (`CACHE_SYNC_INTERVAL`, default 5 minutes). It scans all `entity:*` keys, deletes them, fetches the full mapping set from the database, and repopulates Redis. This heals any drift caused by missed Kafka messages, direct DB edits, or Redis eviction.

| Event | DB | Kafka | Redis |
|-------|-----|-------|-------|
| REST `POST /v1/entity-mappings` | INSERT | publishes "add" | SADD (via Kafka syncer) |
| REST `DELETE /v1/entity-mappings` | DELETE | publishes "remove" | SREM (via Kafka syncer) |
| Direct DB edit | changed | no message | healed by periodic sync |
| Redis restart | untouched | untouched | rebuilt on next router boot or periodic sync |

### 6.5 Performance Characteristics

| Operation | Complexity | Latency |
|-----------|-----------|---------|
| Event type lookup | O(1) hash map | < 1 μs |
| Source glob filter | O(n) per type-matched set | < 10 μs |
| Tenant filter | O(1) set membership | < 1 μs |
| Entity Redis lookup | O(1) SMEMBERS | < 1 ms |
| Subscriber→subscription index | O(1) hash map | < 1 μs |

### 6.6 Custom Mapping Functions (Per Event Type)

The `Mapper` supports registering custom mapping functions per event type. These run after the base ECST→CloudEvent conversion and can transform, filter, or enrich the outbound payload.

```go
// In your ingestion-router setup:
mapper := mapper.New()

// Strip internal fields from invoice events before sending to subscribers
mapper.Register("invoice.paid", func(event model.ECSTEvent, ce model.CloudEvent) (model.CloudEvent, error) {
    var p map[string]interface{}
    json.Unmarshal(event.Payload, &p)
    delete(p, "internal_code")
    delete(p, "audit_trail")
    ce.Data, _ = json.Marshal(p)
    return ce, nil
})

// Add a schema URL to all events (wildcard "*" runs for every type)
mapper.Register("*", func(event model.ECSTEvent, ce model.CloudEvent) (model.CloudEvent, error) {
    ce.DataSchema = "https://schemas.example.com/" + event.Type
    return ce, nil
})

// Reject events that fail a policy check (return error to skip)
mapper.Register("pii.export", func(event model.ECSTEvent, ce model.CloudEvent) (model.CloudEvent, error) {
    return ce, errors.New("PII export events are not externalized")
})
```

**Execution order:** type-specific hook runs first, then wildcard hook. If either returns an error, the event is skipped.

### 6.7 Custom Entity Extraction (Per Event Type)

The `EntityExtractor` determines which entity fields to extract from an event for routing lookups. The default extractor handles `send_to`, `user_id`, and `company_id`. Register custom extractors when an event type stores entity IDs in non-standard fields.

```go
// In your ingestion-router setup:
entityMatcher := matcher.NewEntityMatcher(m, resolver, logger)
extractor := entityMatcher.Extractor()

// task.assigned stores user ID in "owner_id" and has a "team_id" concept
extractor.Register("task.assigned", func(event model.ECSTEvent, payload map[string]interface{}) ([]matcher.EntityRef, []model.SubscriberID) {
    var refs []matcher.EntityRef
    if ownerID, ok := payload["owner_id"].(string); ok {
        refs = append(refs, matcher.EntityRef{Type: "user", ID: ownerID})
    }
    if teamID, ok := payload["team_id"].(string); ok {
        refs = append(refs, matcher.EntityRef{Type: "team", ID: teamID})
    }
    return refs, nil
})

// payment.received — extend the defaults with a custom "merchant" entity
extractor.Register("payment.received", func(event model.ECSTEvent, payload map[string]interface{}) ([]matcher.EntityRef, []model.SubscriberID) {
    // Get the default user_id/company_id/send_to extraction
    refs, direct := matcher.DefaultExtract(event, payload)
    // Add merchant routing on top
    if merchantID, ok := payload["merchant_id"].(string); ok {
        refs = append(refs, matcher.EntityRef{Type: "merchant", ID: merchantID})
    }
    return refs, direct
})
```

**Key points:**
- A custom extractor **replaces** the default for that event type
- Call `matcher.DefaultExtract(event, payload)` inside your custom function to get the defaults and add to them
- Event types without a registered extractor use the default (`user_id`, `company_id`, `send_to`)
- The `EntityRef.Type` field can be any string — use it to define custom entity types like `"team"`, `"merchant"`, `"department"`, as long as corresponding entity mappings exist in Redis

---

## 7. Implementing a Webhook Receiver

### 7.1 Request Format

The dispatcher sends HTTP POST requests with:

```
POST /your/webhook/endpoint
Content-Type: application/cloudevents-batch+json
X-Webhook-Signature: sha256=<hex-encoded-hmac>
X-Webhook-Timestamp: <unix-seconds>
```

The body is a JSON array of CloudEvents:

```json
[
  {"specversion":"1.0","id":"evt1","type":"invoice.paid","source":"billing","time":"...","data":{...}},
  {"specversion":"1.0","id":"evt2","type":"invoice.created","source":"billing","time":"...","data":{...}}
]
```

### 7.2 HMAC Signature Verification

The signature covers `<timestamp>.<body>` using HMAC-SHA256 with the subscriber's signing key:

```python
# Python example
import hmac, hashlib

def verify_webhook(signing_key: bytes, timestamp: str, body: bytes, signature: str) -> bool:
    expected = hmac.new(signing_key, f"{timestamp}.".encode() + body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(f"sha256={expected}", signature)
```

```go
// Go example
func verifyWebhook(signingKey []byte, timestamp string, body []byte, signature string) bool {
    mac := hmac.New(sha256.New, signingKey)
    mac.Write([]byte(timestamp + "."))
    mac.Write(body)
    expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
    return hmac.Equal([]byte(expected), []byte(signature))
}
```

### 7.3 Response Contract

| Status | Meaning |
|--------|---------|
| 2xx | Success — events are acknowledged, cursor advances |
| 4xx | Client error — delivery retried with backoff |
| 5xx | Server error — delivery retried with backoff |
| Timeout (30s) | Treated as failure — delivery retried |

### 7.4 Retry Behavior

On failure, the dispatcher applies exponential backoff with jitter:

```
Attempt 1: ~1s
Attempt 2: ~2s
Attempt 3: ~4s
Attempt 4: ~8s
...capped at 5 minutes
```

### 7.5 Idempotency

Subscribers **must** handle duplicate events. Deduplicate using:
- CloudEvent `id` field
- Queue position (if exposed in headers)
- Source event `entity_id` + `type` combination

---

## 8. Implementing a Streaming Consumer

### 8.1 Connection

Connect via WebSocket:

```
ws://host:18090/v1/stream?subscription_id=<id>&token=<auth>&resume_after=<position>
```

| Parameter | Required | Description |
|-----------|----------|-------------|
| `subscription_id` | Yes | UUID of the streaming subscription |
| `token` | Yes | Auth token for the subscriber |
| `resume_after` | No | Last received position (0 = start from beginning) |

### 8.2 Protocol

**Server → Client (events):**

```json
{"type": "event", "position": 42, "event": {"specversion": "1.0", "id": "evt1", ...}}
```

**Client → Server (acknowledgment):**

```json
{"type": "ack", "position": 42}
```

### 8.3 Example Client (Go)

```go
conn, _, err := websocket.Dial(ctx, url, nil)
if err != nil { log.Fatal(err) }
defer conn.Close(websocket.StatusNormalClosure, "bye")

for {
    _, data, err := conn.Read(ctx)
    if err != nil { break }

    var msg struct {
        Type     string          `json:"type"`
        Position uint64          `json:"position"`
        Event    json.RawMessage `json:"event"`
    }
    json.Unmarshal(data, &msg)

    // Process event...
    processEvent(msg.Event)

    // Send ACK
    ack, _ := json.Marshal(map[string]interface{}{"type": "ack", "position": msg.Position})
    conn.Write(ctx, websocket.MessageText, ack)
}
```

### 8.4 Resume After Disconnect

On reconnect, pass `resume_after=<last_acked_position>` to resume from where you left off. The server replays any events after that position. Duplicates are possible around reconnect boundaries.

A complete working example is available at `examples/stream-client/main.go`.

---

## 9. Adding New Kafka Source Topics

To ingest events from a new Kafka topic:

### Step 1: Register the Topic

Add the topic name to `internal/kafka/topics.go`:

```go
const (
    TopicECSTEvents     = "ecst-events"
    TopicSendableEvents = "sendable-events"
    TopicEntityMappings = "entity-mappings"
    TopicNewSource      = "my-new-source"  // add this
)
```

### Step 2: Add a Consumer

In `services/ingestion-router/main.go`, create an additional consumer:

```go
consumer2 := kafka.NewConsumer(brokers, kafka.TopicNewSource, "ingestion-router-new", logger)
defer consumer2.Close()

// Run in parallel
go func() {
    if err := consumer2.Consume(ctx, router.processMessage); err != nil {
        logger.Error("new source consumer failed", "error", err)
    }
}()
```

### Step 3: Ensure Event Format

Events on the new topic must conform to the ECSTEvent JSON schema:

```json
{
  "event_id": "unique-id",
  "event_type": "domain.action",
  "source_service": "your-service-name",
  "tenant_id": "tenant-id",
  "entity_id": "entity-id",
  "time": "RFC3339 timestamp",
  "version": "1",
  "payload": { ... }
}
```

### Step 4: Create Subscriptions

Subscribers create subscriptions matching the new event types:

```bash
curl -X POST http://localhost:18081/v1/subscribers/{id}/subscriptions \
  -d '{"event_types": ["domain.action"], "source_filter": "your-service-name"}'
```

No deployment changes are needed — the ingestion-router automatically routes matching events.

---

## 10. Queue Storage and Delivery Semantics

### 10.1 RocksDB Storage Engine

Per-subscription queues are persisted in **RocksDB** (via grocksdb CGO bindings) for durability across pod restarts. All subscriptions share a single RocksDB database with key-prefix isolation:

```
RocksDB Key Layout (sorted, enabling efficient prefix scans):

  q\x00<subID>\x00<uint64 pos>  →  <uint64 enqueue_timestamp><event_bytes>   (queue data)
  c\x00<subID>                  →  <uint64 acked_position>                    (cursor)
  t\x00<subID>                  →  <uint64 tail_position>                     (tail)
```

**Design properties:**
- Each subscription's entries are **contiguous on disk** (sorted by subID + position)
- Range scans use RocksDB prefix iterators — only reads the targeted subscription
- Batch writes (`WriteBatch`) atomically append events + update tail in one operation
- Per-subscription mutexes ensure monotonic position assignment without global locking
- LZ4 compression reduces disk footprint
- 128 MB LRU block cache for hot reads

**Storage configuration:**

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_DIR` | `/data/queue` | RocksDB data directory (mount a persistent volume) |
| `STORE_BACKEND` | `rocksdb` | Set to `memory` for ephemeral dev/testing |
| `QUEUE_TTL` | `720h` (30 days) | Retention period for queue entries |

### 10.2 Per-Subscription Queue Model

Each subscription has an independent append-only queue:

```
Subscription "sub_abc":
  Position 1: [2026-04-01T10:00:00Z] CloudEvent A   ← expired by TTL, deleted by compaction
  Position 2: [2026-04-01T10:00:01Z] CloudEvent B   ← expired by TTL, deleted by compaction
  Position 3: [2026-04-07T12:00:00Z] CloudEvent C   ← acked, deleted by cursor compaction
  Position 4: [2026-04-07T12:00:01Z] CloudEvent D   ← pending delivery
  Position 5: [2026-04-07T12:00:02Z] CloudEvent E   ← pending delivery

  tail_seq    = 5   (last appended)
  acked_seq   = 3   (last delivered + confirmed)
  pending     = 2   (next delivery reads from position 4)
```

### 10.3 Compaction and TTL

A background goroutine runs every 30 seconds with two compaction passes:

**Pass 1 — Cursor-based compaction:**
Deletes all entries at or below the acked cursor. These events were successfully delivered and acknowledged, so they are no longer needed.

```
DeleteRange(q\x00<subID>\x00<0>, q\x00<subID>\x00<cursor+1>)
```

**Pass 2 — TTL-based compaction:**
Scans each subscription's queue from the oldest entry. Deletes entries whose enqueue timestamp is older than `now - QUEUE_TTL`. This catches:
- Events from **abandoned subscriptions** that will never be delivered
- Events from **paused subscriptions** that have been idle for too long
- Events that were never delivered due to persistent failures

When TTL expires undelivered entries, the cursor is **automatically advanced** past them so the dispatcher doesn't attempt to deliver events that no longer exist. This is logged:

```json
{"level":"INFO","msg":"TTL expired entries","subscription_id":"sub-1","expired_through":42,"cursor_advanced_from":0}
```

**TTL configuration examples:**

```bash
QUEUE_TTL=720h    # 30 days (default)
QUEUE_TTL=168h    # 7 days
QUEUE_TTL=24h     # 1 day
QUEUE_TTL=1h      # 1 hour (for testing)
```

### 10.4 Value Format

Each queue entry stores the enqueue timestamp alongside the event data:

```
Value = [8-byte big-endian unix nanos][event bytes (serialized CloudEventProto)]
```

This allows TTL compaction to read the timestamp without deserializing the event, and the `EnqueuedAt` field is returned accurately on Read (not approximated at read time).

### 10.5 Delivery Leases

Only one pod may deliver to a subscription at a time. This is enforced via **epoch-fenced leases**:

1. Dispatcher acquires lease → gets epoch N
2. All Read/Ack operations must include epoch N
3. If dispatcher crashes, lease expires after TTL
4. New dispatcher acquires lease → gets epoch N+1
5. Old dispatcher's stale Ack with epoch N is rejected

### 10.6 Replay

Replay resets the `acked_seq` to an earlier position:

```bash
curl -X POST "http://localhost:18085/v1/admin/subscriptions/{id}/replay?from_position=100"
```

Events from position 101 onward are redelivered, provided they haven't been removed by compaction or TTL. Replay is useful for:
- Subscriber data loss recovery
- Reprocessing after a bug fix
- Historical data rebuild (within the TTL window)

---

## 11. Repository Layout

```
webhooks/
├── internal/
│   ├── backoff/            # Exponential backoff with jitter
│   ├── config/             # Environment variable helpers
│   ├── entity/             # Entity mapping resolver (Redis), syncer, cache warmer
│   │   ├── model.go        #   EntityMapping, EntityMappingEvent types
│   │   ├── resolver.go     #   Redis SMEMBERS/SADD/SREM + WarmCache + FlushAndReload
│   │   ├── syncer.go       #   Kafka consumer → Redis updates
│   │   └── warmer.go       #   Startup warm + periodic full reconciliation
│   ├── health/             # Health check HTTP handlers
│   ├── kafka/              # Producer, Consumer, NewConsumerFromEarliest, topic constants
│   ├── mapper/             # ECST → CloudEvent mapper with per-type hook registry
│   ├── matcher/            # Subscription matcher + entity routing
│   │   ├── matcher.go      #   In-memory type/source/tenant index + bySubscriber index
│   │   ├── entity_matcher.go  # EntityMatcher (wraps Matcher + Redis resolver)
│   │   └── entity_extractor.go # Pluggable per-event-type entity field extraction
│   ├── model/              # Domain types (Subscriber, Subscription, CloudEvent, etc.)
│   ├── observability/      # Structured logging (slog)
│   ├── proto/              # gRPC protobuf definitions + generated code
│   └── signature/          # HMAC-SHA256 webhook signing
├── services/
│   ├── admin-api/          # Admin dashboard UI + REST API
│   ├── config-distributor/ # gRPC config streaming (PG LISTEN/NOTIFY)
│   ├── ingestion-router/   # Event matching + entity routing + CloudEvent mapping
│   ├── queue-writer/       # Kafka → gRPC queue append
│   ├── stream-gateway/     # WebSocket streaming delivery with resume
│   ├── subscriber-queue-service/  # RocksDB-backed durable queues + leases + TTL
│   │   ├── rocksdb_store.go       #   RocksDB storage with cursor + TTL compaction
│   │   ├── store.go               #   QueueStore interface + in-memory fallback
│   │   ├── lease.go               #   Delivery lease manager with epoch fencing
│   │   └── service.go             #   gRPC QueueService implementation
│   ├── subscriber-registry/       # CRUD API + entity mappings + Kafka publish
│   │   ├── entity_store.go        #   Entity mapping DB operations
│   │   ├── entity_handler.go      #   REST endpoints for entity mappings
│   │   └── migrations/            #   PostgreSQL schema (subscribers, subscriptions, entity_mappings)
│   └── webhook-dispatcher/        # Webhook batch delivery with HMAC + fair scheduling
├── synthetic/
│   ├── event-generator/    # Synthetic ECST event producer
│   ├── webhook-subscriber/ # Test webhook receiver with signature verification
│   └── stream-subscriber/  # Test WebSocket streaming consumer
├── examples/
│   └── stream-client/      # Example Go streaming client with resume
├── deploy/
│   ├── docker-compose/     # Full local stack (Kafka, PG, Valkey, all services)
│   └── k8s/                # Kubernetes manifests (Deployments, StatefulSets, HPAs)
├── docs/
│   └── architecture.md     # This document
├── go.mod
├── go.sum
└── Makefile
```
