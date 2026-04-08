# Event Conduit

![Event Conduit](event_conduit.png)

An enterprise-grade outbound event delivery platform that consumes internal **ECST events** from Apache Kafka, matches them to subscriber interests, translates to **CloudEvents v1.0**, and delivers them in near real-time via **webhook batches** (HTTPS with HMAC-SHA256 signatures) or **WebSocket streaming**.

## Key Properties

| Property | Detail |
|----------|--------|
| **Delivery** | At-least-once — duplicates possible after crashes |
| **Ordering** | Strict per-subscription — events delivered in queue order |
| **Durability** | RocksDB/Pebble-backed queues survive pod restarts |
| **TTL** | Configurable retention (default 30 days) with background compaction |
| **Latency** | Sub-500ms end-to-end (Redis-based ready queue, no polling) |
| **Batching** | Webhook events batched into single HTTP POSTs (5ms linger window) |
| **Streaming** | WebSocket delivery with Redis pub/sub notification — zero linger |
| **Scalability** | Horizontal — add pods for ingestion, queue writing, dispatching |
| **Fairness** | Round-robin scheduling prevents any subscriber from starving others |

## Architecture

```
Upstream Services → Kafka (ecst-events)
    → Ingestion Router (match + map + entity routing)
        → Kafka (sendable-events)
            → Queue Writer (append + Redis signal)
                → Subscriber Queue Service (RocksDB/Pebble + leases)
                    → Webhook Dispatcher (BZPOPMIN from Redis, batch POST)
                    → Stream Gateway (Redis SUBSCRIBE, WebSocket push)
```

### Services

| Service | Port | Role |
|---------|------|------|
| **subscriber-registry** | 18081 | REST CRUD for subscribers, subscriptions, entity mappings |
| **config-distributor** | — | gRPC streaming config feed (PG LISTEN/NOTIFY) |
| **ingestion-router** | — | Kafka consumer, subscription matching, CloudEvent mapping |
| **queue-writer** | — | Kafka → durable queue append + Redis ready-queue signal |
| **subscriber-queue-service** | — | RocksDB/Pebble queues, delivery leases, TTL compaction |
| **webhook-dispatcher** | — | Redis BZPOPMIN → batch read → HMAC-signed HTTP POST |
| **stream-gateway** | 18090 | Redis SUBSCRIBE → WebSocket push, resume/replay |
| **admin-api** | 18085 | Dashboard UI, pause/resume, replay, stats |
| **valkey** | — | Redis-compatible cache for entity mappings + ready queue + notifications |

### Test & Dev Tools

| Tool | Port | Purpose |
|------|------|---------|
| **producer-ui** | 18070 | Web UI for publishing test events — single event builder + configurable batch generator (count, types, tenants, sources) |
| **consumer-ui** | 18071 | Split-screen web UI: webhook receiver (batch grouping, HMAC, raw JSON) + streaming consumer with subscription picker |
| **java-consumer** | 8072 | Java/Spring Boot/Vaadin streaming consumer with live event grid, auto-reconnect, persistent position tracking |
| **webhook-subscriber** | 19090 | Simple webhook receiver for automated E2E testing |

Events are only generated when you press a button in the producer UI — there is no background event generator.

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Go 1.23+ (for local development)
- RocksDB 10.x (optional, for native RocksDB backend — Pebble is the default)

### Run the Platform

```bash
# Start all core services (9 services + Kafka + PostgreSQL + Valkey)
make compose-up

# Start synthetic test generators
make compose-synthetic

# Start the test UI tools (producer + consumer)
make compose-tools
```

### Access Points

| URL | What |
|-----|------|
| http://localhost:18085/ | Admin Dashboard |
| http://localhost:18070/ | Test Event Producer UI |
| http://localhost:18071/ | Test Event Consumer UI |
| http://localhost:18081/v1/ | Subscriber Registry API |
| ws://localhost:18090/v1/stream | Streaming Gateway WebSocket |

### Create a Test Subscriber

```bash
# Create subscriber pointing at the consumer UI
SUB_ID=$(curl -s -X POST http://localhost:18081/v1/subscribers \
  -H 'Content-Type: application/json' \
  -d '{"name":"My Test","webhook_url":"http://consumer-ui:8071/webhook"}' \
  | jq -r .id)

# Webhook subscription
curl -X POST "http://localhost:18081/v1/subscribers/$SUB_ID/subscriptions" \
  -H 'Content-Type: application/json' \
  -d '{"event_types":["invoice.paid","order.created"],"delivery_mode":"webhook"}'

# Streaming subscription
curl -X POST "http://localhost:18081/v1/subscribers/$SUB_ID/subscriptions" \
  -H 'Content-Type: application/json' \
  -d '{"event_types":["payment.failed","shipment.created"],"delivery_mode":"stream"}'
```

Then open the **Producer UI** (http://localhost:18070/) and publish events — they'll appear in the **Consumer UI** (http://localhost:18071/) in real-time.

## Near Real-Time Delivery

Events flow through the pipeline with sub-500ms latency using an event-driven architecture with **no polling**:

### Webhook Delivery

```
Queue-writer appends event → ZADD dispatch:ready (Redis sorted set)
Dispatcher blocks on BZPOPMIN → wakes instantly → reads batch from queue → HTTP POST
```

- **Linger:** 5ms (only when a single event is pending, to allow burst accumulation)
- **No linger** when multiple events are already queued — delivers the full batch immediately
- **Batching:** Multiple events in one HTTP POST using CloudEvents batch format
- **Fair scheduling:** Sorted set scored by timestamp → oldest-waiting subscription served first

### Streaming Delivery

```
Queue-writer appends event → PUBLISH notify:<sub_id> (Redis pub/sub)
Stream session blocks on SUBSCRIBE → wakes instantly → reads from queue → writes to WebSocket
```

- **Zero linger** — events are pushed to the WebSocket as fast as they arrive
- **No polling** — Redis pub/sub provides instant notification
- **Resume support** — reconnect with `?resume_after=<position>` to replay from any point

## Subscription Matching

Multi-dimensional matching with pluggable per-event-type hooks:

| Dimension | Mechanism | Latency |
|-----------|-----------|---------|
| Event type | In-memory hash map index | < 1μs |
| Source filter | Glob pattern (e.g., `billing-*`) | < 10μs |
| Tenant filter | Set membership check | < 1μs |
| `send_to` field | Direct subscriber ID in payload | < 1μs |
| `user_id` / `company_id` | Redis SMEMBERS lookup | < 1ms |
| Custom entity types | Pluggable `ExtractFunc` per event type | < 1ms |

### Entity-Based Routing

Map users/companies to subscribers for automatic event routing:

```bash
# Map a user to a subscriber
curl -X POST http://localhost:18081/v1/entity-mappings \
  -d '{"entity_type":"user","entity_id":"u_123","subscriber_id":"<uuid>"}'

# Events with user_id=u_123 in metadata or payload now route to that subscriber
```

Entity mappings are cached in Valkey (Redis) with three-layer sync:
1. **Startup warm** — full load from database
2. **Kafka sync** — real-time incremental updates via `entity-mappings` topic
3. **Periodic reconciliation** — full flush+reload every 5 minutes

### Custom Mapping & Extraction Hooks

```go
// Transform outbound payload per event type
mapper.Register("invoice.paid", func(event model.ECSTEvent, ce model.CloudEvent) (model.CloudEvent, error) {
    // Strip internal fields, project only what subscribers need
    var p map[string]interface{}
    json.Unmarshal(event.Payload, &p)
    delete(p, "internal_code")
    ce.Data, _ = json.Marshal(p)
    return ce, nil
})

// Custom entity extraction for non-standard field names
extractor.Register("task.assigned", func(event model.ECSTEvent, payload map[string]interface{}) ([]matcher.EntityRef, []model.SubscriberID) {
    var refs []matcher.EntityRef
    if ownerID, ok := payload["owner_id"].(string); ok {
        refs = append(refs, matcher.EntityRef{Type: "user", ID: ownerID})
    }
    return refs, nil
})
```

## Queue Storage

Durable per-subscription queues backed by **Pebble** (pure Go, default) or **RocksDB** (CGO, build with `-tags rocksdb`):

- Each subscription has an independent ordered queue
- Key layout: `q\x00<subID>\x00<position>` — contiguous on disk per subscription
- Background compaction: removes acked entries + entries past TTL
- Configurable TTL: `QUEUE_TTL=720h` (default 30 days)
- Survives pod restarts (Docker volume at `/data/queue`)

## Development

```bash
# Build all binaries
make build

# Run unit tests (67+ tests)
make test

# Run E2E tests against live Docker Compose stack (12 tests)
make compose-up && make compose-synthetic && make e2e

# Regenerate protobuf
make proto
```

### Key Environment Variables

| Variable | Default | Used By |
|----------|---------|---------|
| `DATABASE_URL` | — | registry, config-distributor, admin-api |
| `KAFKA_BROKERS` | localhost:9092 | all Kafka services |
| `REDIS_ADDR` | localhost:6379 | ingestion-router, queue-writer, dispatcher, stream-gateway |
| `QUEUE_TTL` | 720h | subscriber-queue-service |
| `LINGER_MS` | 5 | webhook-dispatcher |
| `CACHE_SYNC_INTERVAL` | 5m | ingestion-router |
| `STORE_BACKEND` | pebble | subscriber-queue-service (or `rocksdb` with CGO) |

## Repository Structure

```
webhooks/
├── internal/                    # Shared packages
│   ├── backoff/                 #   Exponential backoff with jitter
│   ├── config/                  #   Environment variable helpers
│   ├── entity/                  #   Redis entity resolver, Kafka syncer, cache warmer
│   ├── health/                  #   Health check HTTP handlers
│   ├── kafka/                   #   Producer (batch), Consumer (earliest offset), topics
│   ├── mapper/                  #   ECST→CloudEvent with per-type hook registry
│   ├── matcher/                 #   Type/source/tenant index + entity matcher + extractor
│   ├── model/                   #   Domain types (Subscriber, Subscription, CloudEvent, etc.)
│   ├── observability/           #   Structured logging (slog)
│   ├── proto/                   #   gRPC protobuf definitions + generated code
│   ├── readyqueue/              #   Redis sorted set (BZPOPMIN) + pub/sub notifications
│   └── signature/               #   HMAC-SHA256 webhook signing
├── services/                    # Microservices
│   ├── admin-api/               #   Dashboard UI + REST API
│   ├── config-distributor/      #   gRPC config streaming
│   ├── ingestion-router/        #   Event matching + entity routing
│   ├── queue-writer/            #   Kafka → queue append + Redis signal
│   ├── stream-gateway/          #   WebSocket streaming (Redis pub/sub)
│   ├── subscriber-queue-service/#   Pebble/RocksDB queues + leases + TTL
│   ├── subscriber-registry/     #   CRUD API + entity mappings
│   └── webhook-dispatcher/      #   Redis BZPOPMIN → batch delivery
├── tools/                       # Development & test UI tools
│   ├── producer-ui/             #   Web UI: event builder + configurable batch generator
│   └── consumer-ui/             #   Web UI: webhook receiver + streaming consumer
├── clients/
│   └── java-consumer/           #   Java/Spring Boot/Vaadin streaming consumer
├── synthetic/
│   └── webhook-subscriber/      #   Simple webhook receiver for E2E tests
├── examples/
│   └── stream-client/           #   Example Go streaming client
├── deploy/
│   ├── docker-compose/          #   Full local development stack
│   └── k8s/                     #   Kubernetes manifests (13 files)
├── docs/
│   └── architecture.md          #   Comprehensive architecture document (1000+ lines)
├── tests/
│   └── e2e/                     #   End-to-end tests (12 tests)
├── go.mod
├── go.sum
└── Makefile
```

## Documentation

See [docs/architecture.md](docs/architecture.md) for the comprehensive architecture guide including:
- System design and service inventory
- Event flow from ECST → CloudEvent → delivery
- Build and run instructions
- Administrator guide (subscriber/subscription management)
- Subscriber implementation guide (webhook HMAC verification, streaming protocol)
- How to add new Kafka source topics
- Subscription matching deep dive
- Entity-based routing with Redis cache
- Custom mapping and extraction hooks
- Queue storage and TTL compaction
- Delivery semantics (leases, replay, batching)
