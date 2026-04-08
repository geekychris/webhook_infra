package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/event-gateway/internal/signature"
	"nhooyr.io/websocket"
)

// ---------------------------------------------------------------------------
// Stored event wrapper — unifies webhook and stream channel events
// ---------------------------------------------------------------------------

type storedEvent struct {
	ID            string          `json:"id"`
	Channel       string          `json:"channel"` // "webhook" | "stream"
	ReceivedAt    time.Time       `json:"received_at"`
	Event         json.RawMessage `json:"event"`          // the CloudEvent JSON
	HMACStatus    string          `json:"hmac_status"`    // "valid" | "invalid" | "none"
	QueuePosition uint64          `json:"queue_position"` // stream only
	BatchID       string          `json:"batch_id"`       // webhook: groups events from same HTTP POST
	BatchSize     int             `json:"batch_size"`     // webhook: how many events in this POST
	BatchIndex    int             `json:"batch_index"`    // webhook: position within batch (1-based)
	ContentType   string          `json:"content_type"`   // webhook: Content-Type header
	Signature     string          `json:"signature"`      // webhook: X-Webhook-Signature header
	Timestamp     string          `json:"timestamp_header"` // webhook: X-Webhook-Timestamp header
}

// ---------------------------------------------------------------------------
// Ring buffer (thread-safe, last N events)
// ---------------------------------------------------------------------------

type ringBuffer struct {
	mu   sync.RWMutex
	buf  []storedEvent
	cap  int
	pos  int // next write position
	full bool
}

func newRingBuffer(capacity int) *ringBuffer {
	return &ringBuffer{buf: make([]storedEvent, capacity), cap: capacity}
}

func (rb *ringBuffer) add(e storedEvent) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.buf[rb.pos] = e
	rb.pos = (rb.pos + 1) % rb.cap
	if rb.pos == 0 {
		rb.full = true
	}
}

// snapshot returns events newest-first.
func (rb *ringBuffer) snapshot(channel string, limit int) []storedEvent {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	count := rb.pos
	if rb.full {
		count = rb.cap
	}

	out := make([]storedEvent, 0, count)
	for i := 0; i < count; i++ {
		idx := (rb.pos - 1 - i + rb.cap) % rb.cap
		e := rb.buf[idx]
		if channel != "" && channel != "all" && e.Channel != channel {
			continue
		}
		out = append(out, e)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

func (rb *ringBuffer) clear() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.pos = 0
	rb.full = false
}

func (rb *ringBuffer) count(channel string) int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	total := rb.pos
	if rb.full {
		total = rb.cap
	}
	if channel == "" || channel == "all" {
		return total
	}
	n := 0
	for i := 0; i < total; i++ {
		idx := (rb.pos - 1 - i + rb.cap) % rb.cap
		if rb.buf[idx].Channel == channel {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// Browser WebSocket hub — broadcasts stream events to all connected browsers
// ---------------------------------------------------------------------------

type browserHub struct {
	mu      sync.RWMutex
	clients map[*websocket.Conn]struct{}
}

func newBrowserHub() *browserHub {
	return &browserHub{clients: make(map[*websocket.Conn]struct{})}
}

func (h *browserHub) add(c *websocket.Conn) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *browserHub) remove(c *websocket.Conn) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
}

func (h *browserHub) broadcast(data []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = c.Write(ctx, websocket.MessageText, data)
		cancel()
	}
}

func (h *browserHub) count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// ---------------------------------------------------------------------------
// Stream connection state
// ---------------------------------------------------------------------------

type streamConn struct {
	mu     sync.Mutex
	cancel context.CancelFunc
	active bool
	subID  string
}

var activeStreams sync.Map // subscription_id -> *streamConn

func activeStreamCount() int {
	n := 0
	activeStreams.Range(func(_, _ any) bool { n++; return true })
	return n
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	port := envOr("HTTP_PORT", "8071")
	signingKey := envOr("SIGNING_KEY", "test-signing-key")
	registryURL := envOr("REGISTRY_URL", "http://localhost:18081")
	adminURL := envOr("ADMIN_URL", "http://localhost:18085")
	streamGatewayURL := envOr("STREAM_GATEWAY_URL", "ws://localhost:18090")

	events := newRingBuffer(1000)
	hub := newBrowserHub()

	var webhookTotal atomic.Int64
	var streamTotal atomic.Int64

	mux := http.NewServeMux()

	// Serve embedded UI.
	mux.Handle("/", UIHandler())

	// POST /webhook — receive webhook batches from the dispatcher.
	mux.HandleFunc("/webhook", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read error", http.StatusBadRequest)
			return
		}

		// Determine HMAC status.
		hmacStatus := "none"
		if signingKey != "" {
			sig := r.Header.Get("X-Webhook-Signature")
			ts := r.Header.Get("X-Webhook-Timestamp")
			if sig != "" && ts != "" {
				if signature.Verify([]byte(signingKey), ts, body, sig) {
					hmacStatus = "valid"
				} else {
					hmacStatus = "invalid"
				}
			}
		}

		// Capture HTTP headers for display.
		contentType := r.Header.Get("Content-Type")
		sigHeader := r.Header.Get("X-Webhook-Signature")
		tsHeader := r.Header.Get("X-Webhook-Timestamp")

		// Parse as a batch (array) or single CloudEvent.
		var batch []json.RawMessage
		if err := json.Unmarshal(body, &batch); err != nil {
			batch = []json.RawMessage{body}
		}

		// Generate a batch ID so the UI can group events from the same POST.
		batchID := fmt.Sprintf("batch-%d-%d", time.Now().UnixMicro(), len(batch))

		now := time.Now().UTC()
		for i, raw := range batch {
			var meta struct {
				ID string `json:"id"`
			}
			json.Unmarshal(raw, &meta)

			se := storedEvent{
				ID:          meta.ID,
				Channel:     "webhook",
				ReceivedAt:  now,
				Event:       raw,
				HMACStatus:  hmacStatus,
				BatchID:     batchID,
				BatchSize:   len(batch),
				BatchIndex:  i + 1,
				ContentType: contentType,
				Signature:   sigHeader,
				Timestamp:   tsHeader,
			}
			events.add(se)
			webhookTotal.Add(1)
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"accepted":%d,"batch_id":%q}`, len(batch), batchID)
	})

	// GET /api/events — query stored events.
	mux.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			events.clear()
			webhookTotal.Store(0)
			streamTotal.Store(0)
			writeJSON(w, http.StatusOK, map[string]string{"status": "cleared"})
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		channel := r.URL.Query().Get("channel")
		limitStr := r.URL.Query().Get("limit")
		limit := 100
		if limitStr != "" {
			if v, err := strconv.Atoi(limitStr); err == nil && v > 0 {
				limit = v
			}
		}
		writeJSON(w, http.StatusOK, events.snapshot(channel, limit))
	})

	// GET /api/stats
	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"total_webhook_events": webhookTotal.Load(),
			"total_stream_events":  streamTotal.Load(),
			"active_streams":       activeStreamCount(),
		})
	})

	// Proxy: GET /api/subscriptions -> admin API (lists all subscriptions)
	mux.HandleFunc("/api/subscriptions", func(w http.ResponseWriter, r *http.Request) {
		proxyGET(w, adminURL+"/v1/admin/subscriptions")
	})

	// Proxy: GET /api/subscribers -> registry
	mux.HandleFunc("/api/subscribers", func(w http.ResponseWriter, r *http.Request) {
		proxyGET(w, registryURL+"/v1/subscribers")
	})

	// GET /api/stream/connect?subscription_id=X — backend connects to stream-gateway.
	mux.HandleFunc("/api/stream/connect", func(w http.ResponseWriter, r *http.Request) {
		subID := r.URL.Query().Get("subscription_id")
		if subID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "subscription_id required"})
			return
		}
		gateway := r.URL.Query().Get("gateway")
		if gateway == "" {
			gateway = streamGatewayURL
		}
		resumeAfter := r.URL.Query().Get("resume_after")

		// Disconnect existing stream for this subscription if any.
		if val, ok := activeStreams.Load(subID); ok {
			sc := val.(*streamConn)
			sc.mu.Lock()
			if sc.cancel != nil {
				sc.cancel()
			}
			sc.mu.Unlock()
			activeStreams.Delete(subID)
			time.Sleep(100 * time.Millisecond) // let goroutine wind down
		}

		ctx, cancel := context.WithCancel(context.Background())
		sc := &streamConn{cancel: cancel, active: true, subID: subID}
		activeStreams.Store(subID, sc)

		// Build gateway URL.
		gwURL := strings.TrimRight(gateway, "/") + "/v1/stream"
		gwURL += "?subscription_id=" + subID + "&token=test-token"
		if resumeAfter != "" {
			gwURL += "&resume_after=" + resumeAfter
		}

		go func() {
			defer func() {
				sc.mu.Lock()
				sc.active = false
				sc.mu.Unlock()
				activeStreams.Delete(subID)
				// Notify browsers about disconnect.
				msg, _ := json.Marshal(map[string]any{
					"type":            "stream_status",
					"subscription_id": subID,
					"status":          "disconnected",
				})
				hub.broadcast(msg)
			}()

			log.Printf("[stream] connecting to %s", gwURL)
			conn, _, err := websocket.Dial(ctx, gwURL, nil)
			if err != nil {
				log.Printf("[stream] dial error for %s: %v", subID, err)
				msg, _ := json.Marshal(map[string]any{
					"type":            "stream_status",
					"subscription_id": subID,
					"status":          "error",
					"error":           err.Error(),
				})
				hub.broadcast(msg)
				return
			}
			defer conn.Close(websocket.StatusNormalClosure, "consumer-ui disconnect")

			// Notify browsers: connected.
			statusMsg, _ := json.Marshal(map[string]any{
				"type":            "stream_status",
				"subscription_id": subID,
				"status":          "connected",
			})
			hub.broadcast(statusMsg)
			log.Printf("[stream] connected for subscription %s", subID)

			for {
				_, data, err := conn.Read(ctx)
				if err != nil {
					if ctx.Err() != nil {
						log.Printf("[stream] disconnected for %s (cancelled)", subID)
					} else {
						log.Printf("[stream] read error for %s: %v", subID, err)
					}
					return
				}

				var msg struct {
					Type     string          `json:"type"`
					Position uint64          `json:"position"`
					Event    json.RawMessage `json:"event"`
				}
				if err := json.Unmarshal(data, &msg); err != nil {
					continue
				}

				if msg.Type == "event" {
					// Extract event ID.
					var meta struct {
						ID string `json:"id"`
					}
					json.Unmarshal(msg.Event, &meta)

					se := storedEvent{
						ID:            meta.ID,
						Channel:       "stream",
						ReceivedAt:    time.Now().UTC(),
						Event:         msg.Event,
						HMACStatus:    "none",
						QueuePosition: msg.Position,
					}
					events.add(se)
					streamTotal.Add(1)

					// Forward to browser clients.
					fwd, _ := json.Marshal(map[string]any{
						"type":  "stream_event",
						"event": se,
					})
					hub.broadcast(fwd)

					// ACK back to gateway (position must be a number).
					ack, _ := json.Marshal(map[string]interface{}{
						"type":     "ack",
						"position": msg.Position,
					})
					writeCtx, writeCancel := context.WithTimeout(ctx, 2*time.Second)
					_ = conn.Write(writeCtx, websocket.MessageText, ack)
					writeCancel()
				}
			}
		}()

		writeJSON(w, http.StatusOK, map[string]any{
			"status":          "connecting",
			"subscription_id": subID,
			"gateway":         gwURL,
		})
	})

	// POST /api/stream/disconnect?subscription_id=X
	mux.HandleFunc("/api/stream/disconnect", func(w http.ResponseWriter, r *http.Request) {
		subID := r.URL.Query().Get("subscription_id")
		if subID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "subscription_id required"})
			return
		}
		if val, ok := activeStreams.Load(subID); ok {
			sc := val.(*streamConn)
			sc.mu.Lock()
			if sc.cancel != nil {
				sc.cancel()
			}
			sc.mu.Unlock()
			activeStreams.Delete(subID)
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "disconnected"})
	})

	// GET /api/stream/ws — browser WebSocket for real-time stream forwarding.
	mux.HandleFunc("/api/stream/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
		})
		if err != nil {
			log.Printf("[ws] accept error: %v", err)
			return
		}
		hub.add(conn)
		defer func() {
			hub.remove(conn)
			conn.Close(websocket.StatusNormalClosure, "bye")
		}()

		// Keep the connection alive; read pings/control frames.
		ctx := conn.CloseRead(r.Context())
		<-ctx.Done()
	})

	addr := fmt.Sprintf(":%s", port)
	log.Printf("Consumer UI listening on %s", addr)
	log.Printf("  Webhook URL:    http://localhost:%s/webhook", port)
	log.Printf("  Stream Gateway: %s", streamGatewayURL)
	log.Printf("  Registry:       %s", registryURL)
	log.Printf("  Signing key:    %s", signingKey)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func proxyGET(w http.ResponseWriter, url string) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}
