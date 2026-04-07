package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"

	"github.com/example/event-gateway/internal/config"
	"github.com/example/event-gateway/internal/model"
	"github.com/example/event-gateway/internal/observability"
	"github.com/example/event-gateway/internal/signature"
)

func main() {
	logger := observability.NewLogger("webhook-subscriber")

	httpPort := config.Env("HTTP_PORT", "9090")
	failureRate, _ := strconv.ParseFloat(config.Env("FAILURE_RATE", "0.0"), 64)
	signingKey := []byte(config.Env("SIGNING_KEY", "test-signing-key"))

	var received atomic.Int64

	http.HandleFunc("POST /webhook", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Error("failed to read body", "error", err)
			http.Error(w, "read error", http.StatusInternalServerError)
			return
		}

		// Verify HMAC signature if present
		sig := r.Header.Get("X-Webhook-Signature")
		ts := r.Header.Get("X-Webhook-Timestamp")
		if sig != "" && ts != "" {
			expectedSig := "sha256=" + signature.Sign(signingKey, ts, body)
			if sig != expectedSig {
				logger.Warn("signature mismatch",
					"expected", expectedSig,
					"got", sig,
				)
			}
		}

		// Parse batch
		var events []model.CloudEvent
		if err := json.Unmarshal(body, &events); err != nil {
			logger.Error("failed to parse batch", "error", err)
			http.Error(w, "parse error", http.StatusBadRequest)
			return
		}

		count := received.Add(int64(len(events)))

		// Simulate failures
		if failureRate > 0 && rand.Float64() < failureRate {
			logger.Warn("simulating failure", "events", len(events))
			http.Error(w, "simulated failure", http.StatusInternalServerError)
			return
		}

		logger.Info("received webhook batch",
			"events", len(events),
			"total_received", count,
			"first_type", events[0].Type,
		)

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	http.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]int64{
			"total_received": received.Load(),
		})
	})

	http.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	logger.Info("webhook subscriber starting", "port", httpPort, "failure_rate", failureRate)
	if err := http.ListenAndServe(fmt.Sprintf(":%s", httpPort), nil); err != nil {
		logger.Error("server failed", "error", err)
		os.Exit(1)
	}
}
