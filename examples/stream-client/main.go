// Example streaming consumer that connects to the stream-gateway via WebSocket,
// receives CloudEvents in real-time, and sends ACK messages.
//
// Usage:
//   go run ./examples/stream-client \
//     -gateway ws://localhost:18090/v1/stream \
//     -subscription <subscription-id> \
//     -token test-token
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"nhooyr.io/websocket"
)

type eventMessage struct {
	Type     string          `json:"type"`
	Position uint64          `json:"position"`
	Event    json.RawMessage `json:"event"`
}

type cloudEvent struct {
	SpecVersion string          `json:"specversion"`
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Source      string          `json:"source"`
	Time        string          `json:"time"`
	Subject     string          `json:"subject,omitempty"`
	Data        json.RawMessage `json:"data"`
}

type ackMessage struct {
	Type     string `json:"type"`
	Position uint64 `json:"position"`
}

func main() {
	gateway := flag.String("gateway", "ws://localhost:18090/v1/stream", "Stream gateway WebSocket URL")
	subID := flag.String("subscription", "", "Subscription ID (required)")
	token := flag.String("token", "test-token", "Auth token")
	resumeAfter := flag.Uint64("resume-after", 0, "Resume from this position (0 = start)")
	flag.Parse()

	if *subID == "" {
		fmt.Fprintln(os.Stderr, "Error: -subscription flag is required")
		flag.Usage()
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	url := fmt.Sprintf("%s?subscription_id=%s&token=%s", *gateway, *subID, *token)
	if *resumeAfter > 0 {
		url += fmt.Sprintf("&resume_after=%d", *resumeAfter)
	}

	log.Printf("Connecting to %s", url)

	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "bye")

	log.Println("Connected! Waiting for events...")
	log.Println("Press Ctrl+C to disconnect")
	log.Println()

	var received int
	start := time.Now()

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("\nDisconnected after receiving %d events in %s", received, time.Since(start).Round(time.Second))
				return
			}
			log.Fatalf("Read error: %v", err)
		}

		var msg eventMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			log.Printf("Invalid message: %v", err)
			continue
		}

		if msg.Type == "event" {
			received++

			// Parse the CloudEvent for display
			var ce cloudEvent
			json.Unmarshal(msg.Event, &ce)

			fmt.Printf("[pos=%d] %s from %s | subject=%s | %s\n",
				msg.Position, ce.Type, ce.Source, ce.Subject, ce.ID[:8])

			// Send ACK
			ack := ackMessage{Type: "ack", Position: msg.Position}
			ackData, _ := json.Marshal(ack)
			if err := conn.Write(ctx, websocket.MessageText, ackData); err != nil {
				log.Fatalf("ACK write error: %v", err)
			}
		}
	}
}
