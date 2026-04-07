package observability

import (
	"log/slog"
	"os"
)

// NewLogger creates a structured JSON logger for service use.
func NewLogger(service string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	return slog.New(handler).With("service", service)
}
