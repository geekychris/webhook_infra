package model

import (
	"encoding/json"
	"time"
)

// CloudEvent follows the CloudEvents v1.0 specification.
type CloudEvent struct {
	SpecVersion     string          `json:"specversion"`
	ID              string          `json:"id"`
	Type            string          `json:"type"`
	Source          string          `json:"source"`
	DataContentType string          `json:"datacontenttype"`
	Time            time.Time       `json:"time"`
	Subject         string          `json:"subject,omitempty"`
	DataSchema      string          `json:"dataschema,omitempty"`
	Data            json.RawMessage `json:"data"`
}
