package datatrack

import (
	"encoding/json"
	"time"
)

type Message struct {
	Type        string          `json:"type"`
	Description string          `json:"description"`
	Payload     json.RawMessage `json:"payload"`
}

type CapturedMessage struct {
	CapturedTime time.Time
	RawMessage   *Message
}
