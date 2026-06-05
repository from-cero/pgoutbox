package pgoutbox

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Event represents a single outbox message pending delivery to a message broker.
type Event struct {
	ID           uuid.UUID
	Type         string
	Topic        string
	Payload      json.RawMessage
	Status       string
	AttemptCount int
	MaxAttempts  int
	ReapCount    int
	ScheduledAt  time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

const (
	// EventPending is the initial state of an event when it is created and stored in the outbox.
	// It indicates that the event is waiting to be processed by any outbox relay worker.
	EventPending = "pending"

	// EventProcessing indicates that the event is currently being processed by an outbox relay worker.
	// During this state the worker is attempting to send the event to the message broker and other
	// workers must not process this event concurrently.
	EventProcessing = "processing"

	// EventProcessed indicates that the event has been successfully published to the message broker.
	EventProcessed = "processed"

	// EventFailed indicates the event has exhausted its retry budget and will no longer be processed.
	EventFailed = "failed"
)

func (e *Event) fillDefaultsIfNeeded(maxAttempts int) {
	if e.Status == "" {
		e.Status = EventPending
	}
	if e.MaxAttempts == 0 {
		e.MaxAttempts = maxAttempts
	}
	if e.ScheduledAt.IsZero() {
		e.ScheduledAt = time.Now()
	}
}
