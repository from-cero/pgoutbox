package pgoutbox

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// Event represents a single outbox message pending delivery to a message broker.
type Event struct {
	ID           pgtype.UUID
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

// IDString returns the canonical hyphenated string form of the event id, or an
// empty string when the id is not set. pgtype.UUID has no String method, so use
// this when an event id is needed as text (broker headers, log fields).
func (e *Event) IDString() string {
	if !e.ID.Valid {
		return ""
	}
	b := e.ID.Bytes
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

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
