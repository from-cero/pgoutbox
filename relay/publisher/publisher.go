package publisher

import (
	"context"

	"github.com/from-cero/pgoutbox"
)

// Publisher is an interface that abstracts the mechanism for publishing events to a broker.
type Publisher interface {
	// PublishBatch publishes events to the broker and reports per-event outcomes.
	// The returned slice must be index-aligned with events:
	// 	- a nil element means the corresponding event was acknowledged by the broker,
	// 	- a non-nil element carries that event's failure.
	PublishBatch(ctx context.Context, events []*pgoutbox.Event) []error

	// Close releases any resources held by the Publisher, flushing buffered messages first.
	Close() error
}
