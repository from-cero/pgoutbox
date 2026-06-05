package noop

import (
	"context"

	"github.com/from-cero/pgoutbox"
	"github.com/from-cero/pgoutbox/relay/publisher"
)

var _ publisher.Publisher = (*Publisher)(nil)

// Publisher is a relay.Publisher that discards events and reports success for every one.
type Publisher struct{}

// NewPublisher returns a Publisher.
func NewPublisher() *Publisher {
	return &Publisher{}
}

// PublishBatch discards events and returns a result slice of all-nil errors,
// index-aligned with events as the PublishBatch contract requires, so the relay marks every event processed.
func (p *Publisher) PublishBatch(_ context.Context, events []*pgoutbox.Event) []error {
	return make([]error, len(events))
}

// Close is a no-op and always returns nil.
func (p *Publisher) Close() error { return nil }
