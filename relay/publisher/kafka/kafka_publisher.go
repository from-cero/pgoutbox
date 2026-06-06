package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/from-cero/pgoutbox"
	"github.com/from-cero/pgoutbox/relay"
)

// messageWriter is the subset of *kafka.Writer Publisher uses,
// kept small so PublishBatch's error mapping is testable without a live broker.
type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// Publisher implements relay/publisher.Publisher using the segmentio/kafka-go library.
type Publisher struct {
	writer messageWriter
}

// NewPublisher returns a Publisher tuned for an outbox relay:
//   - Keyless messages with a round-robin balancer spread events across partitions; with no
//     ordering guarantee consumers must tolerate reordering and dedup on the event_id header.
//   - BatchTimeout is lowered from kafka-go's 1s default so small batches do not stall the relay.
//   - Writer retries are kept low; retry policy belongs to the relay.
func NewPublisher(brokers []string) *Publisher {
	return &Publisher{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			RequiredAcks: kafka.RequireAll,
			Balancer:     &kafka.RoundRobin{},
			BatchTimeout: 10 * time.Millisecond,
			MaxAttempts:  3,
			WriteTimeout: 10 * time.Second,
		},
	}
}

// NewPublisherWithWriter returns a Publisher backed by the given kafka.Writer,
// for full control over its configuration (balancer, timeouts, TLS, etc.).
//
// The writer must be synchronous. An async writer (Async: true) returns nil before delivering
// and reports outcomes via its Completion callback, so PublishBatch would ack unsent events and
// lose them; it panics on such a writer to surface the misconfiguration at startup.
func NewPublisherWithWriter(w *kafka.Writer) *Publisher {
	if w.Async {
		panic("kafka: Publisher requires a synchronous writer; Async must be false")
	}
	return &Publisher{writer: w}
}

// PublishBatch writes all events to Kafka, returning per-event errors index-aligned with events;
// a nil element means the broker acknowledged that event.
func (p *Publisher) PublishBatch(ctx context.Context, events []*pgoutbox.Event) []error {
	msgs := make([]kafka.Message, len(events))
	for i, e := range events {
		msgs[i] = kafka.Message{
			Topic: e.Topic,
			Value: e.Payload,
			Headers: []kafka.Header{
				{Key: "event_id", Value: []byte(e.IDString())},
				{Key: "type", Value: []byte(e.Type)},
			},
		}
	}

	results := make([]error, len(events))
	err := p.writer.WriteMessages(ctx, msgs...)
	if err == nil {
		return results
	}

	// kafka-go reports partial failures as WriteErrors, index-aligned with the messages sent.
	if writeErrs, ok := errors.AsType[kafka.WriteErrors](err); ok {
		for i, werr := range writeErrs {
			if i >= len(results) {
				break
			}
			if werr != nil {
				results[i] = classify(fmt.Errorf("write kafka message: %w", werr))
			}
		}
		return results
	}

	// kafka-go rejects oversized messages before sending with a top-level MessageTooLargeError,
	// aborting the batch at the first offender. That event is permanent; the rest stay transient
	// so the relay retries them and catches any further offenders next pass.
	if tooLarge, ok := errors.AsType[kafka.MessageTooLargeError](err); ok {
		badID := headerValue(tooLarge.Message.Headers, "event_id")
		for i := range results {
			if headerValue(msgs[i].Headers, "event_id") == badID {
				results[i] = relay.Permanent(fmt.Errorf("write kafka message: %w", err))
			} else {
				results[i] = fmt.Errorf("abort batch for event %s: %w", events[i].IDString(), errBatchAborted)
			}
		}
		return results
	}

	// Any other error failed the whole batch. Classify it so a non-retriable failure (auth,
	// unsupported version, invalid config) is parked instead of retried until attempts run out.
	whole := classify(fmt.Errorf("write kafka message: %w", err))
	for i := range results {
		results[i] = whole
	}
	return results
}

// headerValue returns the named header's value, or "" if absent,
// used to identify a message in a batch by its unique event_id header.
func headerValue(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// classify marks failures a retry cannot fix as relay.Permanent so the relay parks them
// instead of spending retry budget. A Kafka error code whose Temporary() is false (auth,
// invalid record, unsupported version, oversized message) is permanent; errors without a
// Kafka code (network, timeout, context) stay transient.
func classify(err error) error {
	if kerr, ok := errors.AsType[kafka.Error](err); ok && !kerr.Temporary() {
		return relay.Permanent(err)
	}
	return err
}

// Close flushes any buffered messages and releases the underlying writer.
func (p *Publisher) Close() error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}
	return nil
}
