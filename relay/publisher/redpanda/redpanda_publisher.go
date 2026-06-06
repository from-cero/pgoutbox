package redpanda

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/from-cero/pgoutbox"
	"github.com/from-cero/pgoutbox/relay"
)

// recordProducer is the subset of *kgo.Client Publisher uses,
// kept small so PublishBatch's error mapping is testable without a live broker.
type recordProducer interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
	Close()
}

// Publisher implements relay/publisher.Publisher using the franz-go (twmb/franz-go) library.
type Publisher struct {
	client recordProducer
}

// NewPublisher returns a Publisher backed by a franz-go client tuned for an outbox relay:
//   - AllISRAcks mirrors the kafka publisher's RequireAll for durability.
//   - Extra kgo.Opt values (TLS, SASL, etc.) are accepted via opts so callers
//     are not forced to build the client themselves.
func NewPublisher(brokers []string, opts ...kgo.Opt) (*Publisher, error) {
	defaults := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}
	client, err := kgo.NewClient(append(defaults, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("create redpanda client: %w", err)
	}
	return &Publisher{client: client}, nil
}

// NewPublisherWithClient returns a Publisher backed by the given kgo.Client,
// for full control over its configuration (TLS, SASL, partitioner, etc.).
//
// The client must require broker acknowledgement. With RequiredAcks(NoAck()) ProduceSync
// returns before the broker persists the record, so PublishBatch would ack unsent events and
// lose them. Unlike the kafka publisher this cannot be checked: kgo.Client does not expose
// its config, though franz-go's default idempotency rejects NoAck at client creation.
func NewPublisherWithClient(c *kgo.Client) *Publisher {
	return &Publisher{client: c}
}

// PublishBatch produces all events synchronously, returning per-event errors;
// franz-go returns one ProduceResult per record, so results are always index-aligned with events.
func (p *Publisher) PublishBatch(ctx context.Context, events []*pgoutbox.Event) []error {
	records := make([]*kgo.Record, len(events))
	for i, e := range events {
		records[i] = &kgo.Record{
			Topic: e.Topic,
			Value: e.Payload,
			Headers: []kgo.RecordHeader{
				{Key: "event_id", Value: []byte(e.IDString())},
				{Key: "type", Value: []byte(e.Type)},
			},
		}
	}

	results := make([]error, len(events))
	for i, pr := range p.client.ProduceSync(ctx, records...) {
		if i >= len(results) {
			break
		}
		if pr.Err != nil {
			results[i] = classify(fmt.Errorf("produce redpanda record: %w", pr.Err))
		}
	}
	return results
}

// classify marks failures a retry cannot fix as relay.Permanent so the relay parks them
// instead of spending retry budget. A Kafka error the broker reports as non-retriable (auth,
// invalid record, unsupported version, oversized message) is permanent; everything else
// (network, timeout, context) stays transient.
func classify(err error) error {
	if ke, ok := errors.AsType[*kerr.Error](err); ok && !ke.Retriable {
		return relay.Permanent(err)
	}
	return err
}

// Close flushes pending records and closes the underlying client connection.
func (p *Publisher) Close() error {
	p.client.Close()
	return nil
}
