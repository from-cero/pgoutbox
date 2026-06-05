package redpanda

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/from-cero/pgoutbox"
	"github.com/from-cero/pgoutbox/relay"
)

// fakeClient is a recordProducer that records the produced records and returns
// canned per-record results, so PublishBatch's mapping is testable without a broker.
type fakeClient struct {
	got     []*kgo.Record
	results kgo.ProduceResults
	closed  bool
}

func (c *fakeClient) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	c.got = rs
	if c.results != nil {
		return c.results
	}
	out := make(kgo.ProduceResults, len(rs)) // all nil Err == all acked
	for i, r := range rs {
		out[i].Record = r
	}
	return out
}

func (c *fakeClient) Close() { c.closed = true }

func eventWithID(topic string) *pgoutbox.Event {
	return &pgoutbox.Event{
		ID:      uuid.New(),
		Type:    "test.type",
		Topic:   topic,
		Payload: []byte(`{"k":"v"}`),
	}
}

func headerValue(headers []kgo.RecordHeader, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func TestPublishBatchSuccess(t *testing.T) {
	c := &fakeClient{}
	p := &Publisher{client: c}
	e1, e2 := eventWithID("topic-a"), eventWithID("topic-b")

	results := p.PublishBatch(context.Background(), []*pgoutbox.Event{e1, e2})
	if len(results) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(results))
	}
	for i, err := range results {
		if err != nil {
			t.Errorf("results[%d] = %v, want nil", i, err)
		}
	}

	if len(c.got) != 2 {
		t.Fatalf("client saw %d records, want 2", len(c.got))
	}
	if c.got[0].Topic != "topic-a" || string(c.got[0].Value) != `{"k":"v"}` {
		t.Errorf("record[0] = %+v, unexpected topic/value", c.got[0])
	}
	if id := headerValue(c.got[0].Headers, "event_id"); id != e1.ID.String() {
		t.Errorf("event_id header = %q, want %q", id, e1.ID.String())
	}
	if typ := headerValue(c.got[0].Headers, "type"); typ != "test.type" {
		t.Errorf("type header = %q, want test.type", typ)
	}
}

func TestPublishBatchPerRecordErrors(t *testing.T) {
	e1, e2, e3 := eventWithID("t"), eventWithID("t"), eventWithID("t")
	c := &fakeClient{results: kgo.ProduceResults{
		{Err: nil},
		{Err: &kerr.Error{Message: "NETWORK", Retriable: true}}, // transient
		{Err: &kerr.Error{Message: "AUTH", Retriable: false}},   // permanent
	}}
	p := &Publisher{client: c}

	results := p.PublishBatch(context.Background(), []*pgoutbox.Event{e1, e2, e3})
	if len(results) != 3 {
		t.Fatalf("len(results) = %d, want 3", len(results))
	}
	if results[0] != nil {
		t.Errorf("results[0] = %v, want nil", results[0])
	}
	if results[1] == nil || relay.IsPermanent(results[1]) {
		t.Errorf("results[1] = %v, want a transient error", results[1])
	}
	if results[2] == nil || !relay.IsPermanent(results[2]) {
		t.Errorf("results[2] = %v, want a permanent error", results[2])
	}
}

func TestClassify(t *testing.T) {
	t.Run("non-retriable kafka error becomes permanent", func(t *testing.T) {
		if !relay.IsPermanent(classify(&kerr.Error{Retriable: false})) {
			t.Error("non-retriable error should be permanent")
		}
	})
	t.Run("retriable kafka error stays transient", func(t *testing.T) {
		if relay.IsPermanent(classify(&kerr.Error{Retriable: true})) {
			t.Error("retriable error should stay transient")
		}
	})
	t.Run("non-kafka error stays transient", func(t *testing.T) {
		if relay.IsPermanent(classify(errors.New("network blip"))) {
			t.Error("a non-kafka error should stay transient")
		}
	})
}

func TestClose(t *testing.T) {
	c := &fakeClient{}
	if err := (&Publisher{client: c}).Close(); err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
	if !c.closed {
		t.Error("underlying client was not closed")
	}
}

func TestNewPublisher(t *testing.T) {
	p, err := NewPublisher([]string{"localhost:9092"})
	if err != nil {
		t.Fatalf("NewPublisher() error: %v", err)
	}
	if p == nil {
		t.Fatal("NewPublisher returned nil")
	}
	p.Close()
}

func TestNewPublisherWithClient(t *testing.T) {
	client, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		t.Fatalf("kgo.NewClient error: %v", err)
	}
	p := NewPublisherWithClient(client)
	if p == nil {
		t.Fatal("NewPublisherWithClient returned nil")
	}
	p.Close()
}
