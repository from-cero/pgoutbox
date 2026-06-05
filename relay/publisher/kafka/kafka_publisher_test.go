package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/from-cero/pgoutbox"
	"github.com/from-cero/pgoutbox/relay"
)

// fakeWriter is a messageWriter that records the messages it received and
// returns a canned error, so PublishBatch's error mapping is exercised without a broker.
type fakeWriter struct {
	got      []kafka.Message
	writeErr error
	closeErr error
	closed   bool
}

func (w *fakeWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	w.got = msgs
	return w.writeErr
}

func (w *fakeWriter) Close() error {
	w.closed = true
	return w.closeErr
}

func eventWithID(topic string) *pgoutbox.Event {
	return &pgoutbox.Event{
		ID:      uuid.New(),
		Type:    "test.type",
		Topic:   topic,
		Payload: []byte(`{"k":"v"}`),
	}
}

func TestPublishBatchSuccess(t *testing.T) {
	w := &fakeWriter{}
	p := &Publisher{writer: w}
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

	if len(w.got) != 2 {
		t.Fatalf("writer saw %d messages, want 2", len(w.got))
	}
	if w.got[0].Topic != "topic-a" || string(w.got[0].Value) != `{"k":"v"}` {
		t.Errorf("message[0] = %+v, unexpected topic/value", w.got[0])
	}
	if id := headerValue(w.got[0].Headers, "event_id"); id != e1.ID.String() {
		t.Errorf("event_id header = %q, want %q", id, e1.ID.String())
	}
	if typ := headerValue(w.got[0].Headers, "type"); typ != "test.type" {
		t.Errorf("type header = %q, want test.type", typ)
	}
}

func TestPublishBatchPartialWriteErrors(t *testing.T) {
	e1, e2, e3 := eventWithID("t"), eventWithID("t"), eventWithID("t")
	// e1 acked, e2 transient failure, e3 permanent (non-temporary) failure.
	w := &fakeWriter{writeErr: kafka.WriteErrors{
		nil,
		kafka.RequestTimedOut,          // Temporary() == true
		kafka.TopicAuthorizationFailed, // Temporary() == false
	}}
	p := &Publisher{writer: w}

	results := p.PublishBatch(context.Background(), []*pgoutbox.Event{e1, e2, e3})
	if len(results) != 3 {
		t.Fatalf("len(results) = %d, want 3", len(results))
	}
	if results[0] != nil {
		t.Errorf("results[0] = %v, want nil", results[0])
	}
	if results[1] == nil || relay.IsPermanent(results[1]) {
		t.Errorf("results[1] = %v, want a transient (non-permanent) error", results[1])
	}
	if results[2] == nil || !relay.IsPermanent(results[2]) {
		t.Errorf("results[2] = %v, want a permanent error", results[2])
	}
}

func TestPublishBatchMessageTooLarge(t *testing.T) {
	e1, e2 := eventWithID("t"), eventWithID("t")
	p := &Publisher{}

	// Build the too-large error so its Message carries e2's event_id header.
	tooLarge := kafka.MessageTooLargeError{
		Message: kafka.Message{
			Headers: []kafka.Header{{Key: "event_id", Value: []byte(e2.ID.String())}},
		},
	}
	p.writer = &fakeWriter{writeErr: tooLarge}

	results := p.PublishBatch(context.Background(), []*pgoutbox.Event{e1, e2})
	if len(results) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(results))
	}
	// e1 was aborted (transient), e2 is the oversized offender (permanent).
	if results[0] == nil || relay.IsPermanent(results[0]) {
		t.Errorf("results[0] = %v, want transient abort", results[0])
	}
	if !errors.Is(results[0], errBatchAborted) {
		t.Errorf("results[0] = %v, want errBatchAborted", results[0])
	}
	if results[1] == nil || !relay.IsPermanent(results[1]) {
		t.Errorf("results[1] = %v, want permanent", results[1])
	}
}

func TestPublishBatchWholeBatchError(t *testing.T) {
	t.Run("plain error stays transient for all events", func(t *testing.T) {
		w := &fakeWriter{writeErr: errors.New("connection refused")}
		p := &Publisher{writer: w}
		results := p.PublishBatch(context.Background(), []*pgoutbox.Event{eventWithID("t"), eventWithID("t")})
		for i, err := range results {
			if err == nil || relay.IsPermanent(err) {
				t.Errorf("results[%d] = %v, want transient error", i, err)
			}
		}
	})

	t.Run("non-temporary kafka error is permanent for all events", func(t *testing.T) {
		w := &fakeWriter{writeErr: kafka.SASLAuthenticationFailed}
		p := &Publisher{writer: w}
		results := p.PublishBatch(context.Background(), []*pgoutbox.Event{eventWithID("t"), eventWithID("t")})
		for i, err := range results {
			if !relay.IsPermanent(err) {
				t.Errorf("results[%d] = %v, want permanent error", i, err)
			}
		}
	})
}

func TestClassify(t *testing.T) {
	t.Run("non-temporary kafka error becomes permanent", func(t *testing.T) {
		if !relay.IsPermanent(classify(kafka.UnsupportedVersion)) {
			t.Error("non-temporary kafka error should be permanent")
		}
	})
	t.Run("temporary kafka error stays transient", func(t *testing.T) {
		if relay.IsPermanent(classify(kafka.LeaderNotAvailable)) {
			t.Error("temporary kafka error should stay transient")
		}
	})
	t.Run("non-kafka error stays transient", func(t *testing.T) {
		if relay.IsPermanent(classify(errors.New("network blip"))) {
			t.Error("a non-kafka error should stay transient")
		}
	})
}

func TestHeaderValue(t *testing.T) {
	headers := []kafka.Header{
		{Key: "event_id", Value: []byte("abc")},
		{Key: "type", Value: []byte("x")},
	}
	if got := headerValue(headers, "event_id"); got != "abc" {
		t.Errorf("headerValue = %q, want abc", got)
	}
	if got := headerValue(headers, "missing"); got != "" {
		t.Errorf("headerValue(missing) = %q, want empty", got)
	}
}

func TestClose(t *testing.T) {
	t.Run("returns nil on success", func(t *testing.T) {
		w := &fakeWriter{}
		if err := (&Publisher{writer: w}).Close(); err != nil {
			t.Errorf("Close() = %v, want nil", err)
		}
		if !w.closed {
			t.Error("underlying writer was not closed")
		}
	})

	t.Run("wraps writer errors", func(t *testing.T) {
		sentinel := errors.New("close boom")
		err := (&Publisher{writer: &fakeWriter{closeErr: sentinel}}).Close()
		if !errors.Is(err, sentinel) {
			t.Errorf("Close() = %v, want wrap of %v", err, sentinel)
		}
	})
}

func TestNewPublisher(t *testing.T) {
	if NewPublisher([]string{"localhost:9092"}) == nil {
		t.Error("NewPublisher returned nil")
	}
}

func TestNewPublisherWithWriter(t *testing.T) {
	t.Run("accepts a synchronous writer", func(t *testing.T) {
		if NewPublisherWithWriter(&kafka.Writer{}) == nil {
			t.Error("NewPublisherWithWriter returned nil")
		}
	})

	t.Run("panics on an async writer", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Error("expected a panic for an async writer")
			}
		}()
		NewPublisherWithWriter(&kafka.Writer{Async: true})
	})
}
