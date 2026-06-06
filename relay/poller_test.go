package relay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/from-cero/pgoutbox"
)

// newTestRelay builds a Relay with a quiet logger and the given options.
func newTestRelay(t *testing.T, s Store, p *fakePublisher, opts ...Option) (*Relay, *BatchStats) {
	t.Helper()
	var captured BatchStats
	hookOpt := WithHooks(Hooks{OnBatch: func(bs BatchStats) { captured = bs }})
	r, err := New(s, p, discardLogger(), append([]Option{hookOpt}, opts...)...)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	return r, &captured
}

func TestProcessPendingEventsBatch(t *testing.T) {
	ctx := context.Background()

	t.Run("no pending events does nothing", func(t *testing.T) {
		s := &fakeStore{}
		r, _ := newTestRelay(t, s, &fakePublisher{})
		n, err := r.processPendingEventsBatch(ctx, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if n != 0 {
			t.Errorf("processed = %d, want 0", n)
		}
	})

	t.Run("fetch error is wrapped", func(t *testing.T) {
		sentinel := errors.New("fetch boom")
		s := &fakeStore{fetchErr: sentinel}
		r, _ := newTestRelay(t, s, &fakePublisher{})
		if _, err := r.processPendingEventsBatch(ctx, nil); !errors.Is(err, sentinel) {
			t.Fatalf("err = %v, want wrap of %v", err, sentinel)
		}
	})

	t.Run("all published are marked processed", func(t *testing.T) {
		e1, e2 := newEvent(), newEvent()
		s := &fakeStore{
			fetchBatches:     [][]*pgoutbox.Event{{e1, e2}},
			markProcessedIDs: []pgtype.UUID{e1.ID, e2.ID},
		}
		pub := &fakePublisher{}
		r, stats := newTestRelay(t, s, pub)

		n, err := r.processPendingEventsBatch(ctx, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if n != 2 {
			t.Errorf("processed = %d, want 2", n)
		}
		if len(pub.got) != 2 {
			t.Errorf("publisher saw %d events, want 2", len(pub.got))
		}
		if len(s.markProcessedGot) != 2 {
			t.Errorf("MarkProcessed saw %d events, want 2", len(s.markProcessedGot))
		}
		if stats.Claimed != 2 || stats.Published != 2 || stats.Failed != 0 || stats.Lost != 0 {
			t.Errorf("stats = %+v, want Claimed=2 Published=2", *stats)
		}
	})

	t.Run("topic resolution failure parks the event permanently", func(t *testing.T) {
		e := newEvent()
		e.Topic = "" // forces resolution; no resolver configured
		s := &fakeStore{
			fetchBatches: [][]*pgoutbox.Event{{e}},
			failIDs:      []pgtype.UUID{e.ID},
		}
		pub := &fakePublisher{}
		r, stats := newTestRelay(t, s, pub)

		if _, err := r.processPendingEventsBatch(ctx, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(pub.got) != 0 {
			t.Errorf("event with no topic should not be published, got %d", len(pub.got))
		}
		if len(s.failGot) != 1 {
			t.Fatalf("Fail saw %d events, want 1", len(s.failGot))
		}
		if stats.Failed != 1 || stats.Permanent != 1 {
			t.Errorf("stats = %+v, want Failed=1 Permanent=1", *stats)
		}
	})

	t.Run("transient publish failure is marked failed with backoff", func(t *testing.T) {
		e := newEvent()
		s := &fakeStore{
			fetchBatches:  [][]*pgoutbox.Event{{e}},
			markFailedIDs: []pgtype.UUID{e.ID},
		}
		pub := &fakePublisher{results: []error{errors.New("broker down")}}
		r, stats := newTestRelay(t, s, pub, WithBackoff(func(int) time.Duration { return 7 * time.Second }))

		if _, err := r.processPendingEventsBatch(ctx, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(s.markFailedGot) != 1 {
			t.Fatalf("MarkFailed saw %d events, want 1", len(s.markFailedGot))
		}
		if len(s.markFailedBackoffs) != 1 || s.markFailedBackoffs[0] != 7*time.Second {
			t.Errorf("backoffs = %v, want [7s]", s.markFailedBackoffs)
		}
		if stats.Failed != 1 || stats.Permanent != 0 {
			t.Errorf("stats = %+v, want Failed=1 Permanent=0", *stats)
		}
	})

	t.Run("permanent publish failure is parked, not retried", func(t *testing.T) {
		e := newEvent()
		s := &fakeStore{
			fetchBatches: [][]*pgoutbox.Event{{e}},
			failIDs:      []pgtype.UUID{e.ID},
		}
		pub := &fakePublisher{results: []error{Permanent(errors.New("auth failed"))}}
		r, stats := newTestRelay(t, s, pub)

		if _, err := r.processPendingEventsBatch(ctx, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(s.failGot) != 1 {
			t.Fatalf("Fail saw %d events, want 1", len(s.failGot))
		}
		if len(s.markFailedGot) != 0 {
			t.Errorf("permanent failure should not be marked for retry")
		}
		if stats.Permanent != 1 {
			t.Errorf("stats.Permanent = %d, want 1", stats.Permanent)
		}
	})

	t.Run("misaligned publisher results fail the affected events", func(t *testing.T) {
		e1, e2 := newEvent(), newEvent()
		s := &fakeStore{
			fetchBatches:  [][]*pgoutbox.Event{{e1, e2}},
			markFailedIDs: []pgtype.UUID{e2.ID},
		}
		// publisher returns only one result for two events
		pub := &fakePublisher{results: []error{nil}}
		r, stats := newTestRelay(t, s, pub)

		if _, err := r.processPendingEventsBatch(ctx, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// e1 acked, e2 has no result -> errMisalignedResults -> transient failure
		if len(s.markFailedGot) != 1 || s.markFailedGot[0].ID != e2.ID {
			t.Errorf("expected e2 marked failed, got %+v", s.markFailedGot)
		}
		if stats.Published != 1 || stats.Failed != 1 {
			t.Errorf("stats = %+v, want Published=1 Failed=1", *stats)
		}
	})

	t.Run("claim fence: published event no longer owned counts as lost", func(t *testing.T) {
		e := newEvent()
		s := &fakeStore{
			fetchBatches:     [][]*pgoutbox.Event{{e}},
			markProcessedIDs: nil, // store reports it updated nothing
		}
		r, stats := newTestRelay(t, s, &fakePublisher{})

		if _, err := r.processPendingEventsBatch(ctx, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if stats.Lost != 1 {
			t.Errorf("stats.Lost = %d, want 1", stats.Lost)
		}
	})

	t.Run("shutdown mid-batch unclaims unpublished and keeps publishes", func(t *testing.T) {
		canceled, cancel := context.WithCancel(context.Background())
		cancel()

		e1, e2 := newEvent(), newEvent()
		e2.Topic = "" // will fail topic resolution -> ends up in failures -> unclaimed
		s := &fakeStore{
			fetchBatches:     [][]*pgoutbox.Event{{e1, e2}},
			markProcessedIDs: []pgtype.UUID{e1.ID},
			unclaimIDs:       []pgtype.UUID{e2.ID},
		}
		r, stats := newTestRelay(t, s, &fakePublisher{})

		n, err := r.processPendingEventsBatch(canceled, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if n != 2 {
			t.Errorf("processed = %d, want 2", n)
		}
		if len(s.unclaimGot) != 1 || s.unclaimGot[0].ID != e2.ID {
			t.Errorf("expected e2 unclaimed, got %+v", s.unclaimGot)
		}
		if stats.Unclaimed != 1 {
			t.Errorf("stats.Unclaimed = %d, want 1", stats.Unclaimed)
		}
	})
}

func TestResolveTopic(t *testing.T) {
	t.Run("keeps an existing topic", func(t *testing.T) {
		r, _ := newTestRelay(t, &fakeStore{}, &fakePublisher{})
		e := &pgoutbox.Event{Topic: "existing"}
		if err := r.resolveTopic(e); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if e.Topic != "existing" {
			t.Errorf("Topic = %q, want existing", e.Topic)
		}
	})

	t.Run("no topic and no resolver is a permanent error", func(t *testing.T) {
		r, _ := newTestRelay(t, &fakeStore{}, &fakePublisher{})
		err := r.resolveTopic(&pgoutbox.Event{})
		if !errors.Is(err, ErrNoTopic) || !IsPermanent(err) {
			t.Fatalf("err = %v, want permanent ErrNoTopic", err)
		}
	})

	t.Run("resolver returning empty is a permanent error", func(t *testing.T) {
		r, _ := newTestRelay(t, &fakeStore{}, &fakePublisher{},
			WithTopicResolver(func(*pgoutbox.Event) string { return "" }))
		err := r.resolveTopic(&pgoutbox.Event{})
		if !errors.Is(err, ErrNoTopic) || !IsPermanent(err) {
			t.Fatalf("err = %v, want permanent ErrNoTopic", err)
		}
	})

	t.Run("resolver fills the topic", func(t *testing.T) {
		r, _ := newTestRelay(t, &fakeStore{}, &fakePublisher{},
			WithTopicResolver(func(e *pgoutbox.Event) string { return "resolved." + e.Type }))
		e := &pgoutbox.Event{Type: "x"}
		if err := r.resolveTopic(e); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if e.Topic != "resolved.x" {
			t.Errorf("Topic = %q, want resolved.x", e.Topic)
		}
	})
}

func TestBackoffFor(t *testing.T) {
	t.Run("falls back to DefaultBackoff when nil", func(t *testing.T) {
		r, _ := newTestRelay(t, &fakeStore{}, &fakePublisher{})
		r.cfg.backoff = nil
		d := r.backoffFor(1)
		if d < time.Second/2 || d >= time.Second+1 {
			t.Errorf("backoff %v not in attempt-1 band", d)
		}
	})

	t.Run("uses the configured backoff", func(t *testing.T) {
		r, _ := newTestRelay(t, &fakeStore{}, &fakePublisher{},
			WithBackoff(func(attempt int) time.Duration { return time.Duration(attempt) * time.Minute }))
		if got := r.backoffFor(3); got != 3*time.Minute {
			t.Errorf("backoffFor(3) = %v, want 3m", got)
		}
	})
}

func TestExtractEventIDs(t *testing.T) {
	e1, e2 := newEvent(), newEvent()
	ids := extractEventIDs([]*pgoutbox.Event{e1, e2})
	if len(ids) != 2 || ids[0] != e1.ID || ids[1] != e2.ID {
		t.Errorf("extractEventIDs = %v, want [%v %v]", ids, e1.ID, e2.ID)
	}
}

func TestSubtractIDs(t *testing.T) {
	a, b, c := newID(), newID(), newID()

	t.Run("returns ids in want but not in got", func(t *testing.T) {
		missing := subtractIDs([]pgtype.UUID{a, b, c}, []pgtype.UUID{b})
		if len(missing) != 2 {
			t.Fatalf("missing = %v, want 2 elements", missing)
		}
		set := map[pgtype.UUID]bool{missing[0]: true, missing[1]: true}
		if !set[a] || !set[c] {
			t.Errorf("missing = %v, want a and c", missing)
		}
	})

	t.Run("nothing missing yields nil", func(t *testing.T) {
		if got := subtractIDs([]pgtype.UUID{a, b}, []pgtype.UUID{a, b}); got != nil {
			t.Errorf("subtractIDs = %v, want nil", got)
		}
	})

	t.Run("empty got yields all want", func(t *testing.T) {
		if got := subtractIDs([]pgtype.UUID{a}, nil); len(got) != 1 || got[0] != a {
			t.Errorf("subtractIDs = %v, want [%v]", got, a)
		}
	})
}
