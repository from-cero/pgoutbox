package relay

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/from-cero/pgoutbox"
)

func failuresFor(events ...*pgoutbox.Event) []failure {
	fs := make([]failure, len(events))
	for i, e := range events {
		fs[i] = failure{e: e, cause: errors.New("publish failed")}
	}
	return fs
}

func TestMarkProcessed(t *testing.T) {
	ctx := context.Background()

	t.Run("empty input is a no-op", func(t *testing.T) {
		r, _ := newTestRelay(t, &fakeStore{}, &fakePublisher{})
		if lost := r.markProcessed(ctx, nil, nil); lost != 0 {
			t.Errorf("lost = %d, want 0", lost)
		}
	})

	t.Run("store error returns zero lost", func(t *testing.T) {
		s := &fakeStore{markProcessedErr: errors.New("db down")}
		r, _ := newTestRelay(t, s, &fakePublisher{})
		if lost := r.markProcessed(ctx, nil, []*pgoutbox.Event{newEvent()}); lost != 0 {
			t.Errorf("lost = %d, want 0", lost)
		}
	})

	t.Run("events not updated count as lost", func(t *testing.T) {
		e1, e2 := newEvent(), newEvent()
		s := &fakeStore{markProcessedIDs: []pgtype.UUID{e1.ID}} // e2 missing
		r, _ := newTestRelay(t, s, &fakePublisher{})
		if lost := r.markProcessed(ctx, nil, []*pgoutbox.Event{e1, e2}); lost != 1 {
			t.Errorf("lost = %d, want 1", lost)
		}
	})
}

func TestMarkFailed(t *testing.T) {
	ctx := context.Background()

	t.Run("empty input is a no-op", func(t *testing.T) {
		r, _ := newTestRelay(t, &fakeStore{}, &fakePublisher{})
		if lost := r.markFailed(ctx, nil, nil); lost != 0 {
			t.Errorf("lost = %d, want 0", lost)
		}
	})

	t.Run("store error returns zero lost", func(t *testing.T) {
		s := &fakeStore{markFailedErr: errors.New("db down")}
		r, _ := newTestRelay(t, s, &fakePublisher{})
		if lost := r.markFailed(ctx, nil, failuresFor(newEvent())); lost != 0 {
			t.Errorf("lost = %d, want 0", lost)
		}
	})

	t.Run("distinguishes retry, max-attempts and lost events", func(t *testing.T) {
		retry := newEvent() // attempt 1 < 3: will retry
		retry.AttemptCount = 0
		exhausted := newEvent() // attempt 3 >= 3: reached max attempts
		exhausted.AttemptCount = 2
		lost := newEvent() // not in updated set: lost to the claim fence

		s := &fakeStore{markFailedIDs: []pgtype.UUID{retry.ID, exhausted.ID}}
		r, _ := newTestRelay(t, s, &fakePublisher{})

		got := r.markFailed(ctx, nil, failuresFor(retry, exhausted, lost))
		if got != 1 {
			t.Errorf("lost = %d, want 1", got)
		}
	})
}

func TestFailPermanently(t *testing.T) {
	ctx := context.Background()

	t.Run("empty input is a no-op", func(t *testing.T) {
		r, _ := newTestRelay(t, &fakeStore{}, &fakePublisher{})
		if lost := r.failPermanently(ctx, nil, nil); lost != 0 {
			t.Errorf("lost = %d, want 0", lost)
		}
	})

	t.Run("store error returns zero lost", func(t *testing.T) {
		s := &fakeStore{failErr: errors.New("db down")}
		r, _ := newTestRelay(t, s, &fakePublisher{})
		if lost := r.failPermanently(ctx, nil, failuresFor(newEvent())); lost != 0 {
			t.Errorf("lost = %d, want 0", lost)
		}
	})

	t.Run("events not updated count as lost", func(t *testing.T) {
		parked, lost := newEvent(), newEvent()
		s := &fakeStore{failIDs: []pgtype.UUID{parked.ID}} // lost missing
		r, _ := newTestRelay(t, s, &fakePublisher{})
		if got := r.failPermanently(ctx, nil, failuresFor(parked, lost)); got != 1 {
			t.Errorf("lost = %d, want 1", got)
		}
	})
}

func TestUnclaimEvents(t *testing.T) {
	ctx := context.Background()

	t.Run("empty input is a no-op", func(t *testing.T) {
		r, _ := newTestRelay(t, &fakeStore{}, &fakePublisher{})
		if n := r.unclaimEvents(ctx, nil, nil); n != 0 {
			t.Errorf("unclaimed = %d, want 0", n)
		}
	})

	t.Run("store error returns zero", func(t *testing.T) {
		s := &fakeStore{unclaimErr: errors.New("db down")}
		r, _ := newTestRelay(t, s, &fakePublisher{})
		if n := r.unclaimEvents(ctx, nil, failuresFor(newEvent())); n != 0 {
			t.Errorf("unclaimed = %d, want 0", n)
		}
	})

	t.Run("returns the number unclaimed by the store", func(t *testing.T) {
		e1, e2 := newEvent(), newEvent()
		s := &fakeStore{unclaimIDs: []pgtype.UUID{e1.ID, e2.ID}}
		r, _ := newTestRelay(t, s, &fakePublisher{})
		if n := r.unclaimEvents(ctx, nil, failuresFor(e1, e2)); n != 2 {
			t.Errorf("unclaimed = %d, want 2", n)
		}
	})
}

func TestDrainPendingEvents(t *testing.T) {
	t.Run("loops over a full batch then stops on a short one", func(t *testing.T) {
		full := []*pgoutbox.Event{newEvent(), newEvent()}
		short := []*pgoutbox.Event{newEvent()}
		s := &fakeStore{fetchBatches: [][]*pgoutbox.Event{full, short}}
		r, _ := newTestRelay(t, s, &fakePublisher{}, WithPollerBatchSize(2))

		r.drainPendingEvents(context.Background(), nil)

		if s.fetchCalls != 2 {
			t.Errorf("FetchPending called %d times, want 2", s.fetchCalls)
		}
	})

	t.Run("stops on a fetch error", func(t *testing.T) {
		s := &fakeStore{fetchErr: errors.New("fetch boom")}
		r, _ := newTestRelay(t, s, &fakePublisher{})
		r.drainPendingEvents(context.Background(), nil) // must not panic
	})
}
