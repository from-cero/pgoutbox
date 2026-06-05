package relay

import (
	"context"
	"errors"
	"testing"

	"github.com/from-cero/pgoutbox"
)

func reapedEvent(status string) *pgoutbox.Event {
	e := newEvent()
	e.Status = status
	return e
}

func TestReapBatchStuckEvents(t *testing.T) {
	ctx := context.Background()

	t.Run("counts rescheduled and failed separately", func(t *testing.T) {
		var reap ReapStats
		s := &fakeStore{reapBatches: [][]*pgoutbox.Event{{
			reapedEvent(pgoutbox.EventPending), // rescheduled
			reapedEvent(pgoutbox.EventPending),
			reapedEvent(pgoutbox.EventFailed), // exhausted reap budget
		}}}
		r, err := New(s, &fakePublisher{}, discardLogger(),
			WithHooks(Hooks{OnReap: func(rs ReapStats) { reap = rs }}))
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}

		n, err := r.reapBatchStuckEvents(ctx, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if n != 3 {
			t.Errorf("reaped = %d, want 3", n)
		}
		if reap.Rescheduled != 2 || reap.Failed != 1 {
			t.Errorf("reap stats = %+v, want Rescheduled=2 Failed=1", reap)
		}
	})

	t.Run("no stuck events does not fire the hook", func(t *testing.T) {
		var fired bool
		s := &fakeStore{}
		r, _ := New(s, &fakePublisher{}, discardLogger(),
			WithHooks(Hooks{OnReap: func(ReapStats) { fired = true }}))

		n, err := r.reapBatchStuckEvents(ctx, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if n != 0 {
			t.Errorf("reaped = %d, want 0", n)
		}
		if fired {
			t.Error("reap hook should not fire when nothing was reaped")
		}
	})

	t.Run("propagates store errors", func(t *testing.T) {
		sentinel := errors.New("reap boom")
		s := &fakeStore{reapErr: sentinel}
		r, _ := New(s, &fakePublisher{}, discardLogger())
		if _, err := r.reapBatchStuckEvents(ctx, nil); !errors.Is(err, sentinel) {
			t.Fatalf("err = %v, want %v", err, sentinel)
		}
	})
}

func TestReapStuckEventsDrainsBatches(t *testing.T) {
	// A full batch (== batchSize) triggers another round; a short batch stops the loop.
	full := []*pgoutbox.Event{
		reapedEvent(pgoutbox.EventPending),
		reapedEvent(pgoutbox.EventPending),
	}
	short := []*pgoutbox.Event{reapedEvent(pgoutbox.EventPending)}
	s := &fakeStore{reapBatches: [][]*pgoutbox.Event{full, short}}
	r, _ := New(s, &fakePublisher{}, discardLogger(), WithReaperBatchSize(2))

	r.reapStuckEvents(context.Background(), nil)

	if s.reapCalls != 2 {
		t.Errorf("ReapStuck called %d times, want 2", s.reapCalls)
	}
}
