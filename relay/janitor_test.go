package relay

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDeleteProcessedEvents(t *testing.T) {
	ctx := context.Background()

	t.Run("loops over full batches and reports the total", func(t *testing.T) {
		var swept SweepStats
		// batchSize 10: a full batch (10) continues, a short batch (4) stops.
		s := &fakeStore{deleteCounts: []int64{10, 4}}
		r, err := New(s, &fakePublisher{}, discardLogger(),
			WithRetention(time.Hour), WithJanitorBatchSize(10),
			WithHooks(Hooks{OnSweep: func(ss SweepStats) { swept = ss }}))
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}

		r.deleteProcessedEvents(ctx, nil)

		if s.deleteCalls != 2 {
			t.Errorf("DeleteProcessed called %d times, want 2", s.deleteCalls)
		}
		if swept.Deleted != 14 {
			t.Errorf("swept.Deleted = %d, want 14", swept.Deleted)
		}
	})

	t.Run("does not fire the hook when nothing was deleted", func(t *testing.T) {
		var fired bool
		s := &fakeStore{deleteCounts: []int64{0}}
		r, _ := New(s, &fakePublisher{}, discardLogger(),
			WithRetention(time.Hour), WithJanitorBatchSize(10),
			WithHooks(Hooks{OnSweep: func(SweepStats) { fired = true }}))

		r.deleteProcessedEvents(ctx, nil)

		if fired {
			t.Error("sweep hook should not fire when nothing was deleted")
		}
	})

	t.Run("stops and logs on store error", func(t *testing.T) {
		s := &fakeStore{deleteErr: errors.New("delete boom")}
		r, _ := New(s, &fakePublisher{}, discardLogger(),
			WithRetention(time.Hour), WithJanitorBatchSize(10))

		r.deleteProcessedEvents(ctx, nil) // must not panic; just returns
	})
}
