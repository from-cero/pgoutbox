package relay

import "testing"

func TestHooksNilSafety(t *testing.T) {
	// A zero Hooks has all-nil callbacks; invoking them must not panic.
	var h Hooks
	h.batch(BatchStats{})
	h.reap(ReapStats{})
	h.sweep(SweepStats{})
}

func TestHooksInvoked(t *testing.T) {
	var (
		gotBatch BatchStats
		gotReap  ReapStats
		gotSweep SweepStats
	)
	h := Hooks{
		OnBatch: func(s BatchStats) { gotBatch = s },
		OnReap:  func(s ReapStats) { gotReap = s },
		OnSweep: func(s SweepStats) { gotSweep = s },
	}

	h.batch(BatchStats{Claimed: 3, Published: 2})
	h.reap(ReapStats{Rescheduled: 1, Failed: 4})
	h.sweep(SweepStats{Deleted: 9})

	if gotBatch.Claimed != 3 || gotBatch.Published != 2 {
		t.Errorf("OnBatch got %+v", gotBatch)
	}
	if gotReap.Rescheduled != 1 || gotReap.Failed != 4 {
		t.Errorf("OnReap got %+v", gotReap)
	}
	if gotSweep.Deleted != 9 {
		t.Errorf("OnSweep got %+v", gotSweep)
	}
}
