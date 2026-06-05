package relay

import (
	"time"
)

// Hooks carries optional observability callbacks; any nil field is skipped.
// Callbacks run synchronously on relay goroutines, so keep them cheap (count,
// observe a histogram) and never block. A struct of funcs rather than an
// interface so new hooks can be added without breaking implementers.
type Hooks struct {
	OnBatch func(BatchStats) // after every poll batch
	OnReap  func(ReapStats)  // after every reap batch that touched events
	OnSweep func(SweepStats) // after every janitor sweep that deleted events
}

// BatchStats describes the outcome of one poll batch.
type BatchStats struct {
	Claimed         int           // events claimed from the store
	Published       int           // events acked by the broker
	Failed          int           // events that failed topic resolution or publish
	Permanent       int           // subset of Failed parked immediately as not retryable
	Unclaimed       int           // events returned to pending on shutdown
	Lost            int           // events lost to the claim fence (reaped or re-claimed mid-publish)
	PublishDuration time.Duration // wall time of the PublishBatch call
}

// ReapStats describes the outcome of one reap batch.
type ReapStats struct {
	Rescheduled int // stuck events returned to pending
	Failed      int // stuck events that exhausted their reap budget
}

// SweepStats describes the outcome of one janitor sweep.
type SweepStats struct {
	Deleted int64 // processed events removed
}

func (h Hooks) batch(s BatchStats) {
	if h.OnBatch != nil {
		h.OnBatch(s)
	}
}

func (h Hooks) reap(s ReapStats) {
	if h.OnReap != nil {
		h.OnReap(s)
	}
}

func (h Hooks) sweep(s SweepStats) {
	if h.OnSweep != nil {
		h.OnSweep(s)
	}
}
