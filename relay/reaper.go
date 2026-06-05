package relay

import (
	"context"
	"time"

	"github.com/from-cero/pgoutbox"
)

func (r *Relay) runReaper(ctx context.Context, q pgoutbox.Querier) {
	ticker := time.NewTicker(r.cfg.reaper.interval)
	defer ticker.Stop()
	for {
		r.reapStuckEvents(ctx, q)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Relay) reapStuckEvents(ctx context.Context, q pgoutbox.Querier) {
	for {
		count, err := r.reapBatchStuckEvents(ctx, q)
		if err != nil {
			r.log.ErrorContext(ctx, "run reaper batch", "error", err.Error())
			return
		}
		if count < r.cfg.reaper.batchSize || ctx.Err() != nil {
			return
		}
	}
}

// returns the number of events reaped in this batch
func (r *Relay) reapBatchStuckEvents(ctx context.Context, q pgoutbox.Querier) (int, error) {
	reaped, err := r.s.ReapStuck(
		ctx, q,
		r.cfg.reaper.stuckTimeout, r.backoffFor(1),
		r.cfg.reaper.maxReaps, r.cfg.reaper.batchSize,
	)
	if err != nil {
		return 0, err
	}

	var stats ReapStats
	for _, e := range reaped {
		if e.Status == pgoutbox.EventFailed {
			stats.Failed++
			r.log.ErrorContext(
				ctx, "stuck event exhausted its reap budget and was marked as failed",
				"event", e.ID, "reaps", e.ReapCount,
			)
			continue
		}
		stats.Rescheduled++
		r.log.WarnContext(
			ctx, "stuck event rescheduled for retry", "event", e.ID, "reaps", e.ReapCount,
		)
	}
	if len(reaped) > 0 {
		r.cfg.hooks.reap(stats)
	}
	return len(reaped), nil
}
