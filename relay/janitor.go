package relay

import (
	"context"
	"time"

	"github.com/from-cero/pgoutbox"
)

func (r *Relay) runJanitor(ctx context.Context, q pgoutbox.Querier) {
	ticker := time.NewTicker(r.cfg.janitor.interval)
	defer ticker.Stop()
	for {
		r.deleteProcessedEvents(ctx, q)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Relay) deleteProcessedEvents(ctx context.Context, q pgoutbox.Querier) {
	var total int64
	for {
		deleted, err := r.s.DeleteProcessed(ctx, q, r.cfg.janitor.retention, r.cfg.janitor.batchSize)
		if err != nil {
			r.log.ErrorContext(ctx, "run janitor batch", "error", err.Error())
			return
		}
		total += deleted
		if deleted < int64(r.cfg.janitor.batchSize) || ctx.Err() != nil {
			break
		}
	}
	if total > 0 {
		r.log.InfoContext(ctx, "deleted processed events past retention", "count", total)
		r.cfg.hooks.sweep(SweepStats{Deleted: total})
	}
}
