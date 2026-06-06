package relay

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/from-cero/pgoutbox"
)

// pairs an e with the error that prevented it from being published.
type failure struct {
	e     *pgoutbox.Event
	cause error
}

func (r *Relay) runPoller(ctx context.Context, q pgoutbox.Querier, wake <-chan struct{}) {
	ticker := time.NewTicker(r.cfg.poller.interval)
	defer ticker.Stop()
	for {
		r.drainPendingEvents(ctx, q)

		// wait for the configured poller interval,
		// a wake signal from the listener, or context cancellation
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-wake:
		}
	}
}

func (r *Relay) drainPendingEvents(ctx context.Context, q pgoutbox.Querier) {
	for {
		count, err := r.processPendingEventsBatch(ctx, q)
		if err != nil {
			r.log.ErrorContext(ctx, "run poller batch", "error", err.Error())
			return
		}
		if count < r.cfg.poller.batchSize || ctx.Err() != nil {
			return
		}
	}
}

// returns the number of events processed (claimed, published, failed, or permanently failed) in this batch
func (r *Relay) processPendingEventsBatch(ctx context.Context, q pgoutbox.Querier) (int, error) {
	events, err := r.s.FetchPending(ctx, q, r.cfg.poller.batchSize)
	if err != nil {
		return 0, fmt.Errorf("fetch pending events: %w", err)
	}
	if len(events) == 0 {
		return 0, nil
	}

	stats := BatchStats{Claimed: len(events)}

	publishable := make([]*pgoutbox.Event, 0, len(events))
	var failures []failure
	// filter and prepare events for publishing, separating out failures which are
	// permanent failures and should not be published or retried (e.g. topic resolution failure)
	for _, e := range events {
		if err := r.resolveTopic(e); err != nil {
			failures = append(failures, failure{e: e, cause: fmt.Errorf("resolve topic: %w", err)})
			continue
		}
		publishable = append(publishable, e)
	}

	// publish publishable events and collect publish failures
	published := make([]*pgoutbox.Event, 0, len(publishable))
	if len(publishable) > 0 {
		start := time.Now()
		results := r.pub.PublishBatch(ctx, publishable)
		stats.PublishDuration = time.Since(start)
		if stats.PublishDuration > r.cfg.reaper.stuckTimeout/2 {
			r.log.WarnContext(
				ctx,
				"publishing batch took unusually long, please consider that the reaper may reschedule in-flight events",
				"elapsed", stats.PublishDuration, "stuck_timeout", r.cfg.reaper.stuckTimeout,
			)
		}
		for i, e := range publishable {
			cause := errMisalignedResults
			if i < len(results) {
				cause = results[i]
			}
			if cause != nil {
				failures = append(failures, failure{e: e, cause: fmt.Errorf("publish event: %w", cause)})
				continue
			}
			published = append(published, e)
		}
	}
	stats.Published = len(published)

	if ctx.Err() != nil {
		// shutting down mid-batch: still record successful publishes,
		// but return everything else to pending without burning retry budget

		// the poller's context is already canceled,
		// so the bookkeeping runs on a short detached context.
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.cfg.poller.shutdownGrace)
		defer cancel()
		stats.Lost = r.markProcessed(dctx, q, published)
		stats.Unclaimed = r.unclaimEvents(dctx, q, failures)
		r.cfg.hooks.batch(stats)
		return len(events), nil
	}

	stats.Failed = len(failures)
	var transient, permanent []failure
	for _, f := range failures {
		if IsPermanent(f.cause) {
			permanent = append(permanent, f)
		} else {
			transient = append(transient, f)
		}
	}
	stats.Permanent = len(permanent)
	stats.Lost = r.markProcessed(ctx, q, published)
	stats.Lost += r.markFailed(ctx, q, transient)
	stats.Lost += r.failPermanently(ctx, q, permanent)
	r.cfg.hooks.batch(stats)
	return len(events), nil
}

// returns how many were lost to the claim fence (reaped or re-claimed mid-publish)
func (r *Relay) markProcessed(ctx context.Context, q pgoutbox.Querier, events []*pgoutbox.Event) int {
	if len(events) == 0 {
		return 0
	}
	updatedIDs, err := r.s.MarkProcessed(ctx, q, events)
	if err != nil {
		r.log.ErrorContext(
			ctx, "mark events as processed", "events", extractEventIDs(events), "error", err.Error(),
		)
		return 0
	}
	missingIDs := subtractIDs(extractEventIDs(events), updatedIDs)
	if len(missingIDs) > 0 {
		r.log.WarnContext(
			ctx, "published events were no longer claimed by this worker, duplicate delivery is possible",
			"events", missingIDs,
		)
	}
	return len(missingIDs)
}

// returns how many were lost to the claim fence
func (r *Relay) markFailed(ctx context.Context, q pgoutbox.Querier, failures []failure) int {
	if len(failures) == 0 {
		return 0
	}
	events := make([]*pgoutbox.Event, len(failures))
	backoffs := make([]time.Duration, len(failures))
	for i, f := range failures {
		events[i] = f.e
		backoffs[i] = r.backoffFor(f.e.AttemptCount + 1)
	}
	updatedIDs, err := r.s.MarkFailed(ctx, q, events, backoffs)
	if err != nil {
		r.log.ErrorContext(
			ctx, "mark events as failed", "events", extractEventIDs(events), "error", err.Error(),
		)
		return 0
	}
	updatedSet := make(map[pgtype.UUID]struct{}, len(updatedIDs))
	for _, id := range updatedIDs {
		updatedSet[id] = struct{}{}
	}
	lost := 0
	for _, f := range failures {
		if _, ok := updatedSet[f.e.ID]; !ok {
			lost++
			r.log.WarnContext(
				ctx, "failed event was no longer claimed by this worker, skipping retry accounting",
				"event", f.e.ID, "cause", f.cause,
			)
			continue
		}
		attempt := f.e.AttemptCount + 1
		if attempt >= f.e.MaxAttempts {
			r.log.ErrorContext(
				ctx, "event has reached max attempts and will be marked as failed",
				"event", f.e.ID, "attempt", attempt, "cause", f.cause,
			)
			continue
		}
		r.log.WarnContext(
			ctx, "publish failed, will retry", "event", f.e.ID, "attempt", attempt, "cause", f.cause,
		)
	}
	return lost
}

// parks events whose failure a retry cannot fix and returns how many were lost to the claim fence
func (r *Relay) failPermanently(ctx context.Context, q pgoutbox.Querier, failures []failure) int {
	if len(failures) == 0 {
		return 0
	}
	events := make([]*pgoutbox.Event, len(failures))
	for i, f := range failures {
		events[i] = f.e
	}
	updatedIDs, err := r.s.Fail(ctx, q, events)
	if err != nil {
		r.log.ErrorContext(
			ctx, "mark events as permanently failed", "events", extractEventIDs(events), "error", err.Error(),
		)
		return 0
	}
	updatedSet := make(map[pgtype.UUID]struct{}, len(updatedIDs))
	for _, id := range updatedIDs {
		updatedSet[id] = struct{}{}
	}
	lost := 0
	for _, f := range failures {
		if _, ok := updatedSet[f.e.ID]; !ok {
			lost++
			r.log.WarnContext(
				ctx, "permanently failed event was no longer claimed by this worker",
				"event", f.e.ID, "cause", f.cause,
			)
			continue
		}
		r.log.ErrorContext(
			ctx, "event failed permanently and will not be retried", "event", f.e.ID, "cause", f.cause,
		)
	}
	return lost
}

// unclaim claimed-but-unpublished events to pending on shutdown
func (r *Relay) unclaimEvents(ctx context.Context, q pgoutbox.Querier, failures []failure) int {
	if len(failures) == 0 {
		return 0
	}
	events := make([]*pgoutbox.Event, len(failures))
	for i, f := range failures {
		events[i] = f.e
	}
	updated, err := r.s.Unclaim(ctx, q, events)
	if err != nil {
		r.log.ErrorContext(
			ctx, "unclaim events on shutdown",
			"events", extractEventIDs(events), "error", err.Error(),
		)
		return 0
	}
	r.log.InfoContext(ctx, "returned claimed events to pending on shutdown", "count", len(updated))
	return len(updated)
}

// fills e.Topic; failures are Permanent because a missing topic
// is a configuration error that retrying with backoff cannot fix.
func (r *Relay) resolveTopic(e *pgoutbox.Event) error {
	if e.Topic != "" {
		return nil
	}
	if r.cfg.topicResolver == nil {
		return Permanent(fmt.Errorf("resolve topic for event %s: %w", e.ID, ErrNoTopic))
	}
	topic := r.cfg.topicResolver(e)
	if topic == "" {
		return Permanent(fmt.Errorf("resolve topic for event %s: %w", e.ID, ErrNoTopic))
	}
	e.Topic = topic
	return nil
}

func (r *Relay) backoffFor(attempt int) time.Duration {
	if r.cfg.backoff == nil {
		return DefaultBackoff(attempt)
	}
	return r.cfg.backoff(attempt)
}

func extractEventIDs(events []*pgoutbox.Event) []pgtype.UUID {
	ids := make([]pgtype.UUID, len(events))
	for i, e := range events {
		ids[i] = e.ID
	}
	return ids
}

func subtractIDs(want, got []pgtype.UUID) []pgtype.UUID {
	gotSet := make(map[pgtype.UUID]struct{}, len(got))
	for _, id := range got {
		gotSet[id] = struct{}{}
	}
	var missingIDs []pgtype.UUID
	for _, id := range want {
		if _, ok := gotSet[id]; !ok {
			missingIDs = append(missingIDs, id)
		}
	}
	return missingIDs
}
