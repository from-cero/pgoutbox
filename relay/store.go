package relay

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/from-cero/pgoutbox"
)

// Store defines the relay interface for interacting with the outbox table.
type Store interface {
	// FetchPending returns a batch of pending events, claiming them for processing.
	FetchPending(ctx context.Context, q pgoutbox.Querier, batchSize int) ([]*pgoutbox.Event, error)

	// MarkProcessed marks events as processed. Returns the IDs that were actually updated.
	MarkProcessed(ctx context.Context, q pgoutbox.Querier, es []*pgoutbox.Event) ([]uuid.UUID, error)

	// MarkFailed decrements remaining attempts and reschedules with the given backoffs,
	// or transitions to failed if the retry budget is exhausted.
	MarkFailed(ctx context.Context, q pgoutbox.Querier, es []*pgoutbox.Event, backoffs []time.Duration) (
		[]uuid.UUID, error,
	)

	// Fail marks events as failed without a backoff.
	Fail(ctx context.Context, q pgoutbox.Querier, es []*pgoutbox.Event) ([]uuid.UUID, error)

	// Unclaim returns claimed events to pending, making them available for other pollers.
	Unclaim(ctx context.Context, q pgoutbox.Querier, es []*pgoutbox.Event) ([]uuid.UUID, error)

	// ReapStuck identifies events that have been claimed for processing
	// but have not been marked as processed or failed within the specified stuckTimeout.
	ReapStuck(ctx context.Context, q pgoutbox.Querier, stuckTimeout, backoff time.Duration, maxReaps, batchSize int) (
		[]*pgoutbox.Event, error,
	)

	// DeleteProcessed removes events that were marked as processed more than the specified retention period.
	DeleteProcessed(ctx context.Context, q pgoutbox.Querier, olderThan time.Duration, batchSize int) (int64, error)

	// RequeueFailed resets up to limit failed events back to pending so they can be retried.
	RequeueFailed(ctx context.Context, q pgoutbox.Querier, limit int) (int64, error)
}
