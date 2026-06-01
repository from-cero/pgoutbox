package relay

import (
	"context"
	"time"

	"github.com/from-cero/pgoutbox"
)

type Store interface {
	FetchPending(ctx context.Context, batchSize int) ([]*pgoutbox.Event, error)
	MaskAsProcessed(ctx context.Context, id int64) error
	MaskAsFailed(ctx context.Context, id int64, nextScheduledAt time.Duration) error
	FetchStuck(ctx context.Context, stuckTimeout time.Duration, batchSize int) ([]*pgoutbox.Event, error)
	ResetStuck(ctx context.Context, id int64) error
}
