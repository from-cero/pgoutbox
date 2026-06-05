package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/from-cero/pgoutbox"
)

// Stats provides a snapshot of the outbox table, useful for monitoring and alerting.
type Stats struct {
	Pending          int64         // count of events waiting to be picked up
	Processing       int64         // count of events currently claimed by a poller
	Failed           int64         // count of events that exhausted all retries
	OldestPendingAge time.Duration // lag indicator for pending events
}

// Stats runs a single query that returns a snapshot of the outbox table counters.
func (s *Store) Stats(ctx context.Context, q pgoutbox.Querier) (*Stats, error) {
	var (
		st         Stats
		ageSeconds float64
	)
	sql := fmt.Sprintf(
		`
		SELECT count(*) FILTER (WHERE status = 'pending'),
			   count(*) FILTER (WHERE status = 'processing'),
			   count(*) FILTER (WHERE status = 'failed'),
			   coalesce(extract(epoch FROM now() - min(scheduled_at)
								FILTER (WHERE status = 'pending' AND scheduled_at <= now())), 0)
		FROM %[1]s`, s.table,
	)
	err := q.QueryRow(ctx, sql).Scan(&st.Pending, &st.Processing, &st.Failed, &ageSeconds)
	if err != nil {
		return nil, fmt.Errorf("query stats: %w", err)
	}
	st.OldestPendingAge = time.Duration(ageSeconds * float64(time.Second))
	return &st, nil
}
