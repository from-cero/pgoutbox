package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/from-cero/pgoutbox"
)

// Store provides methods to interact with the outbox table in PostgreSQL.
//
// Store requires PostgreSQL 18 or newer. EnsureSchema defaults the event id to
// uuidv7(), a built-in first added in PostgreSQL 18.
type Store struct {
	table string
}

// NewStore returns a new Store that operates on the specified table name.
func NewStore(table string) *Store {
	return &Store{table: table}
}

// EnsureSchema creates the outbox table, indexes and trigger if they don't exist.
//
// Requires PostgreSQL 18 or newer: the id column defaults to uuidv7(), a
// built-in first added in PostgreSQL 18. On older servers this call fails with
// "function uuidv7() does not exist".
func (s *Store) EnsureSchema(ctx context.Context, q pgoutbox.Querier) error {
	sql := fmt.Sprintf(
		`
		CREATE TABLE IF NOT EXISTS %[1]s
		(
			id            UUID        NOT NULL DEFAULT uuidv7() PRIMARY KEY,
			type          TEXT        NOT NULL,
			topic         TEXT        NOT NULL DEFAULT '',
			payload       JSON        NOT NULL,
			status        TEXT        NOT NULL,
			attempt_count INTEGER     NOT NULL DEFAULT 0,
			max_attempts  INTEGER     NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
			reap_count    INTEGER     NOT NULL DEFAULT 0,
			scheduled_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
			created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
		);

		-- Speeds up the relay poller scan: "pending events that are due, oldest
		-- first". id breaks timestamp ties for deterministic dispatch order.
		CREATE INDEX IF NOT EXISTS idx_%[1]s_pending
			ON %[1]s (scheduled_at, id)
			WHERE status = 'pending';

		-- Speeds up the relay reaper scan: "events stuck in processing the
		-- longest". Events claimed in one batch share updated_at, so id
		-- breaks the tie.
		CREATE INDEX IF NOT EXISTS idx_%[1]s_processing
			ON %[1]s (updated_at, id)
			WHERE status = 'processing';

		-- Speeds up the janitor scan: "processed events past retention, oldest first".
		CREATE INDEX IF NOT EXISTS idx_%[1]s_processed
			ON %[1]s (updated_at, id)
			WHERE status = 'processed';

		-- Supports ops queries and RequeueFailed: "failed events, oldest first".
		CREATE INDEX IF NOT EXISTS idx_%[1]s_failed
			ON %[1]s (updated_at, id)
			WHERE status = 'failed';

		-- Wakes relay listeners on enqueue. NOTIFY fires on commit only,
		-- which matches outbox semantics exactly. The channel is the table
		-- name; see NewListener.
		CREATE OR REPLACE FUNCTION %[1]s_notify() RETURNS trigger
			LANGUAGE plpgsql AS
		$notify$
		BEGIN
			PERFORM pg_notify('%[1]s', '');
			RETURN NULL;
		END
		$notify$;

		CREATE OR REPLACE TRIGGER %[1]s_notify_on_insert
			AFTER INSERT
			ON %[1]s
			FOR EACH STATEMENT
		EXECUTE FUNCTION %[1]s_notify();`, s.table,
	)
	_, err := q.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("setup table, index, and trigger: %w", err)
	}
	return nil
}

// Insert stores the event. The database assigns the id (a v7 UUID) and the
// generated id along with the server-managed columns are scanned back into e.
// Topic, payload, status, max_attempts and scheduled_at are required; other fields are ignored.
func (s *Store) Insert(ctx context.Context, q pgoutbox.Querier, e *pgoutbox.Event) error {
	if !json.Valid(e.Payload) {
		return ErrInvalidPayload
	}
	sql := fmt.Sprintf(
		`
		INSERT INTO %[1]s (type, topic, payload, status, max_attempts, scheduled_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, attempt_count, reap_count, created_at, updated_at`, s.table,
	)
	err := q.QueryRow(
		ctx, sql, e.Type, e.Topic, e.Payload,
		e.Status, e.MaxAttempts, e.ScheduledAt,
	).Scan(&e.ID, &e.AttemptCount, &e.ReapCount, &e.CreatedAt, &e.UpdatedAt)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}
	return nil
}

// FetchPending claims up to batchSize pending events that are due (scheduled_at <= now),
// transitions them to processing and returns them. SELECT FOR UPDATE SKIP LOCKED allows
// multiple workers to call it concurrently without stepping on each other.
func (s *Store) FetchPending(ctx context.Context, q pgoutbox.Querier, batchSize int) ([]*pgoutbox.Event, error) {
	sql := fmt.Sprintf(
		`
		WITH claimed AS (SELECT id FROM %[1]s
						 WHERE status = 'pending' AND scheduled_at <= now()
						 ORDER BY scheduled_at, id
						 FOR UPDATE SKIP LOCKED LIMIT $1)
		UPDATE %[1]s AS o
		SET status = 'processing', updated_at = now()
		FROM claimed WHERE o.id = claimed.id
		RETURNING o.id, o.type, o.topic, o.payload,
			o.status, o.attempt_count, o.max_attempts, o.reap_count,
			o.scheduled_at, o.created_at, o.updated_at`, s.table,
	)
	rows, err := q.Query(ctx, sql, batchSize)
	if err != nil {
		return nil, fmt.Errorf("fetch and claim pending events: %w", err)
	}
	events, err := pgx.CollectRows(rows, collectEvent)
	if err != nil {
		return nil, fmt.Errorf("collect claimed events: %w", err)
	}
	return events, nil
}

// MarkProcessed marks events as processed.
func (s *Store) MarkProcessed(ctx context.Context, q pgoutbox.Querier, e []*pgoutbox.Event) ([]uuid.UUID, error) {
	sql := fmt.Sprintf(
		`
		UPDATE %[1]s AS o
		SET status = 'processed', updated_at = now()
		FROM unnest($1::uuid[], $2::timestamptz[]) AS u(id, claimed_at)
		WHERE o.id = u.id AND o.status = 'processing' AND o.updated_at = u.claimed_at
		RETURNING o.id`, s.table,
	)
	ids, claimedAts := splitClaims(e)
	rows, err := q.Query(ctx, sql, ids, claimedAts)
	if err != nil {
		return nil, fmt.Errorf("mark events as processed: %w", err)
	}
	updatedIDs, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		return nil, fmt.Errorf("collect processed event ids: %w", err)
	}
	return updatedIDs, nil
}

// MarkFailed marks events as failed and schedules retries according to the provided backoff durations.
func (s *Store) MarkFailed(
	ctx context.Context, q pgoutbox.Querier, e []*pgoutbox.Event, backoffs []time.Duration,
) ([]uuid.UUID, error) {
	if len(e) != len(backoffs) {
		return nil, ErrLengthMismatch
	}
	sql := fmt.Sprintf(
		`
		UPDATE %[1]s AS o
		SET status = CASE WHEN o.attempt_count + 1 < o.max_attempts THEN 'pending' ELSE 'failed' END,
			attempt_count = o.attempt_count + 1, scheduled_at = now() + u.backoff, updated_at = now()
		FROM unnest($1::uuid[], $2::interval[], $3::timestamptz[]) AS u(id, backoff, claimed_at)
		WHERE o.id = u.id AND o.status = 'processing' AND o.updated_at = u.claimed_at
		RETURNING o.id`, s.table,
	)
	ids, claimedAts := splitClaims(e)
	intervals := make([]pgtype.Interval, len(backoffs))
	for i, d := range backoffs {
		intervals[i] = durationToInterval(d)
	}
	rows, err := q.Query(ctx, sql, ids, intervals, claimedAts)
	if err != nil {
		return nil, fmt.Errorf("mark events as failed: %w", err)
	}
	updatedIDs, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		return nil, fmt.Errorf("collect mark failed event ids: %w", err)
	}
	return updatedIDs, nil
}

// Fail marks events as failed without a backoff,
// for use when the failure is unrecoverable or retrying would be pointless.
func (s *Store) Fail(ctx context.Context, q pgoutbox.Querier, events []*pgoutbox.Event) ([]uuid.UUID, error) {
	sql := fmt.Sprintf(
		`
		UPDATE %[1]s AS o
		SET status = 'failed', attempt_count = o.attempt_count + 1, updated_at = now()
		FROM unnest($1::uuid[], $2::timestamptz[]) AS u(id, claimed_at)
		WHERE o.id = u.id AND o.status = 'processing' AND o.updated_at = u.claimed_at
		RETURNING o.id`, s.table,
	)
	ids, claimedAts := splitClaims(events)
	rows, err := q.Query(ctx, sql, ids, claimedAts)
	if err != nil {
		return nil, fmt.Errorf("fail events: %w", err)
	}
	updatedIDs, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		return nil, fmt.Errorf("collect failed event ids: %w", err)
	}
	return updatedIDs, nil
}

// Unclaim returns claimed events to pending, making them available for other pollers.
// It only unclaims events that are still in processing and have not been updated since they were claimed
// when the relay's context is canceled mid-batch.
func (s *Store) Unclaim(ctx context.Context, q pgoutbox.Querier, e []*pgoutbox.Event) ([]uuid.UUID, error) {
	sql := fmt.Sprintf(
		`
		UPDATE %[1]s AS o
		SET status = 'pending',	updated_at = now()
		FROM unnest($1::uuid[], $2::timestamptz[]) AS u(id, claimed_at)
		WHERE o.id = u.id AND o.status = 'processing' AND o.updated_at = u.claimed_at
		RETURNING o.id`, s.table,
	)
	ids, claimedAts := splitClaims(e)
	rows, err := q.Query(ctx, sql, ids, claimedAts)
	if err != nil {
		return nil, fmt.Errorf("unclaim mid-batch canceled events: %w", err)
	}
	updatedIDs, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		return nil, fmt.Errorf("collect unclaimed event ids: %w", err)
	}
	return updatedIDs, nil
}

// ReapStuck identifies events that have been claimed for processing
// but have not been marked as processed or failed within the specified stuckTimeout.
// It transitions them back to pending for retry, unless they've exhausted their reap budget (reap_count >= maxReaps).
func (s *Store) ReapStuck(
	ctx context.Context, q pgoutbox.Querier, stuckTimeout, backoff time.Duration, maxReaps, batchSize int,
) ([]*pgoutbox.Event, error) {
	sql := fmt.Sprintf(
		`
		WITH stuck AS (SELECT id FROM %[1]s
					   WHERE status = 'processing' AND updated_at < now() - $1::interval
					   ORDER BY updated_at, id
					   FOR UPDATE SKIP LOCKED LIMIT $2)
		UPDATE %[1]s AS o
		SET status = CASE WHEN o.reap_count + 1 < $4 THEN 'pending' ELSE 'failed' END,
			reap_count = o.reap_count + 1, scheduled_at = now() + $3::interval, updated_at = now()
		FROM stuck WHERE o.id = stuck.id
		RETURNING o.id, o.attempt_count, o.max_attempts, o.reap_count, o.status`, s.table,
	)
	rows, err := q.Query(
		ctx, sql, durationToInterval(stuckTimeout), batchSize,
		durationToInterval(backoff), maxReaps,
	)
	if err != nil {
		return nil, fmt.Errorf("reap stuck events: %w", err)
	}
	events, err := pgx.CollectRows(rows, collectReapedEvent)
	if err != nil {
		return nil, fmt.Errorf("collect reaped events: %w", err)
	}
	return events, nil
}

// DeleteProcessed removes events that were marked as processed more than the specified retention period.
func (s *Store) DeleteProcessed(
	ctx context.Context, q pgoutbox.Querier, olderThan time.Duration, batchSize int,
) (int64, error) {
	sql := fmt.Sprintf(
		`
		WITH to_delete AS (
			SELECT id FROM %[1]s
			WHERE status = 'processed' AND updated_at < now() - $1::interval
			ORDER BY updated_at, id
			FOR UPDATE SKIP LOCKED LIMIT $2
		)
		DELETE FROM %[1]s
		WHERE id IN (SELECT id FROM to_delete)`, s.table,
	)
	tag, err := q.Exec(ctx, sql, durationToInterval(olderThan), batchSize)
	if err != nil {
		return 0, fmt.Errorf("delete processed events: %w", err)
	}
	return tag.RowsAffected(), nil
}

// RequeueFailed resets up to batchSize failed events back to pending so they can be retried.
// Returns the number of events requeued.
func (s *Store) RequeueFailed(ctx context.Context, q pgoutbox.Querier, batchSize int) (int64, error) {
	sql := fmt.Sprintf(
		`
			UPDATE %[1]s
			SET status = 'pending',	attempt_count = 0, reap_count = 0,
				scheduled_at = now(), updated_at = now()
			WHERE id IN (SELECT id FROM %[1]s
						 WHERE status = 'failed'
						 ORDER BY updated_at, id 
						 FOR UPDATE SKIP LOCKED LIMIT $1)`, s.table,
	)
	tag, err := q.Exec(ctx, sql, batchSize)
	if err != nil {
		return 0, fmt.Errorf("requeue failed events: %w", err)
	}
	return tag.RowsAffected(), nil
}

func durationToInterval(d time.Duration) pgtype.Interval {
	return pgtype.Interval{Microseconds: d.Microseconds(), Valid: true}
}

func splitClaims(events []*pgoutbox.Event) ([]uuid.UUID, []time.Time) {
	ids := make([]uuid.UUID, len(events))
	claimedAts := make([]time.Time, len(events))
	for i, e := range events {
		ids[i] = e.ID
		claimedAts[i] = e.UpdatedAt
	}
	return ids, claimedAts
}

func collectEvent(row pgx.CollectableRow) (*pgoutbox.Event, error) {
	var e pgoutbox.Event
	if err := row.Scan(
		&e.ID, &e.Type, &e.Topic, &e.Payload,
		&e.Status, &e.AttemptCount, &e.MaxAttempts, &e.ReapCount,
		&e.ScheduledAt, &e.CreatedAt, &e.UpdatedAt,
	); err != nil {
		return nil, fmt.Errorf("scan row: %w", err)
	}
	return &e, nil
}

func collectReapedEvent(row pgx.CollectableRow) (*pgoutbox.Event, error) {
	var e pgoutbox.Event
	if err := row.Scan(&e.ID, &e.AttemptCount, &e.MaxAttempts, &e.ReapCount, &e.Status); err != nil {
		return nil, fmt.Errorf("scan reaped event: %w", err)
	}
	return &e, nil
}
