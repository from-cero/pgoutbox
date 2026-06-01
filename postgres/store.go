package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/from-cero/pgoutbox"
)

type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type Store struct {
	db    DB
	table string

	insertSQL          string
	fetchPendingSQL    string
	maskAsProcessedSQL string
	maskAsFailedSQL    string
	fetchStuckSQL      string
	resetStuckSQL      string
}

func NewStore(db *pgxpool.Pool, table string) *Store {
	return &Store{
		db:    db,
		table: table,

		insertSQL: `
			INSERT INTO %s (id, data_id, data_type, event_type, topic, payload,
			               status, max_retries, scheduled_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) 
			RETURNING id, retry_count, created_at, updated_at`,
		fetchPendingSQL: `
			WITH claimed AS (
				SELECT id FROM %s
				WHERE status = 'pending' AND scheduled_at <= now()
				ORDER BY scheduled_at
				FOR UPDATE SKIP LOCKED
				LIMIT $1
			)
			UPDATE %s AS o
			SET status = 'processing', updated_at = now()
			FROM claimed WHERE o.id = claimed.id
			RETURNING o.id, o.data_id, o.data_type, o.event_type, o.topic, o.payload,
					o.status, o.retry_count, o.max_retries,
					o.scheduled_at,	o.created_at, o.updated_at`,
		maskAsProcessedSQL: `
			UPDATE %s
			SET status = 'processed', updated_at = now()
			WHERE id = $1 AND status = 'processing'`,
		maskAsFailedSQL: `
			UPDATE %s
			SET status = 'failed', scheduled_at = now() + $2, updated_at = now()
			WHERE id = $1 AND status = 'processing'`,
		fetchStuckSQL: `
			SELECT id, data_id, data_type, event_type, topic, payload,
			status, retry_count, max_retries, scheduled_at, created_at, updated_at
			FROM %s WHERE status = 'processing' AND updated_at < now() - $1
			ORDER BY updated_at LIMIT $2`,
		resetStuckSQL: `
			UPDATE %s
			SET status = 'pending', retry_count = retry_count + 1, updated_at = now()
			WHERE status = 'processing' AND retry_count < max_retries`,
	}
}

func (s *Store) Insert(ctx context.Context, tx pgx.Tx, event *pgoutbox.Event) error {
	status := event.Status
	if status == "" {
		status = pgoutbox.EventPending
	}
	scheduledAt := event.ScheduledAt
	if scheduledAt.IsZero() {
		scheduledAt = time.Now()
	}

	err := tx.QueryRow(
		ctx, fmt.Sprintf(s.insertSQL, s.table),
		event.ID, event.DataID, event.DataType, event.EventType, event.Topic,
		event.Payload, status, event.MaxRetries, scheduledAt,
	).Scan(&event.RetryCount, &event.CreatedAt, &event.UpdatedAt)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}

	event.Status = status
	event.ScheduledAt = scheduledAt
	return nil
}

func (s *Store) FetchPending(ctx context.Context, batchSize int) ([]*pgoutbox.Event, error) {
	rows, err := s.db.Query(ctx, fmt.Sprintf(s.fetchStuckSQL, s.table), batchSize)
	if err != nil {
		return nil, fmt.Errorf("fetch pending events: %w", err)
	}
	events, err := pgx.CollectRows(rows, collectEvent)
	if err != nil {
		return nil, fmt.Errorf("collect pending events: %w", err)
	}
	return events, nil
}

func (s *Store) MaskAsProcessed(ctx context.Context, id int64) error {
	_, err := s.db.Exec(ctx, fmt.Sprintf(s.maskAsProcessedSQL, s.table), id)
	if err != nil {
		return fmt.Errorf("mask processed events: %w", err)
	}
	return nil
}

func (s *Store) MaskAsFailed(ctx context.Context, id int64, nextScheduledAt time.Duration) error {
	_, err := s.db.Exec(ctx, fmt.Sprintf(s.maskAsFailedSQL, s.table), id, nextScheduledAt)
	if err != nil {
		return fmt.Errorf("mask failed events: %w", err)
	}
	return nil
}

func (s *Store) FetchStuck(ctx context.Context, stuckTimeout time.Duration, batchSize int) ([]*pgoutbox.Event, error) {
	rows, err := s.db.Query(ctx, fmt.Sprintf(s.fetchStuckSQL, s.table), stuckTimeout, batchSize)
	if err != nil {
		return nil, fmt.Errorf("fetch stuck events: %w", err)
	}
	events, err := pgx.CollectRows(rows, collectEvent)
	if err != nil {
		return nil, fmt.Errorf("fetch stuck events: %w", err)
	}
	return events, nil
}

func (s *Store) ResetStuck(ctx context.Context, id int64) error {
	_, err := s.db.Exec(ctx, fmt.Sprintf(s.resetStuckSQL, s.table), id)
	if err != nil {
		return fmt.Errorf("reset stuck events: %w", err)
	}
	return nil
}

func collectEvent(row pgx.CollectableRow) (*pgoutbox.Event, error) {
	var e pgoutbox.Event
	if err := row.Scan(
		&e.ID, &e.DataID, &e.DataType, &e.EventType, &e.Topic, &e.Payload,
		&e.Status, &e.RetryCount, &e.MaxRetries, &e.ScheduledAt, &e.CreatedAt, &e.UpdatedAt,
	); err != nil {
		return nil, fmt.Errorf("collect event: %w", err)
	}
	return &e, nil
}
