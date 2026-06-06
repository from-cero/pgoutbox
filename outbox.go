package pgoutbox

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var _ interface {
	Enqueuer
	Transactor
} = (*Outbox)(nil)

// Querier executes SQL queries. Satisfied by *pgxpool.Pool, *pgx.Conn, and pgx.Tx.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// TxBeginner starts transactions. Satisfied by *pgxpool.Pool, *pgx.Conn, and pgx.Tx.
type TxBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Enqueuer adds events to the outbox.
type Enqueuer interface {
	Enqueue(ctx context.Context, q Querier, e *Event) error
}

// Transactor executes a function within a transaction.
type Transactor interface {
	// ExecTx commits if fn returns nil, rolls back otherwise.
	ExecTx(ctx context.Context, txb TxBeginner, fn func(tx pgx.Tx) error) error
}

// Outbox provides methods to enqueue events and execute functions within transactions.
type Outbox struct {
	s   Store
	cfg *config
}

// New creates a new Outbox. s must not be nil.
func New(s Store, opts ...Option) (*Outbox, error) {
	if s == nil {
		return nil, ErrNilOutboxStore
	}
	cfg := applyOptions(opts)
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return &Outbox{
		s:   s,
		cfg: cfg,
	}, nil
}

// Enqueue fills event defaults if not set, then inserts it into the outbox.
func (o *Outbox) Enqueue(ctx context.Context, q Querier, e *Event) error {
	e.fillDefaultsIfNeeded(o.cfg.maxAttempts)
	if err := o.s.Insert(ctx, q, e); err != nil {
		return fmt.Errorf("insert event: %w", err)
	}
	return nil
}

// ExecTx runs fn in a transaction. Commits on success, rolls back if fn returns an error.
func (o *Outbox) ExecTx(ctx context.Context, txb TxBeginner, fn func(tx pgx.Tx) error) error {
	tx, err := txb.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	var committed bool
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	if err := fn(tx); err != nil {
		return fmt.Errorf("execute transaction: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	committed = true
	return nil
}
