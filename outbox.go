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

type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// TxBeginner starts transactions. *pgxpool.Pool, *pgx.Conn and pgx.Tx
type TxBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

type Enqueuer interface {
	Enqueue(ctx context.Context, q Querier, e *Event) error
}

type Transactor interface {
	ExecTx(ctx context.Context, txb TxBeginner, fn func(tx pgx.Tx) error) error
}

type Outbox struct {
	store Store
}

func New(store Store) *Outbox {
	return &Outbox{
		store: store,
	}
}

func (o *Outbox) Enqueue(ctx context.Context, q Querier, e *Event) error {
	if err := o.store.Insert(ctx, q, e); err != nil {
		return fmt.Errorf("insert event: %w", err)
	}
	return nil
}

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
