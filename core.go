package pgoutbox

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var _ Transactor = (*Outbox)(nil)

type Transactor interface {
	Enqueue(ctx context.Context, tx pgx.Tx, event *Event) error
	ExecTx(ctx context.Context, fn func(tx pgx.Tx) error) error
}

type Outbox struct {
	db    *pgxpool.Pool
	store Store
}

func (o *Outbox) Enqueue(ctx context.Context, tx pgx.Tx, event *Event) error {
	if err := o.store.Insert(ctx, tx, event); err != nil {
		return fmt.Errorf("enqueue event: %w", err)
	}
	return nil
}

func (o *Outbox) ExecTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := o.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}

	var committed bool
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	if err := fn(tx); err != nil {
		return fmt.Errorf("execute transaction failed: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction failed: %w", err)
	}
	committed = true
	return nil
}
