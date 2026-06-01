package pgoutbox

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type Store interface {
	Insert(ctx context.Context, tx pgx.Tx, event *Event) error
}
