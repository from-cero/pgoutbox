package pgoutbox

import (
	"context"
)

type Store interface {
	Insert(ctx context.Context, q Querier, e *Event) error
}
