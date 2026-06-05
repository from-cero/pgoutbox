package pgoutbox

import (
	"context"
)

// Store is the interface for inserting events into the outbox.
type Store interface {
	Insert(ctx context.Context, q Querier, e *Event) error
}
