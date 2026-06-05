package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Listener satisfies relay.Listener using Postgres LISTEN/NOTIFY.
// It holds one dedicated connection from the pool while listening;
// the connection is dropped and re-acquired after any error, so transient failures self-heal.
//
// Listener is not safe for concurrent use - the relay drives it from a single goroutine.
type Listener struct {
	pool    *pgxpool.Pool
	channel string
	conn    *pgxpool.Conn
}

// NewListener returns a Listener on the given channel. The channel must be the table name passed to NewStore:
// EnsureSchema installs a trigger that notifies that channel after every insert, on commit.
func NewListener(pool *pgxpool.Pool, channel string) *Listener {
	return &Listener{pool: pool, channel: channel}
}

// WaitForNotification blocks until an enqueue notification arrives or ctx is done.
// Notifications arriving while nobody is waiting are dropped;
// that is fine because the relay's poll remains the source of truth and the listener is only a latency optimization.
func (l *Listener) WaitForNotification(ctx context.Context) error {
	if l.conn == nil {
		conn, err := l.pool.Acquire(ctx)
		if err != nil {
			return fmt.Errorf("acquire listen connection: %w", err)
		}
		if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{l.channel}.Sanitize()); err != nil {
			conn.Release()
			return fmt.Errorf("listen on %q: %w", l.channel, err)
		}
		l.conn = conn
	}

	if _, err := l.conn.Conn().WaitForNotification(ctx); err != nil {
		l.Close()
		return fmt.Errorf("wait for notification: %w", err)
	}
	return nil
}

// Close releases the dedicated listen connection, if any.
// The Listener remains usable: the next WaitForNotification acquires a fresh connection.
func (l *Listener) Close() {
	if l.conn != nil {
		l.conn.Release()
		l.conn = nil
	}
}
