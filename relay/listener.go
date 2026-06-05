package relay

import (
	"context"
	"time"
)

// Listener is an interface that abstracts the mechanism
// for receiving notifications about new events in the outbox.
type Listener interface {
	// WaitForNotification blocks until a notification is received or the context is canceled.
	WaitForNotification(ctx context.Context) error
}

// the best-effort mechanism to wake the poller immediately when new events are available.
func (r *Relay) runListener(ctx context.Context, wake chan<- struct{}) {
	// check for notifications until the context is canceled or deadlined
	for ctx.Err() == nil {
		if err := r.cfg.listener.WaitForNotification(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			r.log.WarnContext(
				ctx, "listener failed, falling back to polling",
				"error", err.Error(),
			)

			// wait for the configured poller interval
			// before trying again to avoid busy-looping on a failing listener
			select {
			case <-ctx.Done():
				return
			case <-time.After(r.cfg.poller.interval):
			}
			continue
		}

		// notify the poller to wake immediately through wake channel
		// if wake is one signal buffered (full), default will drop the signal
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}
