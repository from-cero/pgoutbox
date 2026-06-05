package relay

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/from-cero/pgoutbox"
	"github.com/from-cero/pgoutbox/relay/publisher"
)

// Relay is responsible for polling pending events from the outbox, publishing them using the provided publisher,
// and managing the lifecycle of events including reaping stuck events and deleting processed events past retention.
type Relay struct {
	s   Store
	pub publisher.Publisher
	cfg config
	log *slog.Logger
}

// New creates a new Relay. s and pub must not be nil. If log is nil, slog.Default() is used.
func New(s Store, pub publisher.Publisher, log *slog.Logger, opts ...Option) (*Relay, error) {
	if s == nil {
		return nil, ErrNilRelayStore
	}
	if pub == nil {
		return nil, ErrNilRelayPublisher
	}

	cfg := applyOptions(opts)
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	if log == nil {
		log = slog.Default()
	}
	return &Relay{s: s, pub: pub, cfg: cfg, log: log}, nil
}

// Run polls for pending events and publishes them until ctx is canceled.
// It also runs the reaper and, if retention is configured, the janitor.
func (r *Relay) Run(ctx context.Context, q pgoutbox.Querier) error {
	var wg sync.WaitGroup

	// a channel is used as a signal to wake the poller immediately when new events are available
	var wake chan struct{}
	if r.cfg.listener != nil {
		// the 1-capacity buffered channel ensures not to lose the signal if the listener
		// sends it while the poller is still processing the previous batch
		wake = make(chan struct{}, 1)
	}

	wg.Go(func() { r.runPoller(ctx, q, wake) })
	if r.cfg.listener != nil {
		wg.Go(func() { r.runListener(ctx, wake) })
	}
	wg.Go(func() { r.runReaper(ctx, q) })
	if r.cfg.janitor.retention > 0 {
		wg.Go(func() { r.runJanitor(ctx, q) })
	}

	wg.Wait()
	return nil
}

// RequeueFailed resets up to batchSize failed events back to pending so they can be retried.
// Returns the number of events requeued.
func (r *Relay) RequeueFailed(ctx context.Context, q pgoutbox.Querier, batchSize int) (int64, error) {
	return r.s.RequeueFailed(ctx, q, batchSize)
}
