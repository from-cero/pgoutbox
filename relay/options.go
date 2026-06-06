package relay

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/from-cero/pgoutbox"
)

// Option is a functional option for configuring a Relay.
type Option func(*config)

// --- Poller options.

// WithPollerInterval sets how often the poller scans for pending events.
func WithPollerInterval(d time.Duration) Option { return func(c *config) { c.poller.interval = d } }

// WithPollerBatchSize sets the maximum number of events claimed per poll cycle.
func WithPollerBatchSize(n int) Option { return func(c *config) { c.poller.batchSize = n } }

// WithShutdownGrace sets how long the poller waits for in-flight bookkeeping
// (mark processed, unclaim) to complete after its context is canceled.
func WithShutdownGrace(d time.Duration) Option { return func(c *config) { c.poller.shutdownGrace = d } }

// --- Reaper options.

// WithReaperInterval sets how often the reaper checks for stuck events.
func WithReaperInterval(d time.Duration) Option { return func(c *config) { c.reaper.interval = d } }

// WithReaperBatchSize sets the maximum number of stuck events reclaimed per reap cycle.
func WithReaperBatchSize(n int) Option { return func(c *config) { c.reaper.batchSize = n } }

// WithStuckTimeout sets how long an event may stay in the processing state
// before the reaper reschedules it. It must comfortably exceed the worst case
// duration of a single PublishBatch call (the publisher's write timeout times
// its internal retries), otherwise the reaper will reschedule events that are
// still in flight and produce duplicates.
func WithStuckTimeout(d time.Duration) Option { return func(c *config) { c.reaper.stuckTimeout = d } }

// WithMaxReaps caps how many times a stuck event may be reaped back to pending
// before it is parked as failed. Reaping tracks infrastructure failures
// (crashes, stalls) and is budgeted separately from the delivery retry budget
// (max_attempts), so a crashy deployment cannot exhaust an event's publish
// retries.
func WithMaxReaps(n int) Option { return func(c *config) { c.reaper.maxReaps = n } }

// --- Janitor options.

// WithRetention enables the janitor: processed events older than d are deleted in batches.
// Zero (0, the default) disables deletion and keeps processed events forever,
// which grows the table without bound.
func WithRetention(d time.Duration) Option { return func(c *config) { c.janitor.retention = d } }

// WithJanitorInterval sets how often the janitor runs its deletion sweep.
func WithJanitorInterval(d time.Duration) Option { return func(c *config) { c.janitor.interval = d } }

// WithJanitorBatchSize sets the maximum number of processed events deleted per sweep.
func WithJanitorBatchSize(n int) Option { return func(c *config) { c.janitor.batchSize = n } }

// --- Shared options.

// WithBatchSize sets the batch size for all three actors (poller, reaper, janitor).
func WithBatchSize(n int) Option {
	return func(c *config) {
		c.poller.batchSize = n
		c.reaper.batchSize = n
		c.janitor.batchSize = n
	}
}

// --- Custom behavior options.

// WithListener wakes the poller on enqueue notifications for near-real-time
// dispatch; without it, freshly committed events wait up to the poll interval.
func WithListener(l Listener) Option { return func(c *config) { c.listener = l } }

// WithHooks registers observability callbacks. See Hooks.
func WithHooks(h Hooks) Option { return func(c *config) { c.hooks = h } }

// WithBackoff sets the function used to compute the retry delay for a failed event.
// attempt is 1-based. DefaultBackoff is used when this option is not provided.
func WithBackoff(fn func(attempt int) time.Duration) Option {
	return func(c *config) { c.backoff = fn }
}

// WithTopicResolver sets a function that returns the broker topic for a given event.
// When not set, the relay uses the event's Topic field directly.
func WithTopicResolver(fn func(*pgoutbox.Event) string) Option {
	return func(c *config) { c.topicResolver = fn }
}

type pollerConfig struct {
	interval      time.Duration // default: 5s
	batchSize     int           // default: 100
	shutdownGrace time.Duration // default: 5s
}

type reaperConfig struct {
	interval     time.Duration // default: 30s
	batchSize    int           // default: 100
	stuckTimeout time.Duration // default: 1m
	maxReaps     int           // default: 10
}

type janitorConfig struct {
	retention time.Duration // default: 0 (no deletion)
	interval  time.Duration // default: 5m
	batchSize int           // default: 100
}

type config struct {
	poller  pollerConfig
	reaper  reaperConfig
	janitor janitorConfig

	listener      Listener
	hooks         Hooks
	topicResolver func(event *pgoutbox.Event) string
	backoff       func(attempt int) time.Duration
}

// DefaultBackoff doubles from 1s and caps at 1 minute, with equal jitter (half
// fixed, half random) so events failed by one broker blip do not retry in
// lockstep.
func DefaultBackoff(attempt int) time.Duration {
	const backoffCap = time.Minute
	attempt = max(attempt, 1)
	shift := attempt - 1
	d := backoffCap
	if shift <= 16 {
		if v := time.Second << shift; v < backoffCap {
			d = v
		}
	}
	return d/2 + rand.N(d/2+1)
}

var defaultConfig = config{
	poller: pollerConfig{
		interval:      5 * time.Second,
		batchSize:     100,
		shutdownGrace: 5 * time.Second,
	},
	reaper: reaperConfig{
		interval:     30 * time.Second,
		batchSize:    100,
		stuckTimeout: time.Minute,
		maxReaps:     10,
	},
	janitor: janitorConfig{
		retention: 0,
		interval:  5 * time.Minute,
		batchSize: 100,
	},
	backoff: DefaultBackoff,
}

func applyOptions(opts []Option) *config {
	cfg := defaultConfig
	for _, o := range opts {
		o(&cfg)
	}
	return &cfg
}

func (c *config) validate() error {
	var errs []error
	if c.poller.interval <= 0 {
		errs = append(errs, fmt.Errorf("%w, got: %s", errPollerInterval, c.poller.interval))
	}
	if c.poller.batchSize <= 0 {
		errs = append(errs, fmt.Errorf("%w, got: %d", errPollerBatchSize, c.poller.batchSize))
	}
	if c.poller.shutdownGrace <= 0 {
		errs = append(errs, fmt.Errorf("%w, got: %s", errPollerShutdownGrace, c.poller.shutdownGrace))
	}
	if c.reaper.interval <= 0 {
		errs = append(errs, fmt.Errorf("%w, got: %s", errReaperInterval, c.reaper.interval))
	}
	if c.reaper.batchSize <= 0 {
		errs = append(errs, fmt.Errorf("%w, got: %d", errReaperBatchSize, c.reaper.batchSize))
	}
	if c.reaper.stuckTimeout <= 0 {
		errs = append(errs, fmt.Errorf("%w, got: %s", errReaperStuckTimeout, c.reaper.stuckTimeout))
	}
	if c.reaper.maxReaps <= 0 {
		errs = append(errs, fmt.Errorf("%w, got: %d", errReaperMaxReaps, c.reaper.maxReaps))
	}
	if c.janitor.retention < 0 {
		errs = append(errs, fmt.Errorf("%w, got: %s", errJanitorRetention, c.janitor.retention))
	}
	if c.janitor.interval <= 0 {
		errs = append(errs, fmt.Errorf("%w, got: %s", errJanitorInterval, c.janitor.interval))
	}
	if c.janitor.batchSize <= 0 {
		errs = append(errs, fmt.Errorf("%w, got: %d", errJanitorBatchSize, c.janitor.batchSize))
	}

	if len(errs) > 0 {
		return fmt.Errorf("%w: %w", errInvalidRelayConfig, errors.Join(errs...))
	}
	return nil
}
