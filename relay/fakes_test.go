package relay

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/from-cero/pgoutbox"
)

// discardLogger returns a logger that drops all output, keeping test logs quiet.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeStore is a configurable relay.Store. Each method returns canned values and
// records the events it was called with so tests can assert on the bookkeeping.
type fakeStore struct {
	mu sync.Mutex

	fetchBatches [][]*pgoutbox.Event // returned in order, one per FetchPending call
	fetchErr     error

	markProcessedIDs []uuid.UUID
	markProcessedErr error
	markProcessedGot []*pgoutbox.Event

	markFailedIDs      []uuid.UUID
	markFailedErr      error
	markFailedGot      []*pgoutbox.Event
	markFailedBackoffs []time.Duration

	failIDs []uuid.UUID
	failErr error
	failGot []*pgoutbox.Event

	unclaimIDs []uuid.UUID
	unclaimErr error
	unclaimGot []*pgoutbox.Event

	reapBatches [][]*pgoutbox.Event
	reapErr     error

	deleteCounts []int64
	deleteErr    error

	requeueCount int64
	requeueErr   error

	fetchCalls  int
	reapCalls   int
	deleteCalls int
}

func (f *fakeStore) FetchPending(_ context.Context, _ pgoutbox.Querier, _ int) ([]*pgoutbox.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fetchErr != nil {
		return nil, f.fetchErr
	}
	i := f.fetchCalls
	f.fetchCalls++
	if i < len(f.fetchBatches) {
		return f.fetchBatches[i], nil
	}
	return nil, nil
}

func (f *fakeStore) MarkProcessed(_ context.Context, _ pgoutbox.Querier, es []*pgoutbox.Event) ([]uuid.UUID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.markProcessedGot = es
	return f.markProcessedIDs, f.markProcessedErr
}

func (f *fakeStore) MarkFailed(
	_ context.Context, _ pgoutbox.Querier, es []*pgoutbox.Event, backoffs []time.Duration,
) ([]uuid.UUID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.markFailedGot = es
	f.markFailedBackoffs = backoffs
	return f.markFailedIDs, f.markFailedErr
}

func (f *fakeStore) Fail(_ context.Context, _ pgoutbox.Querier, es []*pgoutbox.Event) ([]uuid.UUID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failGot = es
	return f.failIDs, f.failErr
}

func (f *fakeStore) Unclaim(_ context.Context, _ pgoutbox.Querier, es []*pgoutbox.Event) ([]uuid.UUID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unclaimGot = es
	return f.unclaimIDs, f.unclaimErr
}

func (f *fakeStore) ReapStuck(
	_ context.Context, _ pgoutbox.Querier, _, _ time.Duration, _, _ int,
) ([]*pgoutbox.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reapErr != nil {
		return nil, f.reapErr
	}
	i := f.reapCalls
	f.reapCalls++
	if i < len(f.reapBatches) {
		return f.reapBatches[i], nil
	}
	return nil, nil
}

func (f *fakeStore) DeleteProcessed(_ context.Context, _ pgoutbox.Querier, _ time.Duration, _ int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deleteErr != nil {
		return 0, f.deleteErr
	}
	i := f.deleteCalls
	f.deleteCalls++
	if i < len(f.deleteCounts) {
		return f.deleteCounts[i], nil
	}
	return 0, nil
}

func (f *fakeStore) RequeueFailed(_ context.Context, _ pgoutbox.Querier, _ int) (int64, error) {
	return f.requeueCount, f.requeueErr
}

// fakePublisher returns canned per-event results and records the events it saw.
type fakePublisher struct {
	results []error
	got     []*pgoutbox.Event
	closed  bool
}

func (p *fakePublisher) PublishBatch(_ context.Context, events []*pgoutbox.Event) []error {
	p.got = events
	if p.results != nil {
		return p.results
	}
	return make([]error, len(events)) // all acked
}

func (p *fakePublisher) Close() error {
	p.closed = true
	return nil
}

// fakeListener drives runListener: each WaitForNotification call returns the next
// programmed result, then blocks on ctx once exhausted.
type fakeListener struct {
	results []error
	calls   int
	mu      sync.Mutex
}

func (l *fakeListener) WaitForNotification(ctx context.Context) error {
	l.mu.Lock()
	i := l.calls
	l.calls++
	var res error
	exhausted := i >= len(l.results)
	if !exhausted {
		res = l.results[i]
	}
	l.mu.Unlock()

	if exhausted {
		<-ctx.Done()
		return ctx.Err()
	}
	return res
}

func newEvent() *pgoutbox.Event {
	return &pgoutbox.Event{
		ID:          uuid.New(),
		Type:        "test.event",
		Topic:       "test-topic",
		Status:      pgoutbox.EventProcessing,
		MaxAttempts: 3,
	}
}
