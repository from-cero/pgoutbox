package relay

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	t.Run("rejects a nil store", func(t *testing.T) {
		_, err := New(nil, &fakePublisher{}, discardLogger())
		if !errors.Is(err, ErrNilRelayStore) {
			t.Fatalf("err = %v, want %v", err, ErrNilRelayStore)
		}
	})

	t.Run("rejects a nil publisher", func(t *testing.T) {
		_, err := New(&fakeStore{}, nil, discardLogger())
		if !errors.Is(err, ErrNilRelayPublisher) {
			t.Fatalf("err = %v, want %v", err, ErrNilRelayPublisher)
		}
	})

	t.Run("rejects an invalid config", func(t *testing.T) {
		_, err := New(&fakeStore{}, &fakePublisher{}, discardLogger(), WithPollerInterval(0))
		if !errors.Is(err, errInvalidRelayConfig) {
			t.Fatalf("err = %v, want wrap of %v", err, errInvalidRelayConfig)
		}
	})

	t.Run("defaults the logger when nil", func(t *testing.T) {
		r, err := New(&fakeStore{}, &fakePublisher{}, nil)
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}
		if r.log == nil {
			t.Error("logger should default to slog.Default(), got nil")
		}
	})
}

func TestRelayRequeueFailed(t *testing.T) {
	t.Run("delegates to the store", func(t *testing.T) {
		s := &fakeStore{requeueCount: 12}
		r, _ := New(s, &fakePublisher{}, discardLogger())
		n, err := r.RequeueFailed(context.Background(), nil, 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if n != 12 {
			t.Errorf("RequeueFailed = %d, want 12", n)
		}
	})

	t.Run("propagates store errors", func(t *testing.T) {
		sentinel := errors.New("requeue boom")
		s := &fakeStore{requeueErr: sentinel}
		r, _ := New(s, &fakePublisher{}, discardLogger())
		if _, err := r.RequeueFailed(context.Background(), nil, 100); !errors.Is(err, sentinel) {
			t.Fatalf("err = %v, want %v", err, sentinel)
		}
	})
}

func TestRelayRunStopsWithContext(t *testing.T) {
	// With an already-canceled context every actor should drain once and return,
	// letting Run's WaitGroup complete. A timeout guards against a goroutine leak.
	run := func(t *testing.T, opts ...Option) {
		t.Helper()
		r, err := New(&fakeStore{}, &fakePublisher{}, discardLogger(), opts...)
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		done := make(chan error, 1)
		go func() { done <- r.Run(ctx, nil) }()

		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run() = %v, want nil", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Run() did not return after context cancellation")
		}
	}

	t.Run("poller and reaper only", func(t *testing.T) { run(t) })
	t.Run("with listener", func(t *testing.T) { run(t, WithListener(&fakeListener{})) })
	t.Run("with janitor", func(t *testing.T) { run(t, WithRetention(time.Hour)) })
}
