package relay

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRunListener(t *testing.T) {
	t.Run("delivers a wake signal on notification", func(t *testing.T) {
		l := &fakeListener{results: []error{nil}} // one success, then blocks on ctx
		r, _ := New(&fakeStore{}, &fakePublisher{}, discardLogger(), WithListener(l))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		wake := make(chan struct{}, 1)
		go r.runListener(ctx, wake)

		select {
		case <-wake:
		case <-time.After(time.Second):
			t.Fatal("expected a wake signal")
		}
	})

	t.Run("recovers after a listener error and retries", func(t *testing.T) {
		l := &fakeListener{results: []error{errors.New("conn dropped"), nil}}
		r, _ := New(&fakeStore{}, &fakePublisher{}, discardLogger(),
			WithListener(l), WithPollerInterval(10*time.Millisecond))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		wake := make(chan struct{}, 1)
		go r.runListener(ctx, wake)

		select {
		case <-wake:
			// recovered: after the error and the fallback wait, the next call succeeded.
		case <-time.After(time.Second):
			t.Fatal("listener did not recover after an error")
		}
	})

	t.Run("returns promptly when the context is canceled", func(t *testing.T) {
		l := &fakeListener{} // no results, always blocks on ctx
		r, _ := New(&fakeStore{}, &fakePublisher{}, discardLogger(), WithListener(l))

		ctx, cancel := context.WithCancel(context.Background())
		wake := make(chan struct{}, 1)
		done := make(chan struct{})
		go func() {
			r.runListener(ctx, wake)
			close(done)
		}()

		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("runListener did not return after cancellation")
		}
	})

	t.Run("does not block when the wake channel is full", func(t *testing.T) {
		l := &fakeListener{results: []error{nil, nil}} // two successes then blocks
		r, _ := New(&fakeStore{}, &fakePublisher{}, discardLogger(), WithListener(l))

		ctx, cancel := context.WithCancel(context.Background())
		wake := make(chan struct{}, 1)
		wake <- struct{}{} // pre-fill so the first send must be dropped

		done := make(chan struct{})
		go func() {
			r.runListener(ctx, wake)
			close(done)
		}()

		// give the listener time to process both notifications without deadlocking
		time.Sleep(20 * time.Millisecond)
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("runListener blocked on a full wake channel")
		}
	})
}
