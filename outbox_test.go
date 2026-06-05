package pgoutbox

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
)

// fakeStore records the events passed to Insert and can return a canned error.
type fakeStore struct {
	inserted []*Event
	err      error
}

func (f *fakeStore) Insert(_ context.Context, _ Querier, e *Event) error {
	f.inserted = append(f.inserted, e)
	return f.err
}

// fakeTx embeds pgx.Tx so it satisfies the interface; only the methods the
// Outbox uses are implemented. Any unexpected call panics on the nil embed.
type fakeTx struct {
	pgx.Tx
	commitErr   error
	committed   bool
	rolledBack  bool
	commitCalls int
}

func (t *fakeTx) Commit(_ context.Context) error {
	t.commitCalls++
	if t.commitErr != nil {
		return t.commitErr
	}
	t.committed = true
	return nil
}

func (t *fakeTx) Rollback(_ context.Context) error {
	t.rolledBack = true
	return nil
}

type fakeTxBeginner struct {
	tx       *fakeTx
	beginErr error
}

func (b *fakeTxBeginner) Begin(_ context.Context) (pgx.Tx, error) {
	if b.beginErr != nil {
		return nil, b.beginErr
	}
	return b.tx, nil
}

func TestNew(t *testing.T) {
	t.Run("rejects a nil store", func(t *testing.T) {
		_, err := New(nil)
		if !errors.Is(err, ErrNilOutboxStore) {
			t.Fatalf("New(nil) = %v, want %v", err, ErrNilOutboxStore)
		}
	})

	t.Run("rejects an invalid config", func(t *testing.T) {
		_, err := New(&fakeStore{}, WithMaxAttempts(0))
		if !errors.Is(err, errInvalidOutboxConfig) {
			t.Fatalf("New() = %v, want wrap of errInvalidOutboxConfig", err)
		}
	})

	t.Run("returns an outbox with the applied config", func(t *testing.T) {
		o, err := New(&fakeStore{}, WithMaxAttempts(11))
		if err != nil {
			t.Fatalf("New() unexpected error: %v", err)
		}
		if o.cfg.maxAttempts != 11 {
			t.Errorf("maxAttempts = %d, want 11", o.cfg.maxAttempts)
		}
	})
}

func TestOutboxEnqueue(t *testing.T) {
	t.Run("fills defaults then inserts", func(t *testing.T) {
		s := &fakeStore{}
		o, err := New(s, WithMaxAttempts(4))
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}
		e := &Event{Type: "order.created"}
		if err := o.Enqueue(context.Background(), nil, e); err != nil {
			t.Fatalf("Enqueue() error: %v", err)
		}
		if len(s.inserted) != 1 {
			t.Fatalf("Insert called %d times, want 1", len(s.inserted))
		}
		if e.Status != EventPending {
			t.Errorf("Status = %q, want %q (defaults not filled)", e.Status, EventPending)
		}
		if e.MaxAttempts != 4 {
			t.Errorf("MaxAttempts = %d, want 4", e.MaxAttempts)
		}
	})

	t.Run("wraps insert errors", func(t *testing.T) {
		sentinel := errors.New("boom")
		s := &fakeStore{err: sentinel}
		o, _ := New(s)
		err := o.Enqueue(context.Background(), nil, &Event{})
		if !errors.Is(err, sentinel) {
			t.Fatalf("Enqueue() = %v, want wrap of %v", err, sentinel)
		}
	})
}

func TestOutboxExecTx(t *testing.T) {
	t.Run("commits when fn succeeds", func(t *testing.T) {
		tx := &fakeTx{}
		b := &fakeTxBeginner{tx: tx}
		o, _ := New(&fakeStore{})

		var called bool
		err := o.ExecTx(context.Background(), b, func(pgx.Tx) error {
			called = true
			return nil
		})
		if err != nil {
			t.Fatalf("ExecTx() error: %v", err)
		}
		if !called {
			t.Error("fn was not called")
		}
		if !tx.committed {
			t.Error("transaction was not committed")
		}
		if tx.rolledBack {
			t.Error("transaction should not have rolled back on success")
		}
	})

	t.Run("rolls back when fn fails", func(t *testing.T) {
		tx := &fakeTx{}
		b := &fakeTxBeginner{tx: tx}
		o, _ := New(&fakeStore{})

		sentinel := errors.New("fn failed")
		err := o.ExecTx(context.Background(), b, func(pgx.Tx) error { return sentinel })
		if !errors.Is(err, sentinel) {
			t.Fatalf("ExecTx() = %v, want wrap of %v", err, sentinel)
		}
		if tx.committed {
			t.Error("transaction should not have committed")
		}
		if !tx.rolledBack {
			t.Error("transaction should have rolled back")
		}
	})

	t.Run("returns begin errors without calling fn", func(t *testing.T) {
		sentinel := errors.New("cannot begin")
		b := &fakeTxBeginner{beginErr: sentinel}
		o, _ := New(&fakeStore{})

		var called bool
		err := o.ExecTx(context.Background(), b, func(pgx.Tx) error {
			called = true
			return nil
		})
		if !errors.Is(err, sentinel) {
			t.Fatalf("ExecTx() = %v, want wrap of %v", err, sentinel)
		}
		if called {
			t.Error("fn should not be called when Begin fails")
		}
	})

	t.Run("rolls back when commit fails", func(t *testing.T) {
		sentinel := errors.New("commit failed")
		tx := &fakeTx{commitErr: sentinel}
		b := &fakeTxBeginner{tx: tx}
		o, _ := New(&fakeStore{})

		err := o.ExecTx(context.Background(), b, func(pgx.Tx) error { return nil })
		if !errors.Is(err, sentinel) {
			t.Fatalf("ExecTx() = %v, want wrap of %v", err, sentinel)
		}
		if !tx.rolledBack {
			t.Error("transaction should roll back when commit fails")
		}
	})
}
