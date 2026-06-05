package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/from-cero/pgoutbox"
)

// fakeQuerier is a minimal pgoutbox.Querier for exercising the non-SQL logic in
// Store: argument preparation, early returns, and error wrapping.
type fakeQuerier struct {
	execTag  pgconn.CommandTag
	execErr  error
	queryErr error
	row      pgx.Row

	lastSQL  string
	lastArgs []any
}

func (f *fakeQuerier) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.lastSQL, f.lastArgs = sql, args
	return f.execTag, f.execErr
}

func (f *fakeQuerier) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	f.lastSQL, f.lastArgs = sql, args
	return nil, f.queryErr
}

func (f *fakeQuerier) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	f.lastSQL, f.lastArgs = sql, args
	return f.row
}

// fakeRow lets Insert's RETURNING ... Scan be tested without a database.
type fakeRow struct {
	scan func(dest ...any) error
}

func (r fakeRow) Scan(dest ...any) error { return r.scan(dest...) }

func TestDurationToInterval(t *testing.T) {
	iv := durationToInterval(90 * time.Second)
	if !iv.Valid {
		t.Fatal("interval should be valid")
	}
	if got, want := iv.Microseconds, int64(90_000_000); got != want {
		t.Errorf("Microseconds = %d, want %d", got, want)
	}
}

func TestSplitClaims(t *testing.T) {
	id1, id2 := uuid.New(), uuid.New()
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	events := []*pgoutbox.Event{
		{ID: id1, UpdatedAt: t1},
		{ID: id2, UpdatedAt: t2},
	}

	ids, claimedAts := splitClaims(events)
	if len(ids) != 2 || len(claimedAts) != 2 {
		t.Fatalf("lengths = %d, %d, want 2, 2", len(ids), len(claimedAts))
	}
	if ids[0] != id1 || ids[1] != id2 {
		t.Errorf("ids = %v, want [%v %v]", ids, id1, id2)
	}
	if !claimedAts[0].Equal(t1) || !claimedAts[1].Equal(t2) {
		t.Errorf("claimedAts = %v, want [%v %v]", claimedAts, t1, t2)
	}

	t.Run("empty input yields empty non-nil slices", func(t *testing.T) {
		ids, claimedAts := splitClaims(nil)
		if ids == nil || claimedAts == nil {
			t.Fatal("slices should be non-nil")
		}
		if len(ids) != 0 || len(claimedAts) != 0 {
			t.Errorf("lengths = %d, %d, want 0, 0", len(ids), len(claimedAts))
		}
	})
}

func TestStoreInsert(t *testing.T) {
	s := NewStore("outbox")

	t.Run("rejects invalid JSON payload before touching the database", func(t *testing.T) {
		q := &fakeQuerier{}
		e := &pgoutbox.Event{Payload: json.RawMessage("not json")}
		err := s.Insert(context.Background(), q, e)
		if !errors.Is(err, ErrInvalidPayload) {
			t.Fatalf("Insert() = %v, want %v", err, ErrInvalidPayload)
		}
		if q.lastSQL != "" {
			t.Error("database should not be queried for an invalid payload")
		}
	})

	t.Run("scans returned columns back into the event", func(t *testing.T) {
		wantID := uuid.New()
		created := time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC)
		q := &fakeQuerier{row: fakeRow{scan: func(dest ...any) error {
			*dest[0].(*uuid.UUID) = wantID
			*dest[1].(*int) = 1 // attempt_count
			*dest[2].(*int) = 2 // reap_count
			*dest[3].(*time.Time) = created
			*dest[4].(*time.Time) = created
			return nil
		}}}
		e := &pgoutbox.Event{Payload: json.RawMessage(`{"k":"v"}`)}
		if err := s.Insert(context.Background(), q, e); err != nil {
			t.Fatalf("Insert() error: %v", err)
		}
		if e.ID != wantID {
			t.Errorf("ID = %v, want %v", e.ID, wantID)
		}
		if e.AttemptCount != 1 || e.ReapCount != 2 {
			t.Errorf("AttemptCount=%d ReapCount=%d, want 1, 2", e.AttemptCount, e.ReapCount)
		}
		if !e.CreatedAt.Equal(created) || !e.UpdatedAt.Equal(created) {
			t.Errorf("timestamps not scanned back: %v %v", e.CreatedAt, e.UpdatedAt)
		}
	})

	t.Run("wraps scan errors", func(t *testing.T) {
		sentinel := errors.New("scan boom")
		q := &fakeQuerier{row: fakeRow{scan: func(...any) error { return sentinel }}}
		e := &pgoutbox.Event{Payload: json.RawMessage("{}")}
		if err := s.Insert(context.Background(), q, e); !errors.Is(err, sentinel) {
			t.Fatalf("Insert() = %v, want wrap of %v", err, sentinel)
		}
	})
}

func TestStoreMarkFailedLengthMismatch(t *testing.T) {
	s := NewStore("outbox")
	events := []*pgoutbox.Event{{ID: uuid.New()}}
	backoffs := []time.Duration{time.Second, 2 * time.Second}

	_, err := s.MarkFailed(context.Background(), &fakeQuerier{}, events, backoffs)
	if !errors.Is(err, ErrLengthMismatch) {
		t.Fatalf("MarkFailed() = %v, want %v", err, ErrLengthMismatch)
	}
}

func TestStoreQueryErrorsAreWrapped(t *testing.T) {
	s := NewStore("outbox")
	sentinel := errors.New("query down")
	ctx := context.Background()
	ev := []*pgoutbox.Event{{ID: uuid.New()}}

	tests := []struct {
		name string
		call func(q pgoutbox.Querier) error
	}{
		{"FetchPending", func(q pgoutbox.Querier) error {
			_, err := s.FetchPending(ctx, q, 10)
			return err
		}},
		{"MarkProcessed", func(q pgoutbox.Querier) error {
			_, err := s.MarkProcessed(ctx, q, ev)
			return err
		}},
		{"MarkFailed", func(q pgoutbox.Querier) error {
			_, err := s.MarkFailed(ctx, q, ev, []time.Duration{time.Second})
			return err
		}},
		{"Fail", func(q pgoutbox.Querier) error {
			_, err := s.Fail(ctx, q, ev)
			return err
		}},
		{"Unclaim", func(q pgoutbox.Querier) error {
			_, err := s.Unclaim(ctx, q, ev)
			return err
		}},
		{"ReapStuck", func(q pgoutbox.Querier) error {
			_, err := s.ReapStuck(ctx, q, time.Minute, time.Second, 5, 10)
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &fakeQuerier{queryErr: sentinel}
			if err := tt.call(q); !errors.Is(err, sentinel) {
				t.Fatalf("%s = %v, want wrap of %v", tt.name, err, sentinel)
			}
		})
	}
}

func TestStoreExecMethods(t *testing.T) {
	s := NewStore("outbox")
	ctx := context.Background()

	t.Run("DeleteProcessed returns rows affected", func(t *testing.T) {
		q := &fakeQuerier{execTag: pgconn.NewCommandTag("DELETE 7")}
		n, err := s.DeleteProcessed(ctx, q, time.Hour, 100)
		if err != nil {
			t.Fatalf("DeleteProcessed() error: %v", err)
		}
		if n != 7 {
			t.Errorf("rows affected = %d, want 7", n)
		}
	})

	t.Run("DeleteProcessed wraps exec errors", func(t *testing.T) {
		sentinel := errors.New("exec down")
		q := &fakeQuerier{execErr: sentinel}
		if _, err := s.DeleteProcessed(ctx, q, time.Hour, 100); !errors.Is(err, sentinel) {
			t.Fatalf("DeleteProcessed() = %v, want wrap of %v", err, sentinel)
		}
	})

	t.Run("RequeueFailed returns rows affected", func(t *testing.T) {
		q := &fakeQuerier{execTag: pgconn.NewCommandTag("UPDATE 3")}
		n, err := s.RequeueFailed(ctx, q, 100)
		if err != nil {
			t.Fatalf("RequeueFailed() error: %v", err)
		}
		if n != 3 {
			t.Errorf("rows affected = %d, want 3", n)
		}
	})

	t.Run("RequeueFailed wraps exec errors", func(t *testing.T) {
		sentinel := errors.New("exec down")
		q := &fakeQuerier{execErr: sentinel}
		if _, err := s.RequeueFailed(ctx, q, 100); !errors.Is(err, sentinel) {
			t.Fatalf("RequeueFailed() = %v, want wrap of %v", err, sentinel)
		}
	})
}
