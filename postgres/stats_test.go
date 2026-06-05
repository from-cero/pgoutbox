package postgres

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestStoreStats(t *testing.T) {
	s := NewStore("outbox")
	ctx := context.Background()

	t.Run("scans counters and converts age seconds to a duration", func(t *testing.T) {
		q := &fakeQuerier{row: fakeRow{scan: func(dest ...any) error {
			*dest[0].(*int64) = 5    // pending
			*dest[1].(*int64) = 2    // processing
			*dest[2].(*int64) = 1    // failed
			*dest[3].(*float64) = 90 // oldest pending age, seconds
			return nil
		}}}
		st, err := s.Stats(ctx, q)
		if err != nil {
			t.Fatalf("Stats() error: %v", err)
		}
		if st.Pending != 5 || st.Processing != 2 || st.Failed != 1 {
			t.Errorf("counters = %+v, want 5/2/1", st)
		}
		if st.OldestPendingAge != 90*time.Second {
			t.Errorf("OldestPendingAge = %v, want 90s", st.OldestPendingAge)
		}
	})

	t.Run("wraps scan errors", func(t *testing.T) {
		sentinel := errors.New("stats down")
		q := &fakeQuerier{row: fakeRow{scan: func(...any) error { return sentinel }}}
		if _, err := s.Stats(ctx, q); !errors.Is(err, sentinel) {
			t.Fatalf("Stats() = %v, want wrap of %v", err, sentinel)
		}
	})
}
