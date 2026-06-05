package pgoutbox

import (
	"testing"
	"time"
)

func TestEventFillDefaultsIfNeeded(t *testing.T) {
	t.Run("fills all defaults on a zero event", func(t *testing.T) {
		before := time.Now()
		e := &Event{}
		e.fillDefaultsIfNeeded(5)
		after := time.Now()

		if e.Status != EventPending {
			t.Errorf("Status = %q, want %q", e.Status, EventPending)
		}
		if e.MaxAttempts != 5 {
			t.Errorf("MaxAttempts = %d, want 5", e.MaxAttempts)
		}
		if e.ScheduledAt.Before(before) || e.ScheduledAt.After(after) {
			t.Errorf("ScheduledAt = %v, want between %v and %v", e.ScheduledAt, before, after)
		}
	})

	t.Run("preserves explicitly set fields", func(t *testing.T) {
		scheduled := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
		e := &Event{
			Status:      EventProcessing,
			MaxAttempts: 9,
			ScheduledAt: scheduled,
		}
		e.fillDefaultsIfNeeded(5)

		if e.Status != EventProcessing {
			t.Errorf("Status = %q, want %q", e.Status, EventProcessing)
		}
		if e.MaxAttempts != 9 {
			t.Errorf("MaxAttempts = %d, want 9", e.MaxAttempts)
		}
		if !e.ScheduledAt.Equal(scheduled) {
			t.Errorf("ScheduledAt = %v, want %v", e.ScheduledAt, scheduled)
		}
	})

	t.Run("fills only the unset fields", func(t *testing.T) {
		e := &Event{Status: EventProcessed}
		e.fillDefaultsIfNeeded(7)

		if e.Status != EventProcessed {
			t.Errorf("Status = %q, want %q (should not be overwritten)", e.Status, EventProcessed)
		}
		if e.MaxAttempts != 7 {
			t.Errorf("MaxAttempts = %d, want 7", e.MaxAttempts)
		}
		if e.ScheduledAt.IsZero() {
			t.Error("ScheduledAt should have been filled")
		}
	})
}
