package noop

import (
	"context"
	"testing"

	"github.com/from-cero/pgoutbox"
)

func TestPublisherPublishBatch(t *testing.T) {
	p := NewPublisher()

	t.Run("returns one nil result per event", func(t *testing.T) {
		events := []*pgoutbox.Event{{}, {}, {}}
		results := p.PublishBatch(context.Background(), events)
		if len(results) != len(events) {
			t.Fatalf("len(results) = %d, want %d", len(results), len(events))
		}
		for i, err := range results {
			if err != nil {
				t.Errorf("results[%d] = %v, want nil", i, err)
			}
		}
	})

	t.Run("empty batch yields an empty result slice", func(t *testing.T) {
		results := p.PublishBatch(context.Background(), nil)
		if len(results) != 0 {
			t.Errorf("len(results) = %d, want 0", len(results))
		}
	})
}

func TestPublisherClose(t *testing.T) {
	if err := NewPublisher().Close(); err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
}
