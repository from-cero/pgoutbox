package relay

import (
	"errors"
	"fmt"
	"testing"
)

func TestPermanent(t *testing.T) {
	t.Run("nil stays nil", func(t *testing.T) {
		if Permanent(nil) != nil {
			t.Error("Permanent(nil) should be nil")
		}
	})

	t.Run("marks an error permanent", func(t *testing.T) {
		err := Permanent(errors.New("bad config"))
		if !IsPermanent(err) {
			t.Error("IsPermanent should report true for a marked error")
		}
	})

	t.Run("preserves the message", func(t *testing.T) {
		err := Permanent(errors.New("bad config"))
		if err.Error() != "bad config" {
			t.Errorf("Error() = %q, want %q", err.Error(), "bad config")
		}
	})

	t.Run("unwraps to the original error", func(t *testing.T) {
		sentinel := errors.New("root")
		if !errors.Is(Permanent(sentinel), sentinel) {
			t.Error("errors.Is should find the wrapped error")
		}
	})

	t.Run("detected through additional wrapping", func(t *testing.T) {
		err := fmt.Errorf("context: %w", Permanent(errors.New("root")))
		if !IsPermanent(err) {
			t.Error("IsPermanent should see through fmt.Errorf wrapping")
		}
	})
}

func TestIsPermanent(t *testing.T) {
	t.Run("plain errors are not permanent", func(t *testing.T) {
		if IsPermanent(errors.New("transient")) {
			t.Error("a plain error should not be permanent")
		}
	})

	t.Run("nil is not permanent", func(t *testing.T) {
		if IsPermanent(nil) {
			t.Error("nil should not be permanent")
		}
	})
}
