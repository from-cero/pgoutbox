package pgoutbox

import (
	"errors"
	"testing"
)

func TestApplyOptions(t *testing.T) {
	t.Run("uses defaults when no options given", func(t *testing.T) {
		cfg := applyOptions(nil)
		if cfg.maxAttempts != defaultConfig.maxAttempts {
			t.Errorf("maxAttempts = %d, want %d", cfg.maxAttempts, defaultConfig.maxAttempts)
		}
	})

	t.Run("WithMaxAttempts overrides the default", func(t *testing.T) {
		cfg := applyOptions([]Option{WithMaxAttempts(42)})
		if cfg.maxAttempts != 42 {
			t.Errorf("maxAttempts = %d, want 42", cfg.maxAttempts)
		}
	})

	t.Run("later options win", func(t *testing.T) {
		cfg := applyOptions([]Option{WithMaxAttempts(1), WithMaxAttempts(2)})
		if cfg.maxAttempts != 2 {
			t.Errorf("maxAttempts = %d, want 2", cfg.maxAttempts)
		}
	})
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name        string
		maxAttempts int
		wantErr     error
	}{
		{"positive is valid", 3, nil},
		{"one is valid", 1, nil},
		{"zero is invalid", 0, errMaxAttempts},
		{"negative is invalid", -1, errMaxAttempts},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := config{maxAttempts: tt.maxAttempts}
			err := c.validate()
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("validate() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("validate() = %v, want errors.Is %v", err, tt.wantErr)
			}
			if !errors.Is(err, errInvalidOutboxConfig) {
				t.Errorf("validate() error should wrap errInvalidOutboxConfig, got %v", err)
			}
		})
	}
}
