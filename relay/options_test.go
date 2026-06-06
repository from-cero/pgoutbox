package relay

import (
	"errors"
	"testing"
	"time"

	"github.com/from-cero/pgoutbox"
)

func TestApplyOptions(t *testing.T) {
	t.Run("defaults when no options", func(t *testing.T) {
		cfg := applyOptions(nil)
		if cfg.poller.interval != 5*time.Second {
			t.Errorf("poller.interval = %v, want 5s", cfg.poller.interval)
		}
		if cfg.reaper.maxReaps != 10 {
			t.Errorf("reaper.maxReaps = %d, want 10", cfg.reaper.maxReaps)
		}
		if cfg.janitor.retention != 0 {
			t.Errorf("janitor.retention = %v, want 0", cfg.janitor.retention)
		}
	})

	t.Run("individual options apply", func(t *testing.T) {
		cfg := applyOptions([]Option{
			WithPollerInterval(time.Second),
			WithPollerBatchSize(7),
			WithShutdownGrace(2 * time.Second),
			WithReaperInterval(11 * time.Second),
			WithReaperBatchSize(8),
			WithStuckTimeout(3 * time.Minute),
			WithMaxReaps(4),
			WithRetention(time.Hour),
			WithJanitorInterval(time.Minute),
			WithJanitorBatchSize(9),
		})
		if cfg.poller.interval != time.Second || cfg.poller.batchSize != 7 ||
			cfg.poller.shutdownGrace != 2*time.Second {
			t.Errorf("poller config = %+v", cfg.poller)
		}
		if cfg.reaper.interval != 11*time.Second || cfg.reaper.batchSize != 8 ||
			cfg.reaper.stuckTimeout != 3*time.Minute || cfg.reaper.maxReaps != 4 {
			t.Errorf("reaper config = %+v", cfg.reaper)
		}
		if cfg.janitor.retention != time.Hour || cfg.janitor.interval != time.Minute || cfg.janitor.batchSize != 9 {
			t.Errorf("janitor config = %+v", cfg.janitor)
		}
	})

	t.Run("WithBatchSize sets all three actors", func(t *testing.T) {
		cfg := applyOptions([]Option{WithBatchSize(50)})
		if cfg.poller.batchSize != 50 || cfg.reaper.batchSize != 50 || cfg.janitor.batchSize != 50 {
			t.Errorf("batch sizes = %d/%d/%d, want all 50",
				cfg.poller.batchSize, cfg.reaper.batchSize, cfg.janitor.batchSize)
		}
	})

	t.Run("custom-behavior options apply", func(t *testing.T) {
		l := &fakeListener{}
		var hookCalled bool
		hooks := Hooks{OnBatch: func(BatchStats) { hookCalled = true }}
		cfg := applyOptions([]Option{
			WithListener(l),
			WithHooks(hooks),
			WithBackoff(func(int) time.Duration { return 42 * time.Second }),
			WithTopicResolver(func(*pgoutbox.Event) string { return "resolved" }),
		})
		if cfg.listener != l {
			t.Error("listener not set")
		}
		cfg.hooks.batch(BatchStats{})
		if !hookCalled {
			t.Error("hooks not set")
		}
		if cfg.backoff(1) != 42*time.Second {
			t.Error("backoff not set")
		}
		if cfg.topicResolver(&pgoutbox.Event{}) != "resolved" {
			t.Error("topicResolver not set")
		}
	})

	t.Run("returns an independent config without mutating the default", func(t *testing.T) {
		// poller/reaper/janitor are value fields, so applyOptions deep-copies
		// them and an option must not leak into the package-global defaultConfig
		// or bleed across calls. This guards against making the sub-config fields
		// pointers again, which would alias the shared default.
		want := defaultConfig.poller.batchSize
		cfg := applyOptions([]Option{WithPollerBatchSize(want + 1)})
		if cfg.poller.batchSize != want+1 {
			t.Fatalf("option not applied: poller.batchSize = %d, want %d", cfg.poller.batchSize, want+1)
		}
		if defaultConfig.poller.batchSize != want {
			t.Errorf("applyOptions mutated defaultConfig: poller.batchSize = %d, want %d",
				defaultConfig.poller.batchSize, want)
		}
		if again := applyOptions(nil); again.poller.batchSize != want {
			t.Errorf("config not independent across calls: poller.batchSize = %d, want %d",
				again.poller.batchSize, want)
		}
	})
}

func TestConfigValidate(t *testing.T) {
	valid := func() *config { return applyOptions(nil) }

	t.Run("default config is valid", func(t *testing.T) {
		c := valid()
		if err := c.validate(); err != nil {
			t.Fatalf("default config invalid: %v", err)
		}
	})

	t.Run("zero retention is valid (janitor disabled)", func(t *testing.T) {
		c := valid()
		c.janitor.retention = 0
		if err := c.validate(); err != nil {
			t.Fatalf("zero retention should be valid: %v", err)
		}
	})

	tests := []struct {
		name    string
		mutate  func(*config)
		wantErr error
	}{
		{"poller interval", func(c *config) { c.poller.interval = 0 }, errPollerInterval},
		{"poller batch size", func(c *config) { c.poller.batchSize = 0 }, errPollerBatchSize},
		{"poller shutdown grace", func(c *config) { c.poller.shutdownGrace = 0 }, errPollerShutdownGrace},
		{"reaper interval", func(c *config) { c.reaper.interval = -1 }, errReaperInterval},
		{"reaper batch size", func(c *config) { c.reaper.batchSize = 0 }, errReaperBatchSize},
		{"reaper stuck timeout", func(c *config) { c.reaper.stuckTimeout = 0 }, errReaperStuckTimeout},
		{"reaper max reaps", func(c *config) { c.reaper.maxReaps = 0 }, errReaperMaxReaps},
		{"negative retention", func(c *config) { c.janitor.retention = -1 }, errJanitorRetention},
		{"janitor interval", func(c *config) { c.janitor.interval = 0 }, errJanitorInterval},
		{"janitor batch size", func(c *config) { c.janitor.batchSize = 0 }, errJanitorBatchSize},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := valid()
			tt.mutate(c)
			err := c.validate()
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("validate() = %v, want errors.Is %v", err, tt.wantErr)
			}
			if !errors.Is(err, errInvalidRelayConfig) {
				t.Errorf("error should wrap errInvalidRelayConfig, got %v", err)
			}
		})
	}

	t.Run("reports every violation at once", func(t *testing.T) {
		c := config{} // all zero, every check fails
		err := c.validate()
		for _, want := range []error{
			errPollerInterval, errPollerBatchSize, errPollerShutdownGrace,
			errReaperInterval, errReaperBatchSize, errReaperStuckTimeout, errReaperMaxReaps,
			errJanitorInterval, errJanitorBatchSize,
		} {
			if !errors.Is(err, want) {
				t.Errorf("combined error missing %v", want)
			}
		}
	})
}

func TestDefaultBackoff(t *testing.T) {
	t.Run("stays within the equal-jitter band for each attempt", func(t *testing.T) {
		// base doubles from 1s, capped at 1m; result is in [base/2, base/2 + base/2] = [base/2, base].
		cases := []struct {
			attempt int
			base    time.Duration
		}{
			{1, time.Second},
			{2, 2 * time.Second},
			{3, 4 * time.Second},
			{4, 8 * time.Second},
			{5, 16 * time.Second},
			{6, 32 * time.Second},
			{7, time.Minute},  // 64s capped to 60s
			{20, time.Minute}, // far past the cap
		}
		for _, c := range cases {
			lo := c.base / 2
			hi := c.base/2 + (c.base/2 + 1)
			for i := 0; i < 200; i++ {
				d := DefaultBackoff(c.attempt)
				if d < lo || d >= hi {
					t.Fatalf("attempt %d: backoff %v out of [%v, %v)", c.attempt, d, lo, hi)
				}
			}
		}
	})

	t.Run("clamps non-positive attempts to attempt 1", func(t *testing.T) {
		for _, attempt := range []int{0, -5} {
			d := DefaultBackoff(attempt)
			if d < time.Second/2 || d >= time.Second+1 {
				t.Errorf("attempt %d: backoff %v not in attempt-1 band", attempt, d)
			}
		}
	})
}
