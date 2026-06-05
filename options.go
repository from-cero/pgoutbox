package pgoutbox

import (
	"errors"
	"fmt"
)

// Option represents a functional option for configuring the outbox.
type Option func(*config)

// WithMaxAttempts sets the maximum number of publish attempts before an event is marked failed.
func WithMaxAttempts(n int) Option { return func(c *config) { c.maxAttempts = n } }

type config struct {
	maxAttempts int
}

var defaultConfig = config{
	maxAttempts: 3,
}

func applyOptions(opts []Option) config {
	cfg := defaultConfig
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

func (c *config) validate() error {
	var errs []error
	if c.maxAttempts <= 0 {
		errs = append(errs, fmt.Errorf("%w, got %d", errMaxAttempts, c.maxAttempts))
	}

	if len(errs) > 0 {
		return fmt.Errorf("%w: %w", errInvalidOutboxConfig, errors.Join(errs...))
	}
	return nil
}
