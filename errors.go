package pgoutbox

import (
	"errors"
)

var (
	// ErrNilOutboxStore is returned when a nil Store is provided to New.
	ErrNilOutboxStore = errors.New("outbox's store should not be nil")

	errInvalidOutboxConfig = errors.New("invalid outbox config")
	errMaxAttempts         = errors.New("maxAttempts must be greater than 0")
)
