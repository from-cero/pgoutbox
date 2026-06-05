package relay

import (
	"errors"
)

var (
	// ErrNilRelayStore is returned by New when a nil Store is provided.
	ErrNilRelayStore = errors.New("relay's store should not be nil")

	// ErrNilRelayPublisher is returned by New when a nil Publisher is provided.
	ErrNilRelayPublisher = errors.New("relay's publisher should not be nil")

	// ErrNoTopic is returned when neither the event's Topic field nor the configured
	// topic resolver produces a non-empty topic string for an event.
	ErrNoTopic = errors.New("no topic resolved for event")

	errInvalidRelayConfig  = errors.New("invalid relay config")
	errPollerInterval      = errors.New("poller.interval must be greater than 0")
	errPollerBatchSize     = errors.New("poller.batchSize must be greater than 0")
	errPollerShutdownGrace = errors.New("poller.shutdownGrace must be greater than 0")
	errReaperInterval      = errors.New("reaper.interval must be greater than 0")
	errReaperBatchSize     = errors.New("reaper.batchSize must be greater than 0")
	errReaperStuckTimeout  = errors.New("reaper.stuckTimeout must be greater than 0")
	errReaperMaxReaps      = errors.New("reaper.maxReaps must be greater than 0")
	errJanitorRetention    = errors.New("janitor.retention must not be negative")
	errJanitorInterval     = errors.New("janitor.interval must be greater than 0")
	errJanitorBatchSize    = errors.New("janitor.batchSize must be greater than 0")

	// guards against publishers that violate the PublishBatch contract by returning fewer results than events
	errMisalignedResults = errors.New("publisher returned misaligned batch results")
)

// Permanent marks err as not retryable.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

// IsPermanent reports whether err, or any error it wraps, was marked with Permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

type permanentError struct {
	err error
}

func (p permanentError) Error() string { return p.err.Error() }
func (p permanentError) Unwrap() error { return p.err }
