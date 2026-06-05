package postgres

import (
	"errors"
)

var (
	// ErrLengthMismatch is returned when paired slice arguments differ in length.
	ErrLengthMismatch = errors.New("paired arguments must have equal length")

	// ErrInvalidPayload is returned by Insert before touching the database when the
	// event carries an invalid JSON payload; pass json.RawMessage("{}") for empty payloads.
	ErrInvalidPayload = errors.New("event payload is invalid")
)
