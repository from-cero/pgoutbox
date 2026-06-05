package kafka

import "errors"

// errBatchAborted marks an event that was not sent because another event in the
// same batch was rejected as oversized before kafka-go dispatched the batch.
// It is transient: the relay retries these events on the next poll.
var errBatchAborted = errors.New("batch aborted by oversized message")
