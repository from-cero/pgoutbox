-- Schema for the transactional outbox table used by pgoutbox.
-- Safe to run repeatedly: every statement is guarded with IF NOT EXISTS.

CREATE TABLE IF NOT EXISTS outbox_events (
    id           BIGINT      PRIMARY KEY,
    data_id      TEXT        NOT NULL,
    data_type    TEXT        NOT NULL,
    event_type   TEXT        NOT NULL,
    topic        TEXT        NOT NULL DEFAULT '',
    payload      JSONB       NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'processing', 'processed', 'failed')),
    retry_count  INTEGER     NOT NULL DEFAULT 0,
    max_retries  INTEGER     NOT NULL DEFAULT 3,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Speeds up the relay poll: "pending events that are due, oldest first".
CREATE INDEX IF NOT EXISTS idx_outbox_events_pending
    ON outbox_events (scheduled_at)
    WHERE status = 'pending';

-- Speeds up the reaper scan: "events stuck in processing the longest".
CREATE INDEX IF NOT EXISTS idx_outbox_events_processing
    ON outbox_events (updated_at)
    WHERE status = 'processing';
