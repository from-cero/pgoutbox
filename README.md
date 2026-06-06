# pgoutbox

Transactional outbox for Postgres in Go, with a relay and pluggable Kafka/Redpanda publishers.

`pgoutbox` lets you enqueue events in the same database transaction as your
business writes, then reliably relays them to a message broker. Because the
event row and your data are committed atomically, you never publish an event for
a transaction that rolled back, and never lose an event for one that committed.

## Features

- **Atomic enqueue** - write events in the same `pgx` transaction as your domain data.
- **Relay** - a background worker set that claims pending events and publishes them:
  - **poller** - scans for pending events and publishes them in batches.
  - **reaper** - reschedules events stuck in `processing` (after a crash or stall).
  - **janitor** - deletes processed events past a retention window.
  - **listener** - optional `LISTEN/NOTIFY` wake-up for near-real-time dispatch.
- **At-least-once delivery** with per-event retry budget and configurable backoff.
- **Pluggable publishers** - Kafka and Redpanda out of the box, plus a `noop`
  publisher for testing and a small `Publisher` interface for your own.
- **Observability hooks** for batch, reap, and janitor sweep statistics.

## Install

```sh
go get github.com/from-cero/pgoutbox
```

The publishers live in their own modules so you only pull in the broker client you use:

```sh
go get github.com/from-cero/pgoutbox/relay/publisher/kafka
# or
go get github.com/from-cero/pgoutbox/relay/publisher/redpanda
```

Requires Go 1.26+ and Postgres.

## Quick start

### 1. Create the schema

```go
store := postgres.NewStore("outbox_events")
if err := store.EnsureSchema(ctx, pool); err != nil {
    return err
}
```

The event ID is a `pgtype.UUID`, which `pgx` encodes and decodes natively, so no
extra codec registration is needed.

### 2. Enqueue events inside your transaction

```go
ob, err := pgoutbox.New(store)
if err != nil {
    return err
}

err = ob.ExecTx(ctx, pool, func(tx pgx.Tx) error {
    // ... your business writes on the same tx ...
    return ob.Enqueue(ctx, tx, &pgoutbox.Event{
        Type:    "order.created",
        Topic:   "orders",
        Payload: payload, // json.RawMessage
    })
})
```

`Enqueue` accepts any `pgoutbox.Querier` (`*pgxpool.Pool`, `*pgx.Conn`, or
`pgx.Tx`), so you can use your own transaction instead of `ExecTx`.

### 3. Run the relay

```go
publisher := kafka.NewPublisher([]string{"localhost:9092"})
defer publisher.Close()

listener := postgres.NewListener(pool, "outbox_events")
defer listener.Close()

rl, err := relay.New(
    store, publisher, logger,
    relay.WithListener(listener),
    relay.WithRetention(24*time.Hour),
)
if err != nil {
    return err
}

// Blocks until ctx is canceled; returns nil on graceful shutdown.
if err := rl.Run(ctx, pool); err != nil {
    return err
}
```

A complete, runnable example lives in [`internal/example`](internal/example).
Copy `internal/example/.env.example` to `.env`, point it at your Postgres and
broker, and run it with `go run .`.

## Configuration

### Outbox options

| Option | Default | Description |
| --- | --- | --- |
| `WithMaxAttempts(n)` | `3` | Publish attempts before an event is marked `failed`. |

### Relay options

| Option | Default | Description |
| --- | --- | --- |
| `WithPollerInterval(d)` | `5s` | How often the poller scans for pending events. |
| `WithPollerBatchSize(n)` | `100` | Max events claimed per poll cycle. |
| `WithShutdownGrace(d)` | `5s` | Grace period for in-flight bookkeeping on shutdown. |
| `WithReaperInterval(d)` | `30s` | How often the reaper checks for stuck events. |
| `WithReaperBatchSize(n)` | `100` | Max stuck events reclaimed per reap cycle. |
| `WithStuckTimeout(d)` | `1m` | How long an event may stay `processing` before being rescheduled. |
| `WithMaxReaps(n)` | `10` | Max times a stuck event is reaped before being parked as `failed`. |
| `WithRetention(d)` | `0` (off) | Delete processed events older than `d`. Zero keeps them forever. |
| `WithJanitorInterval(d)` | `5m` | How often the janitor sweeps. |
| `WithJanitorBatchSize(n)` | `100` | Max processed events deleted per sweep. |
| `WithBatchSize(n)` | - | Sets batch size for poller, reaper, and janitor at once. |
| `WithListener(l)` | none | Wake the poller on enqueue notifications. |
| `WithBackoff(fn)` | `DefaultBackoff` | Retry delay for a failed event (1-based attempt). |
| `WithTopicResolver(fn)` | event `Topic` | Map an event to its broker topic. |
| `WithHooks(h)` | none | Observability callbacks. |

`DefaultBackoff` doubles from 1s, caps at 1 minute, and adds equal jitter so
events failed by one broker blip do not retry in lockstep.

### Hooks

```go
relay.WithHooks(relay.Hooks{
    OnBatch: func(s relay.BatchStats) {
        logger.Info("batch", "claimed", s.Claimed,
            "published", s.Published, "failed", s.Failed)
    },
    OnReap:  func(s relay.ReapStats) { /* ... */ },
    OnSweep: func(s relay.SweepStats) { /* ... */ },
})
```

## Event lifecycle

An event moves through these states (the `Status` field):

- `pending` - enqueued, waiting to be claimed.
- `processing` - claimed by the poller, being published.
- `processed` - acknowledged by the broker.
- `failed` - exhausted its retry or reap budget.

Use `relay.RequeueFailed` to reset failed events back to `pending`, and
`store.Stats` to inspect queue depth.

## Delivery guarantees

Delivery is **at-least-once**. The relay claims an event, publishes it, then
marks it processed; a crash between publish and mark leaves the event in
`processing` until the reaper reschedules it, which can produce a duplicate.
Make your consumers idempotent.

Set `WithStuckTimeout` comfortably above the worst-case `PublishBatch` duration
(publisher write timeout times its internal retries) so the reaper does not
reschedule events that are still in flight.

## Custom publishers

Implement the `publisher.Publisher` interface:

```go
type Publisher interface {
    // Index-aligned with events: nil = acked, non-nil = that event's failure.
    PublishBatch(ctx context.Context, events []*pgoutbox.Event) []error
    Close() error
}
```

The `relay/publisher/noop` package provides a no-op implementation for tests.

## License

Released under the [MIT License](LICENSE).
