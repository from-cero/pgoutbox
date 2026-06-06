// Command example runs the full outbox pipeline against a scratch table:
// it enqueues a few events inside transactions, then runs the relay (poller,
// reaper, janitor and LISTEN/NOTIFY listener) until interrupted.
//
// Configuration lives in config.go and is read from the environment.
//
// The scratch table is dropped and recreated on every run, so the schema is
// always current.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/from-cero/pgoutbox"
	"github.com/from-cero/pgoutbox/postgres"
	"github.com/from-cero/pgoutbox/relay"
	"github.com/from-cero/pgoutbox/relay/publisher/kafka"
)

type SamplePayload struct {
	Order  string `json:"order"`
	Amount int    `json:"amount"`
}

// sampleType and sampleTopic label every event the example and benchmark enqueue.
const (
	sampleType  = "test"
	sampleTopic = "outbox-test"
)

func main() {
	if err := run(); err != nil {
		slog.Error("example failed", "error", err.Error())
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := loadConfig()

	pool, err := connect(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer pool.Close()

	store := postgres.NewStore(cfg.Table)
	if err := resetSchema(ctx, pool, store, cfg.Table); err != nil {
		return err
	}

	ob, err := pgoutbox.New(store)
	if err != nil {
		return fmt.Errorf("create outbox: %w", err)
	}

	if cfg.Bench {
		return runEnqueueBench(ctx, ob, pool, store, cfg, logger)
	}

	go func() {
		if err := enqueueSamples(ctx, ob, pool); err != nil {
			slog.Error("example failed", "error", err.Error())
		}
	}()

	publisher := kafka.NewPublisher(cfg.Brokers)
	defer func() { _ = publisher.Close() }()

	listener := postgres.NewListener(pool, cfg.Table)
	defer listener.Close()

	// meter accumulates per-batch stats so we can report publish throughput
	// while sweeping WithPollerBatchSize. Its observe runs on the poller
	// goroutine via OnBatch; report runs on main after Relay.Run returns, and
	// Run's internal WaitGroup synchronizes the two.
	meter := &throughputMeter{}
	rl, err := relay.New(
		store, publisher, logger,
		relay.WithListener(listener),
		relay.WithRetention(cfg.Retention),
		relay.WithHooks(
			relay.Hooks{
				OnBatch: func(s relay.BatchStats) { meter.observe(s, logger) },
			},
		),
	)
	if err != nil {
		return fmt.Errorf("create relay: %w", err)
	}

	logger.Info("relay running, ctrl-c to stop")
	if err := rl.Run(ctx, pool); err != nil { // returns nil on graceful shutdown
		return fmt.Errorf("run relay: %w", err)
	}
	meter.report(logger)

	// The run context is canceled by now; report the final queue state on a fresh one.
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := store.Stats(sctx, pool)
	if err != nil {
		return fmt.Errorf("final stats: %w", err)
	}
	logger.Info(
		"final queue state", "pending", st.Pending, "processing", st.Processing, "failed", st.Failed,
	)
	return nil
}

func connect(ctx context.Context, cfg *config, logger *slog.Logger) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	if cfg.Trace {
		poolCfg.ConnConfig.Tracer = &sqlTracer{logger: logger}
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}

// resetSchema drops and recreates the scratch table so library schema changes
// always apply. Do NOT copy this into a real service; there, EnsureSchema (or
// your migration tool) is the whole story.
func resetSchema(ctx context.Context, pool *pgxpool.Pool, store *postgres.Store, table string) error {
	if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS "+table+" CASCADE"); err != nil {
		return fmt.Errorf("drop table: %w", err)
	}
	if _, err := pool.Exec(ctx, "DROP FUNCTION IF EXISTS "+table+"_notify() CASCADE"); err != nil {
		return fmt.Errorf("drop notify function: %w", err)
	}
	if err := store.EnsureSchema(ctx, pool); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}
	return nil
}

// enqueueSamples writes events the way a real service would: inside the same
// transaction as the business writes. Ids are assigned by the database.
func enqueueSamples(ctx context.Context, ob *pgoutbox.Outbox, pool *pgxpool.Pool) error {
	for i := range 1000 {
		payload, err := json.Marshal(
			&SamplePayload{
				Order:  "order-" + strconv.Itoa(i),
				Amount: i * 10,
			},
		)
		if err != nil {
			return fmt.Errorf("marshal payload: %w", err)
		}

		err = ob.ExecTx(
			ctx, pool, func(tx pgx.Tx) error {
				// Business writes would go here, on the same tx.
				return ob.Enqueue(
					ctx, tx, &pgoutbox.Event{
						Type:    sampleType,
						Topic:   sampleTopic,
						Payload: payload,
					},
				)
			},
		)
		if err != nil {
			return fmt.Errorf("enqueue event %d: %w", i, err)
		}
	}
	return nil
}

// throughputMeter aggregates poll-batch stats to measure publish throughput.
// events_per_sec is computed against PublishDuration only, so it reflects the
// publisher path (the part WithPollerBatchSize tunes), not the two Postgres
// round trips around it. Watch lost: a nonzero value means the reaper reclaimed
// in-flight events mid-publish, the signal that the batch is too large for the
// configured stuck timeout.
type throughputMeter struct {
	batches      atomic.Int64
	published    atomic.Int64
	failed       atomic.Int64
	lost         atomic.Int64
	publishNanos atomic.Int64
}

func (m *throughputMeter) observe(s relay.BatchStats, logger *slog.Logger) {
	m.batches.Add(1)
	m.published.Add(int64(s.Published))
	m.failed.Add(int64(s.Failed))
	m.lost.Add(int64(s.Lost))
	m.publishNanos.Add(int64(s.PublishDuration))

	logger.Info(
		"batch done",
		"claimed", s.Claimed, "published", s.Published, "failed", s.Failed,
		"lost", s.Lost, "publish_ms", s.PublishDuration.Milliseconds(),
		"events_per_sec", eventsPerSec(s.Published, s.PublishDuration),
	)
}

func (m *throughputMeter) report(logger *slog.Logger) {
	published := m.published.Load()
	publishDur := time.Duration(m.publishNanos.Load())
	logger.Info(
		"throughput summary",
		"batches", m.batches.Load(),
		"published", published,
		"failed", m.failed.Load(),
		"lost", m.lost.Load(),
		"publish_seconds", publishDur.Seconds(),
		"events_per_sec", eventsPerSec(int(published), publishDur),
	)
}

func eventsPerSec(events int, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(events) / d.Seconds()
}

// runEnqueueBench measures enqueue throughput for two patterns over the same
// total event count: serial (one event per transaction) and batched (BenchBatch
// events per transaction). Both run on a single goroutine, so the difference
// isolates the per-commit fsync cost that dominates the write path - batching
// amortizes one COMMIT over many inserts. The relay is not started in this mode.
func runEnqueueBench(
	ctx context.Context, ob *pgoutbox.Outbox, pool *pgxpool.Pool,
	store *postgres.Store, cfg *config, logger *slog.Logger,
) error {
	// A fixed payload keeps JSON marshaling out of the timed loop so the numbers
	// reflect the database write path, not encoding.
	payload, err := json.Marshal(&SamplePayload{Order: "bench", Amount: 1})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	logger.Info("enqueue benchmark starting", "total", cfg.BenchTotal, "batch_size", cfg.BenchBatch)

	serial, err := benchEnqueue(ctx, ob, pool, payload, cfg.BenchTotal, 1)
	if err != nil {
		return fmt.Errorf("serial enqueue: %w", err)
	}
	logResult(logger, "serial", cfg.BenchTotal, 1, 1, serial)

	// Reset so the batched run starts from an empty table, comparing on equal footing.
	if err := resetSchema(ctx, pool, store, cfg.Table); err != nil {
		return err
	}

	batched, err := benchEnqueue(ctx, ob, pool, payload, cfg.BenchTotal, cfg.BenchBatch)
	if err != nil {
		return fmt.Errorf("batched enqueue: %w", err)
	}
	logResult(logger, "batched", cfg.BenchTotal, cfg.BenchBatch, 1, batched)

	if err := resetSchema(ctx, pool, store, cfg.Table); err != nil {
		return err
	}

	concurrent, err := benchEnqueueConcurrent(ctx, ob, pool, payload, cfg.BenchTotal, cfg.BenchConc)
	if err != nil {
		return fmt.Errorf("concurrent enqueue: %w", err)
	}
	logResult(logger, "concurrent", cfg.BenchTotal, 1, cfg.BenchConc, concurrent)

	if serial > 0 {
		fields := []any{}
		if batched > 0 {
			fields = append(fields, "batched_over_serial", float64(serial)/float64(batched))
		}
		if concurrent > 0 {
			fields = append(fields, "concurrent_over_serial", float64(serial)/float64(concurrent))
		}
		logger.Info("enqueue speedup", fields...)
	}
	return nil
}

// benchEnqueue inserts total events, perTx events per transaction, and returns
// the wall time taken. perTx of 1 is the serial pattern.
func benchEnqueue(
	ctx context.Context, ob *pgoutbox.Outbox, pool *pgxpool.Pool,
	payload json.RawMessage, total, perTx int,
) (time.Duration, error) {
	start := time.Now()
	for i := 0; i < total; i += perTx {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		n := min(perTx, total-i)
		err := ob.ExecTx(ctx, pool, func(tx pgx.Tx) error {
			for range n {
				e := &pgoutbox.Event{Type: sampleType, Topic: sampleTopic, Payload: payload}
				if err := ob.Enqueue(ctx, tx, e); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return 0, fmt.Errorf("enqueue at offset %d: %w", i, err)
		}
	}
	return time.Since(start), nil
}

// benchEnqueueConcurrent inserts total events using workers goroutines that
// share the pool, each enqueuing serially (one event per transaction). Unlike
// the serial and batched patterns this exercises the other throughput axis:
// concurrent commits ride Postgres group commit, so aggregate throughput
// decouples from single-connection commit latency until WAL, the pool, or CPU
// saturates.
func benchEnqueueConcurrent(
	ctx context.Context, ob *pgoutbox.Outbox, pool *pgxpool.Pool,
	payload json.RawMessage, total, workers int,
) (time.Duration, error) {
	workers = max(workers, 1)
	errs := make([]error, workers)

	var wg sync.WaitGroup
	start := time.Now()
	for w := range workers {
		// Spread total across workers; the first total%workers get one extra.
		share := total / workers
		if w < total%workers {
			share++
		}
		if share == 0 {
			continue
		}
		wg.Add(1)
		go func(w, share int) {
			defer wg.Done()
			for range share {
				if err := ctx.Err(); err != nil {
					errs[w] = err
					return
				}
				err := ob.ExecTx(ctx, pool, func(tx pgx.Tx) error {
					e := &pgoutbox.Event{Type: sampleType, Topic: sampleTopic, Payload: payload}
					return ob.Enqueue(ctx, tx, e)
				})
				if err != nil {
					errs[w] = fmt.Errorf("worker %d: %w", w, err)
					return
				}
			}
		}(w, share)
	}
	wg.Wait()
	elapsed := time.Since(start)

	for _, err := range errs {
		if err != nil {
			return 0, err
		}
	}
	return elapsed, nil
}

func logResult(logger *slog.Logger, mode string, total, perTx, workers int, elapsed time.Duration) {
	logger.Info(
		"enqueue result",
		"mode", mode,
		"events", total,
		"events_per_tx", perTx,
		"workers", workers,
		"elapsed_ms", elapsed.Milliseconds(),
		"events_per_sec", eventsPerSec(total, elapsed),
	)
}

type sqlTracer struct {
	logger *slog.Logger
}

func (t *sqlTracer) TraceQueryStart(
	ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData,
) context.Context {
	t.logger.Info("sql", "query", data.SQL, "args", data.Args)
	return ctx
}

func (t *sqlTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
