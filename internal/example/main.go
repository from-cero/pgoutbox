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
	cfg := LoadConfig()

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
	if err := enqueueSamples(ctx, ob, pool); err != nil {
		return err
	}

	publisher := kafka.NewPublisher(cfg.Brokers)
	defer func() { _ = publisher.Close() }()

	listener := postgres.NewListener(pool, cfg.Table)
	defer listener.Close()

	pl, err := relay.New(
		store, publisher, logger,
		relay.WithListener(listener),
		relay.WithRetention(cfg.Retention),
		relay.WithHooks(relay.Hooks{
			OnBatch: func(s relay.BatchStats) {
				logger.Info(
					"batch done", "claimed", s.Claimed, "published", s.Published,
					"failed", s.Failed, "publish_ms", s.PublishDuration.Milliseconds(),
				)
			},
		}),
	)
	if err != nil {
		return fmt.Errorf("create poller: %w", err)
	}

	logger.Info("poller running, ctrl-c to stop")
	if err := pl.Run(ctx, pool); err != nil { // returns nil on graceful shutdown
		return fmt.Errorf("run poller: %w", err)
	}

	// The run context is canceled by now; report the final queue state on a
	// fresh one.
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

func connect(ctx context.Context, cfg Config, logger *slog.Logger) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	if cfg.Trace {
		poolCfg.ConnConfig.Tracer = &sqlTracer{logger: logger}
	}
	// The event id is a google/uuid.UUID, which pgx cannot encode or decode
	// until the codec is registered on each connection.
	poolCfg.AfterConnect = postgres.RegisterTypes

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
	for i := range 10 {
		payload, err := json.Marshal(&SamplePayload{
			Order:  "order-" + strconv.Itoa(i),
			Amount: i * 10,
		})
		if err != nil {
			return fmt.Errorf("marshal payload: %w", err)
		}

		err = ob.ExecTx(ctx, pool, func(tx pgx.Tx) error {
			// Business writes would go here, on the same tx.
			return ob.Enqueue(ctx, tx, &pgoutbox.Event{
				Type:    "test",
				Topic:   "outbox-test",
				Payload: payload,
			})
		})
		if err != nil {
			return fmt.Errorf("enqueue event %d: %w", i, err)
		}
	}
	return nil
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
