package main

import (
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// Config holds everything the example needs to talk to Postgres and Kafka.
// All values come from the environment, with defaults that target the local
// scratch cluster, so the example runs without any setup.
//
// Environment variables (all optional):
//
//	PGOUTBOX_DSN     postgres connection string
//	PGOUTBOX_BROKERS comma-separated kafka brokers
//	PGOUTBOX_TRACE   set to any value to log every SQL statement
type Config struct {
	// Table is the scratch outbox table; it is dropped and recreated on
	// every run, so the schema is always current.
	Table string
	// DSN is the postgres connection string.
	DSN string
	// Brokers is the list of kafka brokers to publish to.
	Brokers []string
	// Trace, when true, logs every SQL statement.
	Trace bool
	// Retention is how long published events are kept before the janitor
	// deletes them.
	Retention time.Duration
}

// Default values target a local cluster. Point them at a real environment via
// PGOUTBOX_DSN and PGOUTBOX_BROKERS so no host or credential is hardcoded here.
const (
	defaultTable     = "outbox_events"
	defaultDSN       = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	defaultBrokers   = "localhost:9092"
	defaultRetention = 24 * time.Hour
)

// LoadConfig reads the configuration from the environment, falling back to the
// local scratch defaults. A .env file in the working directory is loaded first,
// if present; real environment variables always take precedence over it.
func LoadConfig() Config {
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		slog.Warn("load .env", "error", err.Error())
	}
	return Config{
		Table:     defaultTable,
		DSN:       envOr("PGOUTBOX_DSN", defaultDSN),
		Brokers:   strings.Split(envOr("PGOUTBOX_BROKERS", defaultBrokers), ","),
		Trace:     os.Getenv("PGOUTBOX_TRACE") != "",
		Retention: defaultRetention,
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
