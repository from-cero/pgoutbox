package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	pgxuuid "github.com/vgarvardt/pgx-google-uuid/v5"
)

// RegisterTypes teaches a connection's type map how to encode and decode the
// google/uuid.UUID used for the event id. pgx does not support google/uuid out
// of the box, so this must run on every connection before the store is used.
//
// Wire it into your pool config's AfterConnect hook:
//
//	cfg, err := pgxpool.ParseConfig(dsn)
//	// ...
//	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
//		return postgres.RegisterTypes(ctx, conn)
//	}
func RegisterTypes(_ context.Context, conn *pgx.Conn) error {
	pgxuuid.Register(conn.TypeMap())
	return nil
}

// RegisterTypesOnMap registers the type mappings directly on a pgtype.Map, for
// callers that manage the type map themselves rather than through AfterConnect.
func RegisterTypesOnMap(m *pgtype.Map) {
	pgxuuid.Register(m)
}
