package pgkit

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4/pgxpool"
)

var (
	SQL = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
)

type Database struct {
	*pgxpool.Pool
}

type Config struct {
	Database          string   `toml:"database"`
	Hosts             []string `toml:"hosts"`
	Username          string   `toml:"username"`
	Password          string   `toml:"password"`
	DebugQueries      bool     `toml:"debug_queries"`
	ReportQueryErrors bool     `toml:"report_query_errors"`
	MaxOpenConns      int      `toml:"max_open_conns"`
	MaxIdleConns      int      `toml:"max_idle_conns"`
	ConnMaxLifetime   string   `toml:"conn_max_lifetime"`
}

func Connect(appName string, cfg Config) (*Database, error) {
	if len(cfg.Hosts) == 0 {
		return nil, fmt.Errorf("invalid config param: hosts")
	}
	host := cfg.Hosts[0] // TODO: Why do we have multiple hosts anyway?

	uri := fmt.Sprintf("postgres://%s:%s@%s/%s?application_name=%v",
		cfg.Username,
		cfg.Password,
		host,
		cfg.Database,
		appName,
	)

	pgxCfg, err := pgxpool.ParseConfig(uri)
	if err != nil {
		return nil, err
	}
	// TODO... check values, etc. setup, etc.
	// pgCfg.MaxConns = int32(cfg.MaxIdleConns)

	return ConnectWithPGXConfig(appName, pgxCfg)
}

func ConnectWithPGXConfig(appName string, pgxConfig *pgxpool.Config) (*Database, error) {
	pool, err := pgxpool.ConnectConfig(context.Background(), pgxConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to db: %w", err)
	}

	db := &Database{
		Pool: pool,
	}

	return db, nil
}
