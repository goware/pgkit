package pgkit

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/georgysavva/scany/v2/dbscan"
	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

type DB struct {
	Conn  *pgxpool.Pool
	SQL   *StatementBuilder
	Query *Querier
}

// TxQuery returns a new Querier that uses the given pgx.Tx
func (d *DB) TxQuery(tx pgx.Tx) *Querier {
	return &Querier{Tx: tx, SQL: d.SQL, pool: d.Conn, Scan: d.Query.Scan}
}

// TxQueryFromContext returns a new Querier that uses the pgx.Tx in the given context
func (d *DB) TxQueryFromContext(ctx context.Context) *Querier {
	tx := TxFromContext(ctx)
	if tx == nil {
		return nil
	}
	return d.TxQuery(tx)
}

type Config struct {
	Database        string `toml:"database"`
	Host            string `toml:"host"`
	Username        string `toml:"username"`
	Password        string `toml:"password"`
	MaxConns        int32  `toml:"max_conns"`
	MinConns        int32  `toml:"min_conns"`
	ConnMaxLifetime string `toml:"conn_max_lifetime"`  // ie. "3600s" or "1h"
	ConnMaxIdleTime string `toml:"conn_max_idle_time"` // ie. "1800s" or "30m"

	Override func(cfg *pgx.ConnConfig) `toml:"-"`
	Tracer   pgx.QueryTracer
}

func Connect(appName string, cfg Config) (*DB, error) {
	poolCfg, err := pgxpool.ParseConfig(getConnectURI(appName, cfg))
	if err != nil {
		return nil, wrapErr(err)
	}

	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 4
	}
	if cfg.ConnMaxLifetime == "" {
		cfg.ConnMaxLifetime = "1h"
	}
	if cfg.ConnMaxIdleTime == "" {
		cfg.ConnMaxIdleTime = "30m"
	}

	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns

	poolCfg.MaxConnLifetime, err = time.ParseDuration(cfg.ConnMaxLifetime)
	if err != nil {
		return nil, fmt.Errorf("pgkit: config invalid conn_max_lifetime value: %w", err)
	}

	poolCfg.MaxConnIdleTime, err = time.ParseDuration(cfg.ConnMaxIdleTime)
	if err != nil {
		return nil, fmt.Errorf("pgkit: config invalid conn_max_idle_time value: %w", err)
	}

	poolCfg.MaxConnLifetimeJitter = time.Minute // Prevent connections from being closed at the same time.
	poolCfg.HealthCheckPeriod = time.Minute

	poolCfg.ConnConfig.Tracer = cfg.Tracer
	// override settings on *pgx.ConnConfig object
	if cfg.Override != nil {
		cfg.Override(poolCfg.ConnConfig)
	}

	return ConnectWithPGX(appName, poolCfg)
}

func ConnectWithPGX(appName string, pgxConfig *pgxpool.Config) (*DB, error) {
	pool, err := pgxpool.NewWithConfig(context.Background(), pgxConfig)
	if err != nil {
		return nil, fmt.Errorf("pgkit: failed to connect to db: %w", err)
	}

	db := &DB{
		Conn: pool,
	}

	db.SQL = &StatementBuilder{StatementBuilderType: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}

	// TODO: It might be handy to let developers disable this option in "dev" mode. However,
	//       true is a good default value, see https://github.com/goware/pgkit/issues/13.
	allowUnknownColumns := true

	dbScanAPI, err := pgxscan.NewDBScanAPI(dbscan.WithAllowUnknownColumns(allowUnknownColumns))
	if err != nil {
		return nil, wrapErr(err)
	}

	pgxScanAPI, err := pgxscan.NewAPI(dbScanAPI)
	if err != nil {
		return nil, wrapErr(err)
	}

	db.Query = &Querier{pool: db.Conn, Scan: pgxScanAPI, SQL: db.SQL}

	return db, nil
}

func ConnectWithStdlib(appName string, cfg Config) (*sql.DB, error) {
	connCfg, err := pgx.ParseConfig(getConnectURI(appName, cfg))
	if err != nil {
		return nil, err
	}

	db := stdlib.OpenDB(*connCfg)
	return db, nil
}

func getConnectURI(appName string, cfg Config) string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?application_name=%v",
		cfg.Username,
		cfg.Password,
		cfg.Host,
		cfg.Database,
		appName,
	)
}

type hasErr interface {
	Err() error
}

type hasDBTableName interface {
	DBTableName() string
}

// wrapErr wraps an error so we can add the "pgkit:" prefix to messages, this way in case of a
// db oriented error, a developer can quickly identify the source of the problem being
// related to db app logic.
func wrapErr(err error) error {
	if err == nil {
		return nil
	} else {
		return fmt.Errorf("pgkit: %w", err)
	}
}
