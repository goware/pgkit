package pgkit

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type DB struct {
	Conn  *pgxpool.Pool
	SQL   *StatementBuilder
	Query *Querier
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

func Connect(appName string, cfg Config) (*DB, error) {
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

func ConnectWithPGXConfig(appName string, pgxConfig *pgxpool.Config) (*DB, error) {
	pool, err := pgxpool.ConnectConfig(context.Background(), pgxConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to db: %w", err)
	}

	db := &DB{
		Conn: pool,
	}

	db.SQL = &StatementBuilder{StatementBuilderType: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
	db.Query = &Querier{DB: db, SQL: db.SQL}

	return db, nil
}

type Sqlizer interface {
	// ToSql converts a runtime builder structure to an executable SQL query, returns:
	// query string, query values, and optional error
	ToSql() (string, []interface{}, error)
}

type Querier struct {
	DB  *DB
	SQL *StatementBuilder
}

func (q *Querier) Exec(ctx context.Context, query Sqlizer) (pgconn.CommandTag, error) {
	conn := q.DB.Conn
	sql, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}
	return conn.Exec(ctx, sql, args...)
}

func (q *Querier) QueryRows(ctx context.Context, query Sqlizer) (pgx.Rows, error) {
	conn := q.DB.Conn
	sql, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}
	return conn.Query(ctx, sql, args...)
}

func (q *Querier) QueryRow(ctx context.Context, query Sqlizer) pgx.Row {
	conn := q.DB.Conn
	sql, args, err := query.ToSql()
	if err != nil {
		return errRow{err}
	}
	return conn.QueryRow(ctx, sql, args...)
}

func (q *Querier) GetAll(ctx context.Context, query sq.SelectBuilder, dest interface{}) error {
	rows, err := q.QueryRows(ctx, query)
	if err != nil {
		return err
	}
	return pgxscan.ScanAll(dest, rows)
}

func (q *Querier) GetOne(ctx context.Context, query sq.SelectBuilder, dest interface{}) error {
	rows, err := q.QueryRows(ctx, query.Limit(1))
	if err != nil {
		return err
	}
	return pgxscan.ScanOne(dest, rows)
}

type HasDBTableName interface {
	DBTableName() string
}

type StatementBuilder struct {
	sq.StatementBuilderType
}

func (s *StatementBuilder) InsertRecord(record interface{}, optTableName ...string) sq.InsertBuilder {
	tableName := ""
	if len(optTableName) > 0 {
		tableName = optTableName[0]
	} else {
		if getTableName, ok := record.(HasDBTableName); ok {
			tableName = getTableName.DBTableName()
		}
	}

	cols, vals, err := Map(record)
	if err != nil {
		// return insert statement without setting the record data
		// TODO: we need to record this error somehow though, likely log, or some error thing
		return s.StatementBuilderType.Insert(tableName)
	}

	return s.StatementBuilderType.Insert(tableName).Columns(cols...).Values(vals...)
}

// TODO: add InsertRecords

// TODO: add UpdateRecord and UpdateRecords ...
