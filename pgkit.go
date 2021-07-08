package pgkit

import (
	"context"
	"fmt"
	"reflect"

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

type hasErr interface {
	Err() error
}

type Querier struct {
	DB  *DB
	SQL *StatementBuilder
}

func (q *Querier) Exec(ctx context.Context, query Sqlizer) (pgconn.CommandTag, error) {
	// check for query errors
	if getErr, ok := query.(hasErr); ok && getErr.Err() != nil {
		return nil, getErr.Err()
	}

	// get connection, sqlize, and execute
	conn := q.DB.Conn
	sql, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}
	return conn.Exec(ctx, sql, args...)
}

func (q *Querier) QueryRows(ctx context.Context, query Sqlizer) (pgx.Rows, error) {
	// check for query errors
	if getErr, ok := query.(hasErr); ok && getErr.Err() != nil {
		return nil, getErr.Err()
	}

	// get connection, sqlize, and query
	conn := q.DB.Conn
	sql, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}
	return conn.Query(ctx, sql, args...)
}

func (q *Querier) QueryRow(ctx context.Context, query Sqlizer) pgx.Row {
	// check for query errors
	if getErr, ok := query.(hasErr); ok && getErr.Err() != nil {
		return errRow{getErr.Err()}
	}

	// get connection, sqlize, and query
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

type hasDBTableName interface {
	DBTableName() string
}

type StatementBuilder struct {
	sq.StatementBuilderType
}

func (s *StatementBuilder) InsertRecord(record interface{}, optTableName ...string) InsertBuilder {
	tableName := ""
	if len(optTableName) > 0 {
		tableName = optTableName[0]
	} else {
		if getTableName, ok := record.(hasDBTableName); ok {
			tableName = getTableName.DBTableName()
		}
	}

	insert := sq.InsertBuilder(s.StatementBuilderType)

	cols, vals, err := Map(record)
	if err != nil {
		return InsertBuilder{InsertBuilder: insert, err: err}
	}

	return InsertBuilder{InsertBuilder: insert.Into(tableName).Columns(cols...).Values(vals...)}
}

func (s StatementBuilder) InsertRecords(records interface{}, optTableName ...string) InsertBuilder {
	insert := sq.InsertBuilder(s.StatementBuilderType)

	v := reflect.ValueOf(records)
	if v.Kind() != reflect.Slice {
		return InsertBuilder{InsertBuilder: insert, err: fmt.Errorf("records must be a slice type")}
	}

	if v.Len() == 0 {
		return InsertBuilder{InsertBuilder: insert, err: fmt.Errorf("records slice is empty")}
	}

	tableName := ""
	if len(optTableName) > 0 {
		tableName = optTableName[0]
	}

	for i := 0; i < v.Len(); i++ {
		r := v.Index(i)
		record := r.Interface()

		if i == 0 && tableName == "" {
			if getTableName, ok := record.(hasDBTableName); ok {
				tableName = getTableName.DBTableName()
			}
		}

		cols, vals, err := Map(record)
		if err != nil {
			return InsertBuilder{InsertBuilder: insert, err: err}
		}

		if i == 0 {
			insert = insert.Columns(cols...).Values(vals...)
		} else {
			insert = insert.Values(vals...)
		}
	}

	return InsertBuilder{InsertBuilder: insert.Into(tableName)}
}

// TODO: add UpdateRecord and UpdateRecords ...

type InsertBuilder struct {
	sq.InsertBuilder
	err error
}

func (b InsertBuilder) Err() error {
	return b.err
}
