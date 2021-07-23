package pgkit

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jackc/pgx/v4/stdlib"
)

type DB struct {
	Conn  *pgxpool.Pool
	SQL   *StatementBuilder
	Query *Querier
}

type Config struct {
	Database        string `toml:"database"`
	Host            string `toml:"host"`
	Username        string `toml:"username"`
	Password        string `toml:"password"`
	MaxConns        int32  `toml:"max_conns"`
	MinConns        int32  `toml:"min_conns"`
	ConnMaxLifetime string `toml:"conn_max_lifetime"` // ie. "1800s" or "1h"
}

func Connect(appName string, cfg Config) (*DB, error) {
	poolCfg, err := pgxpool.ParseConfig(getConnectURI(appName, cfg))
	if err != nil {
		return nil, wrapErr(err)
	}

	if cfg.MaxConns == 0 {
		cfg.MaxConns = 4
	}
	if cfg.ConnMaxLifetime == "" {
		cfg.ConnMaxLifetime = "1h"
	}

	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns

	poolCfg.MaxConnLifetime, err = time.ParseDuration(cfg.ConnMaxLifetime)
	if err != nil {
		return nil, fmt.Errorf("pgkit: config invalid conn_max_lifetime value: %w", err)
	}

	poolCfg.MaxConnIdleTime = time.Minute * 30

	poolCfg.HealthCheckPeriod = time.Minute

	return ConnectWithPGX(appName, poolCfg)
}

func ConnectWithPGX(appName string, pgxConfig *pgxpool.Config) (*DB, error) {
	pool, err := pgxpool.ConnectConfig(context.Background(), pgxConfig)
	if err != nil {
		return nil, fmt.Errorf("pgkit: failed to connect to db: %w", err)
	}

	db := &DB{
		Conn: pool,
	}

	db.SQL = &StatementBuilder{StatementBuilderType: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
	db.Query = &Querier{pool: db.Conn, SQL: db.SQL}

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

func (d *DB) TxQuery(tx pgx.Tx) *Querier {
	return &Querier{tx: tx, SQL: d.SQL}
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
	pool *pgxpool.Pool
	tx   pgx.Tx
	SQL  *StatementBuilder
}

func (q *Querier) Exec(ctx context.Context, query Sqlizer) (pgconn.CommandTag, error) {
	// check for query errors
	if getErr, ok := query.(hasErr); ok && getErr.Err() != nil {
		return nil, wrapErr(getErr.Err())
	}

	sql, args, err := query.ToSql()
	if err != nil {
		return nil, wrapErr(err)
	}

	var tag pgconn.CommandTag
	if q.tx != nil {
		tag, err = q.tx.Exec(ctx, sql, args...)
	} else {
		tag, err = q.pool.Exec(ctx, sql, args...)
	}

	if err != nil {
		return nil, wrapErr(err)
	}
	return tag, nil
}

func (q *Querier) QueryRows(ctx context.Context, query Sqlizer) (pgx.Rows, error) {
	// check for query errors
	if getErr, ok := query.(hasErr); ok && getErr.Err() != nil {
		return nil, wrapErr(getErr.Err())
	}

	sql, args, err := query.ToSql()
	if err != nil {
		return nil, wrapErr(err)
	}

	var rows pgx.Rows
	if q.tx != nil {
		rows, err = q.tx.Query(ctx, sql, args...)
	} else {
		rows, err = q.pool.Query(ctx, sql, args...)
	}

	if err != nil {
		return nil, wrapErr(err)
	}
	return rows, nil
}

func (q *Querier) QueryRow(ctx context.Context, query Sqlizer) pgx.Row {
	// check for query errors
	if getErr, ok := query.(hasErr); ok && getErr.Err() != nil {
		return errRow{wrapErr(getErr.Err())}
	}

	sql, args, err := query.ToSql()
	if err != nil {
		return errRow{wrapErr(err)}
	}

	if q.tx != nil {
		return q.tx.QueryRow(ctx, sql, args...)
	} else {
		return q.pool.QueryRow(ctx, sql, args...)
	}
}

func (q *Querier) GetAll(ctx context.Context, query sq.SelectBuilder, dest interface{}) error {
	rows, err := q.QueryRows(ctx, query)
	if err != nil {
		return wrapErr(err)
	}
	return wrapErr(pgxscan.ScanAll(dest, rows))
}

func (q *Querier) GetOne(ctx context.Context, query sq.SelectBuilder, dest interface{}) error {
	rows, err := q.QueryRows(ctx, query.Limit(1))
	if err != nil {
		return wrapErr(err)
	}
	return wrapErr(pgxscan.ScanOne(dest, rows))
}

type hasDBTableName interface {
	DBTableName() string
}

type StatementBuilder struct {
	sq.StatementBuilderType
}

func (s *StatementBuilder) InsertRecord(record interface{}, optTableName ...string) InsertBuilder {
	tableName := getTableName(record, optTableName...)
	insert := sq.InsertBuilder(s.StatementBuilderType)

	cols, vals, err := Map(record)
	if err != nil {
		return InsertBuilder{InsertBuilder: insert, err: wrapErr(err)}
	}

	return InsertBuilder{InsertBuilder: insert.Into(tableName).Columns(cols...).Values(vals...)}
}

func (s StatementBuilder) InsertRecords(recordsSlice interface{}, optTableName ...string) InsertBuilder {
	insert := sq.InsertBuilder(s.StatementBuilderType)

	v := reflect.ValueOf(recordsSlice)
	if v.Kind() != reflect.Slice {
		return InsertBuilder{InsertBuilder: insert, err: wrapErr(fmt.Errorf("records must be a slice type"))}
	}
	if v.Len() == 0 {
		return InsertBuilder{InsertBuilder: insert, err: wrapErr(fmt.Errorf("records slice is empty"))}
	}

	tableName := ""
	if len(optTableName) > 0 {
		tableName = optTableName[0]
	}

	for i := 0; i < v.Len(); i++ {
		record := v.Index(i).Interface()

		if i == 0 && tableName == "" {
			if getTableName, ok := record.(hasDBTableName); ok {
				tableName = getTableName.DBTableName()
			}
		}

		cols, vals, err := Map(record)
		if err != nil {
			return InsertBuilder{InsertBuilder: insert, err: wrapErr(err)}
		}

		if i == 0 {
			insert = insert.Columns(cols...).Values(vals...)
		} else {
			insert = insert.Values(vals...)
		}
	}

	return InsertBuilder{InsertBuilder: insert.Into(tableName)}
}

func (s StatementBuilder) UpdateRecord(record interface{}, whereExpr sq.Eq, optTableName ...string) UpdateBuilder {
	tableName := getTableName(record, optTableName...)
	update := sq.UpdateBuilder(s.StatementBuilderType)

	cols, vals, err := Map(record)
	if err != nil {
		return UpdateBuilder{UpdateBuilder: update, err: wrapErr(err)}
	}
	valMap, err := createMap(cols, vals, nil)
	if err != nil {
		return UpdateBuilder{UpdateBuilder: update, err: wrapErr(err)}
	}

	return UpdateBuilder{UpdateBuilder: update.Table(tableName).SetMap(valMap).Where(whereExpr)}
}

func (s StatementBuilder) UpdateRecordColumns(record interface{}, whereExpr sq.Eq, filterCols []string, optTableName ...string) UpdateBuilder {
	tableName := getTableName(record, optTableName...)
	update := sq.UpdateBuilder(s.StatementBuilderType)

	cols, vals, err := Map(record)
	if err != nil {
		return UpdateBuilder{UpdateBuilder: update, err: wrapErr(err)}
	}

	// when filter is empty or nil, update the entire record
	var filter []string
	if filterCols == nil || len(filterCols) == 0 {
		filter = nil
	} else {
		filter = filterCols
	}

	valMap, err := createMap(cols, vals, filter)
	if err != nil {
		return UpdateBuilder{UpdateBuilder: update, err: wrapErr(err)}
	}

	return UpdateBuilder{UpdateBuilder: update.Table(tableName).SetMap(valMap).Where(whereExpr)}
}

type InsertBuilder struct {
	sq.InsertBuilder
	err error
}

func (b InsertBuilder) Err() error { return b.err }

type UpdateBuilder struct {
	sq.UpdateBuilder
	err error
}

func (b UpdateBuilder) Err() error { return b.err }

func getTableName(record interface{}, optTableName ...string) string {
	tableName := ""
	if len(optTableName) > 0 {
		tableName = optTableName[0]
	} else {
		if getTableName, ok := record.(hasDBTableName); ok {
			tableName = getTableName.DBTableName()
		}
	}
	return tableName
}

func createMap(k []string, v []interface{}, filterK []string) (map[string]interface{}, error) {
	if len(k) != len(v) {
		return nil, fmt.Errorf("key and value pair is not of equal length")
	}

	m := make(map[string]interface{}, len(k))

	for i := 0; i < len(k); i++ {
		if filterK == nil || len(filterK) == 0 {
			m[k[i]] = v[i]
			continue
		}
		for x := 0; x < len(filterK); x++ {
			if filterK[x] == k[i] {
				m[k[i]] = v[i]
				break
			}
		}
	}

	return m, nil
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
