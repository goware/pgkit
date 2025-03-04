package pgkit

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Querier struct {
	pool *pgxpool.Pool
	Tx   pgx.Tx
	Scan *pgxscan.API
	SQL  *StatementBuilder
}

func (q *Querier) Exec(ctx context.Context, query Sqlizer) (pgconn.CommandTag, error) {
	// check for query errors
	if getErr, ok := query.(hasErr); ok && getErr.Err() != nil {
		return pgconn.CommandTag{}, wrapErr(getErr.Err())
	}

	sql, args, err := query.ToSql()
	if err != nil {
		return pgconn.CommandTag{}, wrapErr(err)
	}

	var tag pgconn.CommandTag
	if q.Tx != nil {
		tag, err = q.Tx.Exec(ctx, sql, args...)
	} else {
		tag, err = q.pool.Exec(ctx, sql, args...)
	}

	if err != nil {
		return pgconn.CommandTag{}, wrapErr(err)
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
	if q.Tx != nil {
		rows, err = q.Tx.Query(ctx, sql, args...)
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

	if q.Tx != nil {
		return q.Tx.QueryRow(ctx, sql, args...)
	} else {
		return q.pool.QueryRow(ctx, sql, args...)
	}
}

func (q *Querier) GetAll(ctx context.Context, query Sqlizer, dest interface{}) error {
	rows, err := q.QueryRows(ctx, query)
	if err != nil {
		return wrapErr(err)
	}
	return wrapErr(q.Scan.ScanAll(dest, rows))
}

func (q *Querier) GetOne(ctx context.Context, query Sqlizer, dest interface{}) error {
	switch builder := query.(type) {
	case sq.SelectBuilder:
		query = builder.Limit(1)
	case sq.DeleteBuilder:
		query = builder.Limit(1)
	}

	rows, err := q.QueryRows(ctx, query)
	if err != nil {
		return wrapErr(err)
	}
	return wrapErr(q.Scan.ScanOne(dest, rows))
}

func (q *Querier) BatchExec(ctx context.Context, queries Queries) ([]pgconn.CommandTag, error) {
	if len(queries) == 0 {
		return nil, wrapErr(fmt.Errorf("empty query"))
	}

	// check for query errors
	for _, query := range queries {
		if getErr, ok := query.(hasErr); ok && getErr.Err() != nil {
			return nil, wrapErr(getErr.Err())
		}
	}

	// Prepare queries
	batch := &pgx.Batch{}
	for _, query := range queries {
		sql, args, err := query.ToSql()
		if err != nil {
			return nil, wrapErr(err)
		}
		batch.Queue(sql, args...)
	}

	// Send batch
	var results pgx.BatchResults
	if q.Tx != nil {
		results = q.Tx.SendBatch(ctx, batch)
	} else {
		results = q.pool.SendBatch(ctx, batch)
	}
	defer results.Close()

	// Exec the number of times as we have queries in the batch so we may get the exec
	// result and potential error response.
	tags := make([]pgconn.CommandTag, 0, batch.Len())
	for i := 0; i < batch.Len(); i++ {
		tag, err := results.Exec()
		if err != nil {
			return tags, wrapErr(err)
		}
		tags = append(tags, tag)
	}

	return tags, nil
}

func (q *Querier) BatchQuery(ctx context.Context, queries Queries) (pgx.BatchResults, int, error) {
	if len(queries) == 0 {
		return nil, 0, wrapErr(fmt.Errorf("empty query"))
	}

	// check for query errors
	for _, query := range queries {
		if getErr, ok := query.(hasErr); ok && getErr.Err() != nil {
			return nil, 0, wrapErr(getErr.Err())
		}
	}

	// Prepare queries
	batch := &pgx.Batch{}
	for _, query := range queries {
		sql, args, err := query.ToSql()
		if err != nil {
			return nil, 0, wrapErr(err)
		}
		batch.Queue(sql, args...)
	}

	// Send batch
	var batchResults pgx.BatchResults
	if q.Tx != nil {
		batchResults = q.Tx.SendBatch(ctx, batch)
	} else {
		batchResults = q.pool.SendBatch(ctx, batch)
	}
	// defer results.Close()

	// NOTE: the caller of BatchQuery must close the `batchResults` themselves.
	return batchResults, batch.Len(), nil
}

// NOTE: WIP/experimentation to offer sugar to scan a batch of the same kinds of queries.
// func (q *Querier) BatchGetAll(ctx context.Context, queries Queries, dest interface{}) error {
// 	batchResults, batchLen, err := q.BatchQuery(ctx, queries)
// 	if err != nil {
// 		return wrapErr(err)
// 	}
// 	defer batchResults.Close()

// 	// for i, rows := range batchRows {
// 	// 	err := q.scan.ScanAll(dest[i], rows)
// 	// 	if err != nil {
// 	// 		return wrapErr(err)
// 	// 	}
// 	// }

// 	for i := 0; i < batchLen; i++ {
// 		rows, err := batchResults.Query()
// 		if err != nil {
// 			return wrapErr(err)
// 		}
// 		err = q.scan.ScanAll(dest, rows)
// 		if err != nil {
// 			return wrapErr(err)
// 		}
// 	}

// 	return nil
// }

type Sqlizer interface {
	// ToSql converts a runtime builder structure to an executable SQL query, returns:
	// query string, query values, and optional error
	ToSql() (string, []interface{}, error)
}

type Query Sqlizer

type Queries []Query

func (q *Queries) Add(query Sqlizer) {
	*q = append(*q, query)
}

func (q Queries) Len() int {
	return len(q)
}

// RawSQL allows you to build queries by hand easily. Note, it will auto-replace `?“ placeholders
// to postgres $X format. As well, if you run the same query over and over, consider to use
// `RawQuery(..)` instead, as it's a cached version of RawSQL.
type RawSQL struct {
	Query     string
	Args      []interface{}
	statement bool
	err       error
}

func (r RawSQL) Prepare(query string) (string, int, error) {
	if query == "" {
		return "", 0, fmt.Errorf("pgkit: empty query")
	}

	n := strings.Count(query, "?")
	if !r.statement && n != len(r.Args) {
		return "", 0, fmt.Errorf("pgkit: expecting %d args but received %d", n, len(r.Args))
	}

	parts := strings.Split(query, "?")

	q := bytes.Buffer{}
	for i, p := range parts {
		if p == "" {
			continue
		}
		q.WriteString(p)
		if i < n {
			q.WriteString(fmt.Sprintf("$%d", i+1))
		}
	}

	return q.String(), n, nil
}

func (r RawSQL) Err() error {
	return r.err
}

func (r RawSQL) ToSql() (string, []interface{}, error) {
	if r.Query == "" {
		return "", nil, fmt.Errorf("pgkit: empty query called with ToSql")
	}

	if r.err != nil {
		// error may have occured somewhere when building the query
		return "", nil, r.err
	}

	if r.statement {
		// for statement queries, we assume its prepared correctly by RawStatement
		return r.Query, r.Args, nil
	}

	if r.Args == nil || len(r.Args) == 0 {
		return r.Query, r.Args, nil // assume no params passed
	}

	q, _, err := r.Prepare(r.Query)
	if err != nil {
		return "", nil, err
	}

	// NOTE: below doesnt appear to be necessary, the driver below will do it
	// args := make([]interface{}, len(r.Args))
	// for i, arg := range r.Args {
	// 	v, ok := arg.(driver.Valuer)
	// 	if ok {
	// 		args[i] = v
	// 	} else {
	// 		args[i] = arg
	// 	}
	// }

	return q, r.Args, nil
}

// RawStatement allows you to build query statements by hand, where the query will remain the same
// but the arguments can change. The number of arguments must always be the same.
type RawStatement struct {
	query   RawSQL
	numArgs int
	err     error
}

func (r RawStatement) Err() error {
	return r.err
}

func (r RawStatement) GetQuery() string {
	return r.query.Query
}

func (r RawStatement) NumArgs() int {
	return r.numArgs
}

func (r RawStatement) Build(args ...interface{}) Sqlizer {
	if len(args) != r.numArgs {
		return RawSQL{err: fmt.Errorf("pgkit: invalid arguments passed to statement, expecting %d args but received %d", r.numArgs, len(args))}
	}
	return RawSQL{Query: r.query.Query, Args: args, statement: true}
}

func RawQuery(query string) RawStatement {
	rs := RawStatement{}
	rq := RawSQL{Query: query, statement: true}

	q, n, err := rq.Prepare(query)
	if err != nil {
		rs.query = rq
		rs.err = err
		return rs
	}

	rq.Query = q
	rs.query = rq
	rs.numArgs = n
	return rs
}

func RawQueryf(queryFormat string, a ...interface{}) RawStatement {
	return RawQuery(fmt.Sprintf(queryFormat, a...))
}
