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

func (q *Querier) GetAll(ctx context.Context, query Sqlizer, dest interface{}) error {
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
	if q.tx != nil {
		results = q.tx.SendBatch(ctx, batch)
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
	if q.tx != nil {
		batchResults = q.tx.SendBatch(ctx, batch)
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
// 	// 	err := pgxscan.ScanAll(dest[i], rows)
// 	// 	if err != nil {
// 	// 		return wrapErr(err)
// 	// 	}
// 	// }

// 	for i := 0; i < batchLen; i++ {
// 		rows, err := batchResults.Query()
// 		if err != nil {
// 			return wrapErr(err)
// 		}
// 		err = pgxscan.ScanAll(dest, rows)
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
