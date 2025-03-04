package pgkit

import (
	"context"

	"github.com/jackc/pgx/v5"
)

var ErrNoRows = pgx.ErrNoRows

var txContextKey = &struct{}{}

func WithTx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, txContextKey, tx)
}

func NoTx(ctx context.Context) context.Context {
	return WithTx(ctx, nil)
}

func TxFromContext(ctx context.Context) pgx.Tx {
	tx, _ := ctx.Value(txContextKey).(pgx.Tx)
	return tx
}

type errRow struct {
	err error
}

func (e errRow) Scan(dest ...interface{}) error { return e.err }
