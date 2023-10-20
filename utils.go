package pgkit

import "github.com/jackc/pgx/v5"

var ErrNoRows = pgx.ErrNoRows

type errRow struct {
	err error
}

func (e errRow) Scan(dest ...interface{}) error { return e.err }
