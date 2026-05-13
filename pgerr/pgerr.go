// Package pgerr provides PostgreSQL error inspection helpers built on
// pgconn.PgError. SQLSTATE code constants are re-exported from
// github.com/jackc/pgerrcode — import that package directly if you need
// the full table of codes or class-membership helpers.
package pgerr

import (
	"database/sql"
	"errors"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// IsErrorNoRows reports whether err is or wraps pgx.ErrNoRows or sql.ErrNoRows.
func IsErrorNoRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows)
}

// IsUniqueViolation reports whether err is a Postgres unique constraint
// violation (SQLSTATE 23505). When true, the returned string is the
// constraint name reported by the server (empty if the driver did not
// surface it).
func IsUniqueViolation(err error) (string, bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != pgerrcode.UniqueViolation {
		return "", false
	}
	return pgErr.ConstraintName, true
}

// IsFatal reports whether err is a Postgres error that should be treated as
// fatal for retry/alerting purposes. SQLSTATE classes 00 (success), 01
// (warning), 02 (no data), and 23 (integrity constraint violation) are
// non-fatal; every other class is fatal. Non-PgError errors return false
// (no opinion).
func IsFatal(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || len(pgErr.Code) < 2 {
		return false
	}
	switch pgErr.Code[:2] {
	case "00", "01", "02", "23":
		return false
	}
	return true
}
