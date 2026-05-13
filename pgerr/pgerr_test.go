package pgerr_test

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/goware/pgkit/v2/pgerr"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

func TestIsErrorNoRows(t *testing.T) {
	t.Run("pgx.ErrNoRows", func(t *testing.T) {
		assert.True(t, pgerr.IsErrorNoRows(pgx.ErrNoRows))
	})
	t.Run("sql.ErrNoRows", func(t *testing.T) {
		assert.True(t, pgerr.IsErrorNoRows(sql.ErrNoRows))
	})
	t.Run("wrapped pgx.ErrNoRows", func(t *testing.T) {
		assert.True(t, pgerr.IsErrorNoRows(fmt.Errorf("scan failed: %w", pgx.ErrNoRows)))
	})
	t.Run("nil", func(t *testing.T) {
		assert.False(t, pgerr.IsErrorNoRows(nil))
	})
	t.Run("unrelated error", func(t *testing.T) {
		assert.False(t, pgerr.IsErrorNoRows(errors.New("boom")))
	})
}

func TestIsUniqueViolation(t *testing.T) {
	t.Run("unique violation with constraint name", func(t *testing.T) {
		err := &pgconn.PgError{Code: pgerrcode.UniqueViolation, ConstraintName: "uq_users_email"}
		name, ok := pgerr.IsUniqueViolation(err)
		assert.True(t, ok)
		assert.Equal(t, "uq_users_email", name)
	})
	t.Run("wrapped unique violation", func(t *testing.T) {
		pgErr := &pgconn.PgError{Code: pgerrcode.UniqueViolation, ConstraintName: "uq_users_email"}
		wrapped := fmt.Errorf("insert failed: %w", pgErr)
		name, ok := pgerr.IsUniqueViolation(wrapped)
		assert.True(t, ok)
		assert.Equal(t, "uq_users_email", name)
	})
	t.Run("unique violation without constraint name", func(t *testing.T) {
		err := &pgconn.PgError{Code: pgerrcode.UniqueViolation}
		name, ok := pgerr.IsUniqueViolation(err)
		assert.True(t, ok)
		assert.Equal(t, "", name)
	})
	t.Run("different pg error code", func(t *testing.T) {
		err := &pgconn.PgError{Code: pgerrcode.ForeignKeyViolation, ConstraintName: "fk_x"}
		name, ok := pgerr.IsUniqueViolation(err)
		assert.False(t, ok)
		assert.Equal(t, "", name)
	})
	t.Run("non pg error", func(t *testing.T) {
		name, ok := pgerr.IsUniqueViolation(errors.New("boom"))
		assert.False(t, ok)
		assert.Equal(t, "", name)
	})
	t.Run("nil", func(t *testing.T) {
		name, ok := pgerr.IsUniqueViolation(nil)
		assert.False(t, ok)
		assert.Equal(t, "", name)
	})
}

func TestIsFatal(t *testing.T) {
	t.Run("class 00 successful completion is non-fatal", func(t *testing.T) {
		assert.False(t, pgerr.IsFatal(&pgconn.PgError{Code: pgerrcode.SuccessfulCompletion}))
	})
	t.Run("class 01 warning is non-fatal", func(t *testing.T) {
		assert.False(t, pgerr.IsFatal(&pgconn.PgError{Code: pgerrcode.Warning}))
	})
	t.Run("class 02 no data is non-fatal", func(t *testing.T) {
		assert.False(t, pgerr.IsFatal(&pgconn.PgError{Code: pgerrcode.NoData}))
	})
	t.Run("class 23 unique violation is non-fatal", func(t *testing.T) {
		assert.False(t, pgerr.IsFatal(&pgconn.PgError{Code: pgerrcode.UniqueViolation}))
	})
	t.Run("class 08 connection exception is fatal", func(t *testing.T) {
		assert.True(t, pgerr.IsFatal(&pgconn.PgError{Code: pgerrcode.ConnectionException}))
	})
	t.Run("class 57 operator intervention is fatal", func(t *testing.T) {
		assert.True(t, pgerr.IsFatal(&pgconn.PgError{Code: pgerrcode.QueryCanceled}))
	})
	t.Run("class XX internal error is fatal", func(t *testing.T) {
		assert.True(t, pgerr.IsFatal(&pgconn.PgError{Code: pgerrcode.InternalError}))
	})
	t.Run("wrapped fatal pg error", func(t *testing.T) {
		pgErr := &pgconn.PgError{Code: pgerrcode.ConnectionFailure}
		assert.True(t, pgerr.IsFatal(fmt.Errorf("query: %w", pgErr)))
	})
	t.Run("non pg error", func(t *testing.T) {
		assert.False(t, pgerr.IsFatal(errors.New("boom")))
	})
	t.Run("nil", func(t *testing.T) {
		assert.False(t, pgerr.IsFatal(nil))
	})
	t.Run("short code is non-fatal", func(t *testing.T) {
		assert.False(t, pgerr.IsFatal(&pgconn.PgError{Code: "X"}))
	})
}
