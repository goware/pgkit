package pgkit_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	mrand "math/rand"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/goware/pgkit"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
)

var (
	DB *pgkit.Database
)

func init() {
	mrand.Seed(time.Now().UnixNano())

	var err error
	DB, err = pgkit.Connect("pgkit_test", pgkit.Config{
		Database: "pgkit_test",
		Hosts:    []string{"localhost"},
		Username: "postgres",
		Password: "postgres",
	})
	if err != nil {
		log.Fatal(fmt.Errorf("failed to connect db: %w", err))
	}

	err = DB.Ping(context.Background())
	if err != nil {
		log.Fatal(fmt.Errorf("failed to ping db: %w", err))
	}
}

func TestPing(t *testing.T) {
	err := DB.Ping(context.Background())
	assert.NoError(t, err)
}

func TestBasicInsertAndSelect(t *testing.T) {
	truncateTable(t, "accounts")

	// Insert
	insertq, args, err := DB.SQL.Insert("accounts").Columns("name", "disabled").Values("peter", false).ToSql()
	assert.NoError(t, err)

	_, err = DB.Exec(context.Background(), insertq, args...)
	assert.NoError(t, err)

	// Select all
	selectq, args := DB.SQL.Select("*").From("accounts").MustSql()

	// rows, err := DB.Query(context.Background(), selectq, args...)
	// assert.NoError(t, err)

	var accounts []*Account

	// TODO ..
	// err = DB.Scan.Select(context.Background(), &accounts, selectq, args...)

	err = pgxscan.Select(context.Background(), DB, &accounts, selectq, args...)

	assert.NoError(t, err)
	assert.Len(t, accounts, 1)
	assert.Equal(t, "peter", accounts[0].Name)
}

func TestInsertingRecords(t *testing.T) {

	// Create & insert record
	rec := &Account{
		Name:     "joe",
		Disabled: true,
	}

	// Map from object to columns / values
	cols, vals, err := pgkit.Map(rec, nil)
	assert.NoError(t, err)

	// Build insert query
	insertq, args, err := DB.SQL.Insert("accounts").Columns(cols...).Values(vals...).ToSql()
	assert.NoError(t, err)

	// Exec the insert query
	_, err = DB.Exec(context.Background(), insertq, args...)
	assert.NoError(t, err)

	// Select query
	selectq, args, err := DB.SQL.Select("*").From("accounts").Where(sq.Eq{"name": "joe"}).ToSql()
	assert.NoError(t, err)

	// Build select query
	rows, err := DB.Query(context.Background(), selectq, args...)
	defer rows.Close()
	assert.NoError(t, err)

	// Scan result into *Account object
	a := &Account{}
	err = pgxscan.ScanOne(a, rows)
	assert.NoError(t, err)

	assert.True(t, a.ID != 0)
	assert.Equal(t, "joe", a.Name)
	assert.Equal(t, true, a.Disabled)
}

func TestQueryWithNoResults(t *testing.T) {
	selectq, args, err := DB.SQL.Select("*").From("accounts").Where(sq.Eq{"name": "no-match"}).ToSql()
	assert.NoError(t, err)

	var accounts []*Account

	// shorthand
	{
		err = pgxscan.Select(context.Background(), DB, &accounts, selectq, args...)
		assert.NoError(t, err)
		assert.Len(t, accounts, 0)
	}

	// or, with more verbose method:
	{
		rows, err := DB.Query(context.Background(), selectq, args...)
		defer rows.Close()
		assert.NoError(t, err)

		err = pgxscan.ScanAll(&accounts, rows)

		assert.NoError(t, err)
		assert.Len(t, accounts, 0)
	}

	// scan one -- returning 'no rows' error
	{
		var a *Account
		err = pgxscan.Get(context.Background(), DB, a, selectq, args...)
		assert.True(t, errors.Is(err, pgx.ErrNoRows))
	}
}
