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
	"github.com/davecgh/go-spew/spew"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/goware/pgkit"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
)

var (
	DB *pgkit.DB
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

	err = DB.Conn.Ping(context.Background())
	if err != nil {
		log.Fatal(fmt.Errorf("failed to ping db: %w", err))
	}
}

func TestPing(t *testing.T) {
	err := DB.Conn.Ping(context.Background())
	assert.NoError(t, err)
}

func TestSugarInsertAndSelectRows(t *testing.T) {
	truncateTable(t, "accounts")

	// Insert
	q1 := DB.SQL.Insert("accounts").Columns("name", "disabled").Values("peter", false)
	_, err := DB.Query.Exec(context.Background(), q1)
	assert.NoError(t, err)

	// Select
	var accounts []*Account
	q2 := DB.SQL.Select("*").From("accounts")
	err = DB.Query.GetAll(context.Background(), q2, &accounts)

	assert.NoError(t, err)
	assert.Len(t, accounts, 1)
	assert.True(t, accounts[0].ID != 0)
	assert.Equal(t, "peter", accounts[0].Name)
}

// TestInsertAndSelectRows is a more verbose version of TestSugarBasicInsertAndSelectRows
func TestInsertAndSelectRows(t *testing.T) {
	truncateTable(t, "accounts")

	// Insert
	insertq, args, err := DB.SQL.Insert("accounts").Columns("name", "disabled").Values("peter", false).ToSql()
	assert.NoError(t, err)

	_, err = DB.Conn.Exec(context.Background(), insertq, args...)
	assert.NoError(t, err)

	// Select all
	selectq, args := DB.SQL.Select("*").From("accounts").MustSql()

	var accounts []*Account
	err = pgxscan.Select(context.Background(), DB.Conn, &accounts, selectq, args...)

	assert.NoError(t, err)
	assert.Len(t, accounts, 1)
	assert.True(t, accounts[0].ID != 0)
	assert.Equal(t, "peter", accounts[0].Name)
}

func TestSugarInsertAndSelectRecords(t *testing.T) {
	truncateTable(t, "accounts")

	// Insert
	rec := &Account{
		Name:     "joe",
		Disabled: true,
	}

	q1 := DB.SQL.InsertRecord(rec) //, "accounts")
	_, err := DB.Query.Exec(context.Background(), q1)
	assert.NoError(t, err)

	// Select all
	var accounts []*Account
	q2 := DB.SQL.Select("*").From("accounts")
	err = DB.Query.GetAll(context.Background(), q2, &accounts)
	assert.NoError(t, err)
	assert.Len(t, accounts, 1)
	assert.True(t, accounts[0].ID != 0)
	assert.Equal(t, "joe", accounts[0].Name)

	// Select one -- into object
	account := &Account{}
	err = DB.Query.GetOne(context.Background(), q2, account)
	assert.NoError(t, err)
	assert.Len(t, accounts, 1)
	assert.True(t, accounts[0].ID != 0)
	assert.Equal(t, "joe", accounts[0].Name)

	// Select one -- into struct value
	var accountv Account
	err = DB.Query.GetOne(context.Background(), q2, &accountv)
	assert.NoError(t, err)
	assert.Len(t, accounts, 1)
	assert.True(t, accounts[0].ID != 0)
	assert.Equal(t, "joe", accounts[0].Name)
}

// TestInsertAndSelectRecords is a more verbose version of TestSugarInsertAndSelectRecords
func TestInsertAndSelectRecords(t *testing.T) {
	truncateTable(t, "accounts")

	// Create & insert record
	rec := &Account{
		Name:     "joe",
		Disabled: true,
	}

	// Map from object to columns / values
	cols, vals, err := pgkit.Map(rec)
	assert.NoError(t, err)

	// Build insert query
	insertq, args, err := DB.SQL.Insert("accounts").Columns(cols...).Values(vals...).ToSql()
	assert.NoError(t, err)

	// Exec the insert query
	_, err = DB.Conn.Exec(context.Background(), insertq, args...)
	assert.NoError(t, err)

	// Select query
	selectq, args, err := DB.SQL.Select("*").From("accounts").Where(sq.Eq{"name": "joe"}).ToSql()
	assert.NoError(t, err)

	// Build select query
	rows, err := DB.Conn.Query(context.Background(), selectq, args...)
	defer rows.Close()
	assert.NoError(t, err)

	// Scan result into *Account object
	a := &Account{}
	err = pgxscan.ScanOne(a, rows)
	assert.NoError(t, err)

	assert.True(t, a.ID != 0)
	assert.Equal(t, "joe", a.Name)
	assert.Equal(t, true, a.Disabled)

	// Insert another record, with short-hand syntax
	rec.Name = "joe2" // reusing same object, because it works
	cols, vals, err = pgkit.Map(rec)
	assert.NoError(t, err)
	insertq, args, err = DB.SQL.Insert("accounts").Columns(cols...).Values(vals...).ToSql()
	assert.NoError(t, err)
	_, err = DB.Conn.Exec(context.Background(), insertq, args...)
	assert.NoError(t, err)
}

func TestSugarQueryWithNoResults(t *testing.T) {
	q := DB.SQL.Select("*").From("accounts").Where(sq.Eq{"name": "no-match"})

	var account interface{}
	err := DB.Query.GetOne(context.Background(), q, &account)
	assert.True(t, errors.Is(err, pgkit.ErrNoRows))
}

func TestQueryWithNoResults(t *testing.T) {
	selectq, args, err := DB.SQL.Select("*").From("accounts").Where(sq.Eq{"name": "no-match"}).ToSql()
	assert.NoError(t, err)

	var accounts []*Account

	// shorthand
	{
		err = pgxscan.Select(context.Background(), DB.Conn, &accounts, selectq, args...)
		assert.NoError(t, err)
		assert.Len(t, accounts, 0)
	}

	// or, with more verbose method:
	{
		rows, err := DB.Conn.Query(context.Background(), selectq, args...)
		defer rows.Close()
		assert.NoError(t, err)

		err = pgxscan.ScanAll(&accounts, rows)

		assert.NoError(t, err)
		assert.Len(t, accounts, 0)
	}

	// scan one -- returning 'no rows' error
	{
		var a *Account
		err = pgxscan.Get(context.Background(), DB.Conn, a, selectq, args...)
		assert.True(t, errors.Is(err, pgx.ErrNoRows))
	}
}

// func TestInsertRowWithBigInt(t *testing.T) {
// 	truncateTable(t, "stats")

// 	s := &Stat{Key: "count", Num: dbtype.NewBigInt(2)}

// 	// Insert
// 	q1 := DB.SQL.InsertRecord(s, "stats")
// 	_, err := DB.Query.Exec(context.Background(), q1)
// 	assert.NoError(t, err)

// 	// Select
// 	var sout Stat
// 	q2 := DB.SQL.Select("*").From("stats")
// 	err = DB.Query.GetOne(context.Background(), q2, &sout)
// 	assert.NoError(t, err)
// 	assert.Equal(t, "count", sout.Key)
// 	assert.True(t, sout.Num.Int64() == 2)
// }

// TODO: ...... lets see how upper does it......

// TODO: can we have....... JSONBMap map[string]interface{} .. would be nice..?

// ................... i think what upper does is converts this type in the builder and scanner ..
// builder goes from map-type to jsonb and scanner goes from jsonb to map-type ..

// interesting... we could make sugar for this.. but.. i think id need my own scanner..?

func TestRowsWithJSONB(t *testing.T) {
	truncateTable(t, "logs")

	// etc := pgtype.JSONB{}
	// err := etc.Set(map[string]interface{}{"a": 1})
	// assert.NoError(t, err)

	// l := &Log{Message: "hi", Etc: etc}

	etc := JSONBMap{"a": 1}

	// Insert
	// q1 := DB.SQL.InsertRecord(l, "logs")
	q1 := DB.SQL.Insert("logs").Columns("message", "etc").Values("hi", etc)
	_, err := DB.Query.Exec(context.Background(), q1)
	assert.NoError(t, err)

	// Select
	var lout Log
	q2 := DB.SQL.Select("*").From("logs").Limit(1)
	err = DB.Query.GetOne(context.Background(), q2, &lout)
	assert.NoError(t, err)
	assert.Equal(t, "hi", lout.Message)
	// assert.True(t, sout.Num.Int64() == 2)

	fmt.Println("==>", lout.Etc)

	// m, ok := lout.Etc.Get().(map[string]interface{})
	// assert.True(t, ok)

	spew.Dump(lout)
}

// TODO: test jsonb, and big.Int custom types.....

// TODO: transactions.. ugh......

// TODO: batch support, right in here..? kinda makes sense..
