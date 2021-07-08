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
	"github.com/goware/pgkit/dbtype"
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

func TestRowsWithJSONB(t *testing.T) {
	truncateTable(t, "logs")

	etc := dbtype.JSONBMap{"a": 1}

	// Insert
	q1 := DB.SQL.Insert("logs").Columns("message", "etc").Values("hi", etc)
	_, err := DB.Query.Exec(context.Background(), q1)
	assert.NoError(t, err)

	// Select
	var lout Log
	q2 := DB.SQL.Select("*").From("logs")
	err = DB.Query.GetOne(context.Background(), q2, &lout)
	assert.NoError(t, err)
	assert.Equal(t, "hi", lout.Message)
	assert.Equal(t, float64(1), lout.Etc["a"]) // json will convert numbers to float64
}

func TestRecordsWithJSONB(t *testing.T) {
	truncateTable(t, "logs")

	log := &Log{
		Message: "recording",
		Etc:     dbtype.JSONBMap{"place": "Toronto"},
	}

	// Insert
	q1 := DB.SQL.InsertRecord(log, "logs")
	_, err := DB.Query.Exec(context.Background(), q1)
	assert.NoError(t, err)

	// Select
	var lout Log
	q2 := DB.SQL.Select("*").From("logs").Limit(1)
	err = DB.Query.GetOne(context.Background(), q2, &lout)
	assert.NoError(t, err)
	assert.Equal(t, "recording", lout.Message)
	assert.Equal(t, "Toronto", lout.Etc["place"])
}

func TestRecordsWithJSONStruct(t *testing.T) {
	truncateTable(t, "articles")

	article := &Article{
		Author: "Gary",
		Content: Content{
			Title: "How to cook pizza",
			Body:  "flour+water+salt+yeast+cheese",
		},
	}

	// Assert record mapping for nested jsonb struct
	cols, _, err := pgkit.Map(article)
	assert.NoError(t, err)
	assert.Equal(t, []string{"author", "content"}, cols)

	// Insert record
	q1 := DB.SQL.InsertRecord(article, "articles")
	_, err = DB.Query.Exec(context.Background(), q1)
	assert.NoError(t, err)

	// Select record
	aout := &Article{}
	q2 := DB.SQL.Select("*").From("articles")
	err = DB.Query.GetOne(context.Background(), q2, aout)
	assert.NoError(t, err)
	assert.Equal(t, "Gary", aout.Author)
	assert.Equal(t, "How to cook pizza", aout.Content.Title)
}

func TestRowsWithBigInt(t *testing.T) {
	truncateTable(t, "stats")

	{
		stat := &Stat{Key: "count", Num: dbtype.NewBigInt(2)}

		// Insert
		q1 := DB.SQL.InsertRecord(stat, "stats")
		_, err := DB.Query.Exec(context.Background(), q1)
		assert.NoError(t, err)

		// Select
		var sout Stat
		q2 := DB.SQL.Select("*").From("stats").Where(sq.Eq{"key": "count"})
		err = DB.Query.GetOne(context.Background(), q2, &sout)
		assert.NoError(t, err)
		assert.Equal(t, "count", sout.Key)
		assert.True(t, sout.Num.Int64() == 2)
	}

	// another one, big number this time
	{
		stat := &Stat{Key: "count2", Num: dbtype.NewBigIntFromString("12323942398472837489234", 0)}

		// Insert
		q1 := DB.SQL.InsertRecord(stat, "stats")
		_, err := DB.Query.Exec(context.Background(), q1)
		assert.NoError(t, err)

		// Select
		var sout Stat
		q2 := DB.SQL.Select("*").From("stats").Where(sq.Eq{"key": "count2"})
		err = DB.Query.GetOne(context.Background(), q2, &sout)
		assert.NoError(t, err)
		assert.Equal(t, "count2", sout.Key)
		assert.True(t, sout.Num.String() == "12323942398472837489234")
	}
}

// TODO: transactions..

// TODO: batch support, right in here..? kinda makes sense..
