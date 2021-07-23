package pgkit_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	mrand "math/rand"
	"sort"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/goware/pgkit"
	"github.com/goware/pgkit/dbtype"
	"github.com/jackc/pgx/v4"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	DB *pgkit.DB
)

func init() {
	mrand.Seed(time.Now().UnixNano())

	var err error
	DB, err = pgkit.Connect("pgkit_test", pgkit.Config{
		Database:        "pgkit_test",
		Host:            "localhost",
		Username:        "postgres",
		Password:        "postgres",
		ConnMaxLifetime: "1h",
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

func TestSugarInsertAndSelectRecordsReturningID(t *testing.T) {
	truncateTable(t, "accounts")

	// Insert
	rec := &Account{
		Name:     "joe",
		Disabled: true,
	}

	err := DB.Query.QueryRow(context.Background(), DB.SQL.InsertRecord(rec).Suffix(`RETURNING "id"`)).Scan(&rec.ID)
	assert.NoError(t, err)
	assert.True(t, rec.ID > 0)

	// Select one -- into object
	account := &Account{}
	err = DB.Query.GetOne(context.Background(), DB.SQL.Select("*").From("accounts"), account)
	assert.NoError(t, err)
	assert.Equal(t, "joe", account.Name)
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
	sort.Sort(sort.StringSlice(cols))
	assert.Equal(t, []string{"alias", "author", "content"}, cols)

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

	// {
	// 	stat := &Stat{Key: "count", Num: dbtype.NewBigInt(2)}

	// 	// Insert
	// 	q1 := DB.SQL.InsertRecord(stat, "stats")
	// 	_, err := DB.Query.Exec(context.Background(), q1)
	// 	assert.NoError(t, err)

	// 	// Select
	// 	var sout Stat
	// 	q2 := DB.SQL.Select("*").From("stats").Where(sq.Eq{"key": "count"})
	// 	err = DB.Query.GetOne(context.Background(), q2, &sout)
	// 	assert.NoError(t, err)
	// 	assert.Equal(t, "count", sout.Key)
	// 	assert.True(t, sout.Num.Int64() == 2)
	// 	assert.Nil(t, sout.Rating)
	// }

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

		pretty.Println(sout.Rating)

		assert.Nil(t, sout.Rating)
	}

	// last, with opt rating
	{
		v := dbtype.NewBigInt(5)
		stat := &Stat{
			Key:    "count3",
			Num:    dbtype.NewBigIntFromString("44", 0),
			Rating: v,
		}

		// Insert
		q1 := DB.SQL.InsertRecord(stat, "stats")
		_, err := DB.Query.Exec(context.Background(), q1)
		assert.NoError(t, err)

		// Select
		var sout Stat
		q2 := DB.SQL.Select("*").From("stats").Where(sq.Eq{"key": "count3"})
		err = DB.Query.GetOne(context.Background(), q2, &sout)
		assert.NoError(t, err)
		assert.Equal(t, "count3", sout.Key)
		assert.True(t, sout.Num.String() == "44")
		assert.True(t, sout.Rating.String() == "5")
	}

	// bigint ending 0 test
	{
		stat := &Stat{Key: "count3", Num: dbtype.NewBigInt(21000)}

		// Insert
		q1 := DB.SQL.InsertRecord(stat, "stats")
		_, err := DB.Query.Exec(context.Background(), q1)
		assert.NoError(t, err)

		// Select
		var sout Stat
		q2 := DB.SQL.Select("*").From("stats").Where(sq.Eq{"key": "count3"})
		err = DB.Query.GetOne(context.Background(), q2, &sout)
		assert.NoError(t, err)
		assert.Equal(t, "count3", sout.Key)
		assert.True(t, sout.Num.Int64() == 21000)
	}
}

func TestSugarInsertAndSelectMultipleRecords(t *testing.T) {
	truncateTable(t, "accounts")

	names := []string{"mary", "gary", "larry"}

	records := []*Account{}
	for _, n := range names {
		records = append(records, &Account{Name: n})
	}

	// Insert
	q1 := DB.SQL.InsertRecords(records) //, "accounts")
	_, err := DB.Query.Exec(context.Background(), q1)
	assert.NoError(t, err)

	// Select all
	var accounts []*Account
	q2 := DB.SQL.Select("*").From("accounts").OrderBy("name")
	err = DB.Query.GetAll(context.Background(), q2, &accounts)
	assert.NoError(t, err)
	assert.Len(t, accounts, 3)
	assert.Equal(t, "gary", accounts[0].Name)
	assert.Equal(t, "larry", accounts[1].Name)
	assert.Equal(t, "mary", accounts[2].Name)
}

func TestSugarUpdateRecord(t *testing.T) {
	truncateTable(t, "accounts")

	// Insert
	account := &Account{Name: "julia"}
	_, err := DB.Query.Exec(context.Background(), DB.SQL.InsertRecord(account))
	assert.NoError(t, err)

	// Query
	accountResp := &Account{}
	err = DB.Query.GetOne(context.Background(), DB.SQL.Select("*").From("accounts"), accountResp)
	assert.NoError(t, err)
	assert.Equal(t, "julia", accountResp.Name)
	assert.True(t, accountResp.ID != 0)

	// Update
	accountResp.Name = "JUL14"
	_, err = DB.Query.Exec(context.Background(), DB.SQL.UpdateRecord(accountResp, sq.Eq{"id": accountResp.ID})) //, "accounts"))
	assert.NoError(t, err)

	// Query
	accountResp2 := &Account{}
	err = DB.Query.GetOne(context.Background(), DB.SQL.Select("*").From("accounts"), accountResp2)
	assert.NoError(t, err)
	assert.Equal(t, "JUL14", accountResp.Name)
	assert.True(t, accountResp2.ID != 0)
	assert.True(t, accountResp2.ID == accountResp.ID)
	assert.True(t, accountResp2.CreatedAt == accountResp.CreatedAt)
}

func TestSugarUpdateRecordColumns(t *testing.T) {
	truncateTable(t, "accounts")

	// Insert
	account := &Account{Name: "peter"}
	_, err := DB.Query.Exec(context.Background(), DB.SQL.InsertRecord(account))
	assert.NoError(t, err)

	// Query
	accountResp := &Account{}
	err = DB.Query.GetOne(context.Background(), DB.SQL.Select("*").From("accounts"), accountResp)
	assert.NoError(t, err)
	assert.Equal(t, "peter", accountResp.Name)
	assert.True(t, accountResp.ID != 0)
	assert.False(t, accountResp.Disabled)

	// Update
	accountResp.Name = "p3t3r"
	accountResp.Disabled = true
	_, err = DB.Query.Exec(context.Background(), DB.SQL.UpdateRecordColumns(accountResp, sq.Eq{"id": accountResp.ID}, []string{"disabled"})) //, "accounts"))
	assert.NoError(t, err)

	// Query
	accountResp2 := &Account{}
	err = DB.Query.GetOne(context.Background(), DB.SQL.Select("*").From("accounts"), accountResp2)
	assert.NoError(t, err)
	assert.Equal(t, "peter", accountResp2.Name) // should not have changed, expect as previous was recorded
	assert.True(t, accountResp2.Disabled)
	assert.True(t, accountResp2.ID != 0)
	assert.True(t, accountResp2.ID == accountResp.ID)
}

func TestTransactionBasics(t *testing.T) {
	truncateTable(t, "accounts")

	// Insert some rows + commit
	DB.Conn.BeginFunc(context.Background(), func(tx pgx.Tx) error {
		// Insert 1
		insertq, args, err := DB.SQL.Insert("accounts").Columns("name", "disabled").Values("peter", false).ToSql()
		require.NoError(t, err)

		_, err = tx.Exec(context.Background(), insertq, args...)
		require.NoError(t, err)

		// Insert 2
		insertq2, args2, err := DB.SQL.Insert("accounts").Columns("name", "disabled").Values("mario", true).ToSql()
		require.NoError(t, err)

		_, err = tx.Exec(context.Background(), insertq2, args2...)
		require.NoError(t, err)

		return nil
	})

	// Assert above records have been made
	{
		var accounts []*Account
		q := DB.SQL.Select("*").From("accounts").OrderBy("name")
		err := DB.Query.GetAll(context.Background(), q, &accounts)
		require.NoError(t, err)
		assert.Len(t, accounts, 2)
		assert.True(t, accounts[0].Name == "mario")
		assert.True(t, accounts[1].Name == "peter")
	}

	// Insert some rows -- but rollback
	DB.Conn.BeginFunc(context.Background(), func(tx pgx.Tx) error {
		// Insert 1
		insertq, args, err := DB.SQL.Insert("accounts").Columns("name", "disabled").Values("zelda", false).ToSql()
		require.NoError(t, err)

		_, err = tx.Exec(context.Background(), insertq, args...)
		require.NoError(t, err)

		// Insert 2
		insertq2, args2, err := DB.SQL.Insert("accounts").Columns("name", "disabled").Values("princess", true).ToSql()
		require.NoError(t, err)

		_, err = tx.Exec(context.Background(), insertq2, args2...)
		require.NoError(t, err)

		return fmt.Errorf("something bad happend")
	})

	// Assert above records were rolled back
	{
		var accounts []*Account
		q := DB.SQL.Select("*").From("accounts").OrderBy("name")
		err := DB.Query.GetAll(context.Background(), q, &accounts)
		require.NoError(t, err)
		assert.Len(t, accounts, 2)
	}
}

func TestSugarTransaction(t *testing.T) {
	truncateTable(t, "accounts")

	DB.Conn.BeginFunc(context.Background(), func(tx pgx.Tx) error {
		rec1 := &Account{
			Name:     "peter",
			Disabled: false,
		}

		q1 := DB.SQL.InsertRecord(rec1)
		_, err := DB.TxQuery(tx).Exec(context.Background(), q1)
		require.NoError(t, err)

		rec2 := &Account{
			Name:     "mario",
			Disabled: true,
		}

		q2 := DB.SQL.InsertRecord(rec2)
		_, err = DB.TxQuery(tx).Exec(context.Background(), q2)
		require.NoError(t, err)

		return nil
	})

	// Assert above records have been made
	{
		var accounts []*Account
		q := DB.SQL.Select("*").From("accounts").OrderBy("name")
		err := DB.Query.GetAll(context.Background(), q, &accounts)
		require.NoError(t, err)
		assert.Len(t, accounts, 2)
		assert.True(t, accounts[0].Name == "mario")
		assert.True(t, accounts[1].Name == "peter")
	}
}

// TODO: batch support .. with test
