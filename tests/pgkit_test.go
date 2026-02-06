package pgkit_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"sort"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/goware/pgkit/v2"
	"github.com/goware/pgkit/v2/db"
	"github.com/goware/pgkit/v2/dbtype"
	"github.com/goware/pgkit/v2/tracer"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	DB *pgkit.DB
)

func init() {
	var err error
	DB, err = connectToDb(pgkit.Config{
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
	require.NoError(t, err)

	// Select
	var accounts []*Account
	q2 := DB.SQL.Select("*").From("accounts")
	err = DB.Query.GetAll(context.Background(), q2, &accounts)

	require.NoError(t, err)
	require.Len(t, accounts, 1)
	assert.NotZero(t, accounts[0].ID)
	assert.Equal(t, "peter", accounts[0].Name)
}

// TestInsertAndSelectRows is a more verbose version of TestSugarBasicInsertAndSelectRows
func TestInsertAndSelectRows(t *testing.T) {
	truncateTable(t, "accounts")

	// Insert
	insertq, args, err := DB.SQL.Insert("accounts").Columns("name", "disabled").Values("peter", false).ToSql()
	require.NoError(t, err)

	_, err = DB.Conn.Exec(context.Background(), insertq, args...)
	require.NoError(t, err)

	// Select all
	selectq, args := DB.SQL.Select("*").From("accounts").MustSql()

	var accounts []*Account
	err = DB.Query.Scan.Select(context.Background(), DB.Conn, &accounts, selectq, args...)

	require.NoError(t, err)
	require.Len(t, accounts, 1)
	assert.NotZero(t, accounts[0].ID)
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
	assert.NotZero(t, accounts[0].ID)
	assert.Equal(t, "joe", accounts[0].Name)

	// Select one -- into object
	account := &Account{}
	err = DB.Query.GetOne(context.Background(), q2, account)
	assert.NoError(t, err)
	assert.Len(t, accounts, 1)
	assert.NotZero(t, accounts[0].ID)
	assert.Equal(t, "joe", accounts[0].Name)

	// Select one -- into struct value
	var accountv Account
	err = DB.Query.GetOne(context.Background(), q2, &accountv)
	assert.NoError(t, err)
	assert.Len(t, accounts, 1)
	assert.NotZero(t, accounts[0].ID)
	assert.Equal(t, "joe", accounts[0].Name)
}

func TestGetOneInTransaction(t *testing.T) {
	ctx := context.Background()
	q2 := DB.SQL.Select("*").From("accounts")

	var account Account
	err := pgx.BeginFunc(ctx, DB.Conn, func(tx pgx.Tx) error {
		if err := DB.TxQuery(tx).GetOne(ctx, q2, &account); err != nil {
			return fmt.Errorf("get one: %w", err)
		}

		return nil
	})

	assert.NoError(t, err)
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
	assert.NotZero(t, rec.ID)

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
	assert.NoError(t, err)
	defer rows.Close()

	// Scan result into *Account object
	a := &Account{}
	err = DB.Query.Scan.ScanOne(a, rows)
	assert.NoError(t, err)

	assert.NotZero(t, a.ID)
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
	assert.ErrorIs(t, err, pgkit.ErrNoRows)
}

func TestQueryWithNoResults(t *testing.T) {
	selectq, args, err := DB.SQL.Select("*").From("accounts").Where(sq.Eq{"name": "no-match"}).ToSql()
	assert.NoError(t, err)

	var accounts []*Account

	// shorthand
	{
		err = DB.Query.Scan.Select(context.Background(), DB.Conn, &accounts, selectq, args...)
		assert.NoError(t, err)
		assert.Len(t, accounts, 0)
	}

	// or, with more verbose method:
	{
		rows, err := DB.Conn.Query(context.Background(), selectq, args...)
		assert.NoError(t, err)
		defer rows.Close()

		err = DB.Query.Scan.ScanAll(&accounts, rows)

		assert.NoError(t, err)
		assert.Len(t, accounts, 0)
	}

	// scan one -- returning 'no rows' error
	{
		var a *Account
		err = DB.Query.Scan.Get(context.Background(), DB.Conn, a, selectq, args...)
		assert.ErrorIs(t, err, pgkit.ErrNoRows)
	}
}

func TestRowsWithJSONB(t *testing.T) {
	truncateTable(t, "logs")

	etc := map[string]interface{}{"a": 1}

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
		// RawData: []byte{5, 6, 7, 8},
		RawData: dbtype.HexBytes{5, 6, 7, 8},
		Etc:     map[string]interface{}{"place": "Toronto"},
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
	// assert.Equal(t, []byte{5, 6, 7, 8}, lout.RawData)
	assert.Equal(t, dbtype.HexBytes{5, 6, 7, 8}, lout.RawData)
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
	sort.Strings(cols)
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
		assert.Equal(t, int64(2), sout.Num.Int64())
		assert.True(t, sout.Num.IsValid)

		assert.False(t, sout.Rating.IsValid)
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
		assert.Equal(t, "12323942398472837489234", sout.Num.String())
		assert.True(t, sout.Num.IsValid)

		assert.False(t, sout.Rating.IsValid)
	}

	{
		stat := &Stat{
			Key:    "count3",
			Num:    dbtype.NewBigIntFromString("44", 0),
			Rating: dbtype.NewBigInt(5),
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
		assert.Equal(t, "44", sout.Num.String())
		assert.True(t, sout.Num.IsValid)
		assert.Equal(t, "5", sout.Rating.String())
		assert.True(t, sout.Rating.IsValid)
	}

	// bigint ending 0 test
	{
		stat := &Stat{Key: "count4", Num: dbtype.NewBigInt(21000)}

		// Insert
		q1 := DB.SQL.InsertRecord(stat, "stats")
		_, err := DB.Query.Exec(context.Background(), q1)
		assert.NoError(t, err)

		// Select
		var sout Stat
		q2 := DB.SQL.Select("*").From("stats").Where(sq.Eq{"key": "count4"})
		err = DB.Query.GetOne(context.Background(), q2, &sout)
		assert.NoError(t, err)
		assert.Equal(t, "count4", sout.Key)
		assert.Equal(t, int64(21000), sout.Num.Int64())
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
	assert.NotZero(t, accountResp.ID)

	// Update
	accountResp.Name = "JUL14"
	_, err = DB.Query.Exec(context.Background(), DB.SQL.UpdateRecord(accountResp, sq.Eq{"id": accountResp.ID})) //, "accounts"))
	assert.NoError(t, err)

	// Query
	accountResp2 := &Account{}
	err = DB.Query.GetOne(context.Background(), DB.SQL.Select("*").From("accounts"), accountResp2)
	assert.NoError(t, err)
	assert.Equal(t, "JUL14", accountResp.Name)
	assert.NotZero(t, accountResp2.ID)
	assert.Equal(t, accountResp2.ID, accountResp.ID)
	assert.Equal(t, accountResp2.CreatedAt, accountResp.CreatedAt)
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
	assert.NotZero(t, accountResp.ID)
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
	assert.NotZero(t, accountResp2.ID)
	assert.Equal(t, accountResp2.ID, accountResp.ID)
}

func TestTransactionBasics(t *testing.T) {
	truncateTable(t, "accounts")

	// Insert some rows + commit
	err := pgx.BeginFunc(context.Background(), DB.Conn, func(tx pgx.Tx) error {
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
	require.NoError(t, err)

	// Assert above records have been made
	{
		var accounts []*Account
		q := DB.SQL.Select("*").From("accounts").OrderBy("name")
		err := DB.Query.GetAll(context.Background(), q, &accounts)
		require.NoError(t, err)
		assert.Len(t, accounts, 2)
		assert.Equal(t, "mario", accounts[0].Name)
		assert.Equal(t, "peter", accounts[1].Name)
	}

	// Insert some rows -- but rollback
	err = pgx.BeginFunc(context.Background(), DB.Conn, func(tx pgx.Tx) error {
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
	require.Error(t, err)

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

	err := pgx.BeginFunc(context.Background(), DB.Conn, func(tx pgx.Tx) error {
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
	require.NoError(t, err)

	// Assert above records have been made
	{
		var accounts []*Account
		q := DB.SQL.Select("*").From("accounts").OrderBy("name")
		err := DB.Query.GetAll(context.Background(), q, &accounts)
		require.NoError(t, err)
		assert.Len(t, accounts, 2)
		assert.Equal(t, "mario", accounts[0].Name)
		assert.Equal(t, "peter", accounts[1].Name)
	}
}

func TestBatchTransaction(t *testing.T) {
	ctx := context.Background()

	truncateTable(t, "accounts")

	err := pgx.BeginFunc(context.Background(), DB.Conn, func(tx pgx.Tx) error {
		batch := &pgx.Batch{}

		rec := &Account{}
		for i := 0; i < 10; i++ {
			rec.Name = fmt.Sprintf("user-%d", i)
			sql, args, err := DB.SQL.InsertRecord(rec).ToSql()
			assert.NoError(t, err)
			batch.Queue(sql, args...)
		}
		if batch.Len() == 0 {
			return nil
		}

		br := tx.SendBatch(ctx, batch)
		_, err := br.Exec()
		if err != nil {
			return err
		}
		err = br.Close()
		if err != nil {
			return err
		}

		return nil
	})
	assert.NoError(t, err)

	var accounts []*Account
	q := DB.SQL.Select("*").From("accounts").OrderBy("name")
	err = DB.Query.GetAll(context.Background(), q, &accounts)
	assert.NoError(t, err)
	assert.Len(t, accounts, 10)
}

func TestSugarBatchTransaction(t *testing.T) {
	ctx := context.Background()

	truncateTable(t, "accounts")

	err := pgx.BeginFunc(context.Background(), DB.Conn, func(tx pgx.Tx) error {
		queries := pgkit.Queries{}

		for i := 0; i < 10; i++ {
			rec := &Account{Name: fmt.Sprintf("user-%d", i)}
			queries.Add(DB.SQL.InsertRecord(rec))
		}

		_, err := DB.TxQuery(tx).BatchExec(ctx, queries)
		return err
	})
	assert.NoError(t, err)

	var accounts []*Account
	q := DB.SQL.Select("*").From("accounts").OrderBy("name")
	err = DB.Query.GetAll(context.Background(), q, &accounts)
	assert.NoError(t, err)
	assert.Len(t, accounts, 10)
}

func TestBatchQuery(t *testing.T) {
	ctx := context.Background()

	batch := &pgx.Batch{}

	names := []string{}
	for i := 0; i < 10; i++ {
		names = append(names, fmt.Sprintf("user-%d", i))
	}
	for _, name := range names {
		batch.Queue(fmt.Sprintf("select * from accounts where name='%s'", name))
	}

	br := DB.Conn.SendBatch(ctx, batch)

	var accounts []*Account

	for i := 0; i < batch.Len(); i++ {
		rows, err := br.Query()
		require.NoError(t, err)

		var account Account
		err = DB.Query.Scan.ScanOne(&account, rows)
		require.NoError(t, err)
		accounts = append(accounts, &account)
	}
	err := br.Close()
	require.NoError(t, err)

	require.Len(t, accounts, len(names))
	for i := 0; i < len(names); i++ {
		assert.Equal(t, names[i], accounts[i].Name)
	}
}

func TestSugarBatchQuery(t *testing.T) {
	ctx := context.Background()

	names := []string{}
	for i := 0; i < 10; i++ {
		names = append(names, fmt.Sprintf("user-%d", i))
	}

	queries := pgkit.Queries{}
	for _, name := range names {
		queries.Add(DB.SQL.Select("*").From("accounts").Where(db.Cond{"name": name}))
	}

	batchResults, batchLen, err := DB.Query.BatchQuery(ctx, queries)
	require.NoError(t, err)

	defer func() {
		err := batchResults.Close()
		require.NoError(t, err)
	}()

	var accounts []*Account

	for i := 0; i < batchLen; i++ {
		rows, err := batchResults.Query()
		require.NoError(t, err)

		var account Account
		err = DB.Query.Scan.ScanOne(&account, rows)
		require.NoError(t, err)
		accounts = append(accounts, &account)
	}

	require.Len(t, accounts, len(names))
	for i := 0; i < len(names); i++ {
		assert.Equal(t, names[i], accounts[i].Name)
	}

	/*
		err := DB.Query.BatchGetAll(ctx, queries, &accounts)
		require.NoError(t, err)

		// batchRows, err := DB.Query.BatchQuery(ctx, queries)
		// require.NoError(t, err)

		// for _, rows := range batchRows {
		// 	var account Account
		// 	err := DB.Query.Scan.ScanOne(&account, rows)
		// 	require.NoError(t, err)
		// 	accounts = append(accounts, &account)
		// }

		require.Len(t, accounts, len(names))
		for i := 0; i < len(names); i++ {
			assert.Equal(t, names[i], accounts[i].Name)
		}*/
}

func TestRawSQLQuery(t *testing.T) {
	// q := pgkit.RawSQL{Query: "SELECT * FROM accounts WHERE name=? OR name=?", Args: []interface{}{"user-1", "user-2"}}
	q := pgkit.RawSQL{Query: "SELECT * FROM accounts WHERE name IN (?,?) OR name=?", Args: []interface{}{"user-1", "user-2", "user-3"}}

	// q := DB.SQL.Select("*").From("accounts").Where(sq.Eq{"name": []string{"user-1", "user-2"}})

	sql, args, err := q.ToSql()
	// fmt.Println("sql", sql)
	// fmt.Println("args", args)
	require.NoError(t, err)
	require.NotEmpty(t, sql)
	require.NotEmpty(t, args)

	accounts := []*Account{}
	err = DB.Query.GetAll(context.Background(), q, &accounts)
	require.NoError(t, err)
	require.Len(t, accounts, 3)
}

func TestRawStatementQuery(t *testing.T) {
	stmt := pgkit.RawQuery("SELECT * FROM accounts WHERE name IN (?,?)")

	require.NoError(t, stmt.Err())
	require.NotEmpty(t, stmt.GetQuery())
	require.Equal(t, 2, stmt.NumArgs())

	q := stmt.Build("user-1", "user-2")

	sql, args, err := q.ToSql()
	// fmt.Println("sql", sql)
	// fmt.Println("args", args)
	require.NoError(t, err)
	require.NotEmpty(t, sql)
	require.NotEmpty(t, args)

	accounts := []*Account{}
	err = DB.Query.GetAll(context.Background(), q, &accounts)
	require.NoError(t, err)
	require.Len(t, accounts, 2)
}

type LogRecord struct {
	Msg      string        `json:"msg,omitempty"`
	Query    string        `json:"query,omitempty"`
	Args     []interface{} `json:"args,omitempty"`
	Err      string        `json:"err"`
	Duration time.Duration `json:"duration,omitempty"`
}

func TestSlogQueryTracerWithValuesReplaced(t *testing.T) {
	buf, slogTracer := getTracer([]tracer.Option{tracer.WithLogAllQueries(), tracer.WithLogValues(), tracer.WithLogFailedQueries()})

	dbClient, err := connectToDb(pgkit.Config{
		Database:        "pgkit_test",
		Host:            "localhost",
		Username:        "postgres",
		Password:        "postgres",
		ConnMaxLifetime: "1h",
		Tracer:          tracer.NewSQLTracer(slogTracer),
	})
	require.NoError(t, err)

	defer dbClient.Conn.Close()

	stmt := pgkit.RawQuery("SELECT * FROM accounts WHERE name IN (?,?)")
	q := stmt.Build("user-1", "user-2")

	accounts := []*Account{}
	// ignore err to find out if we logged the sql error using slogTracer
	_ = dbClient.Query.GetAll(context.Background(), q, &accounts)

	r := bufio.NewReader(buf)
	line, _, err := r.ReadLine()
	if err != nil {
		log.Fatal(fmt.Errorf("failed to read line: %w", err))
	}
	var record LogRecord
	err = json.Unmarshal(line, &record)
	if err != nil {
		log.Fatal(err)
	}

	assert.Equal(t, "SELECT * FROM accounts WHERE name IN ('user-1','user-2')", record.Query)
}

func TestSlogQueryTracerWithCustomLoggingFunctions(t *testing.T) {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, nil)
	logger := slog.New(handler)
	slogTracer := tracer.NewLogTracer(nil,
		tracer.WithLogValues(),
		tracer.WithLogAllQueries(),
		tracer.WithLogFailedQueries(),
		tracer.WithLogStartHook(func(ctx context.Context, query string, args []any) {
			logger.Info(query, args...)
		}),
		tracer.WithLogEndHook(func(ctx context.Context, query string, duration time.Duration) {
			logger.Info(query, slog.Duration("duration", duration))
		}),
	)

	dbClient, err := connectToDb(pgkit.Config{
		Database:        "pgkit_test",
		Host:            "localhost",
		Username:        "postgres",
		Password:        "postgres",
		ConnMaxLifetime: "1h",
		Tracer:          tracer.NewSQLTracer(slogTracer),
	})
	if err != nil {
		log.Fatal(err)
	}

	defer dbClient.Conn.Close()

	stmt := pgkit.RawQuery("SELECT * FROM accounts WHERE name IN (?,?)")
	q := stmt.Build("user-1", "user-2")

	accounts := []*Account{}
	// ignore err to find out if we logged the sql error using slogTracer
	_ = dbClient.Query.GetAll(context.Background(), q, &accounts)

	records := []*LogRecord{
		{
			Msg: "SELECT * FROM accounts WHERE name IN (user-1,user-2)",
		},
		{
			Msg:      "SELECT * FROM accounts WHERE name IN (user-1,user-2)",
			Duration: 0,
		},
	}

	var record LogRecord
	reader := bufio.NewReader(buf)
	for _, r := range records {
		line, _, err := reader.ReadLine()
		if err != nil {
			log.Fatal(fmt.Errorf("read line: %w", err))
		}
		err = json.Unmarshal(line, &record)
		if err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, "SELECT * FROM accounts WHERE name IN (user-1,user-2)", r.Msg)
	}
}

func TestSlogQueryTracerUsingContextToInit(t *testing.T) {
	buf, slogTracer := getTracer([]tracer.Option{})

	dbClient, err := connectToDb(pgkit.Config{
		Database:        "pgkit_test",
		Host:            "localhost",
		Username:        "postgres",
		Password:        "postgres",
		ConnMaxLifetime: "1h",
		Tracer:          tracer.NewSQLTracer(slogTracer),
	})
	require.NoError(t, err)

	defer dbClient.Conn.Close()

	stmt := pgkit.RawQuery("SELECT * FROM accounts WHERE name IN (?,?)")
	q := stmt.Build("user-1", "user-2")

	accounts := []*Account{}
	// ignore err to find out if we logged the sql error using slogTracer
	ctx := context.Background()
	ctx = tracer.WithTracingEnabled(ctx)

	_ = dbClient.Query.GetAll(ctx, q, &accounts)
	_ = dbClient.Query.GetAll(context.Background(), q, &accounts)

	r := bufio.NewReader(buf)
	line, _, err := r.ReadLine()
	if err != nil {
		log.Fatal(fmt.Errorf("read line: %w", err))
	}
	var record LogRecord
	err = json.Unmarshal(line, &record)
	if err != nil {
		log.Fatal(err)
	}
	assert.Equal(t, "SELECT * FROM accounts WHERE name IN ($1,$2)", record.Query)

	line, _, err = r.ReadLine()
	if err != nil {
		log.Fatal(fmt.Errorf("read line: %w", err))
	}
	err = json.Unmarshal(line, &record)
	if err != nil {
		log.Fatal(err)
	}

	assert.Equal(t, "SELECT * FROM accounts WHERE name IN ($1,$2)", record.Query)
	assert.Equal(t, "query end", record.Msg)

	// second line does not exist
	// because we passed context without logging enabled
	line, _, err = r.ReadLine()
	if err != io.EOF {
		log.Fatal(fmt.Errorf("expected EOF, got: %s: %w", string(line), err))
	}
}

func TestSlogQueryTracerWithErr(t *testing.T) {
	buf, slogTracer := getTracer([]tracer.Option{tracer.WithLogAllQueries(), tracer.WithLogFailedQueries()})

	dbClient, err := connectToDb(pgkit.Config{
		Database:        "pgkit_test",
		Host:            "localhost",
		Username:        "postgres",
		Password:        "postgres",
		ConnMaxLifetime: "1h",
		Tracer:          tracer.NewSQLTracer(slogTracer),
	})
	require.NoError(t, err)

	defer dbClient.Conn.Close()

	stmt := pgkit.RawQuery("SELECT random_columns FROM accounts WHERE name IN (?,?)")
	q := stmt.Build("user-1", "user-2")

	accounts := []*Account{}
	// ignore err to find out if we logged the sql error using slogTracer
	_ = dbClient.Query.GetAll(context.Background(), q, &accounts)

	r := bufio.NewReader(buf)
	sqlLine, _, err := r.ReadLine()
	if err != nil {
		log.Fatal(fmt.Errorf("failed to read line: %w", err))
	}
	var sqlRecord LogRecord
	err = json.Unmarshal(sqlLine, &sqlRecord)
	if err != nil {
		log.Fatal(err)
	}

	errLine, _, err := r.ReadLine()
	if err != nil {
		log.Fatal(fmt.Errorf("failed to read line: %w", err))
	}

	var errRecord LogRecord
	err = json.Unmarshal(errLine, &errRecord)
	if err != nil {
		log.Fatal(err)
	}

	assert.Equal(t, "SELECT random_columns FROM accounts WHERE name IN ($1,$2)", sqlRecord.Query)
	assert.Equal(t, "SELECT random_columns FROM accounts WHERE name IN ($1,$2)", errRecord.Query)
	assert.Equal(t, "ERROR: column \"random_columns\" does not exist (SQLSTATE 42703)", errRecord.Err)
}

func TestSlogSlowQuery(t *testing.T) {
	buf, slogTracer := getTracer([]tracer.Option{tracer.WithLogSlowQueriesThreshold(50 * time.Millisecond)})

	dbClient, err := connectToDb(pgkit.Config{
		Database:        "pgkit_test",
		Host:            "localhost",
		Username:        "postgres",
		Password:        "postgres",
		ConnMaxLifetime: "1h",
		Tracer:          tracer.NewSQLTracer(slogTracer),
	})
	require.NoError(t, err)

	defer dbClient.Conn.Close()

	stmt := pgkit.RawQuery("SELECT pg_sleep(0.1)")

	_, err = dbClient.Query.Exec(context.Background(), stmt.Build())
	if err != nil {
		log.Fatal(fmt.Errorf("failed to exec sql: %w", err))
	}
	r := bufio.NewReader(buf)
	sqlLine, _, err := r.ReadLine()
	if err != nil {
		log.Fatal(fmt.Errorf("failed to read line: %w", err))
	}

	var sqlRecord LogRecord
	err = json.Unmarshal(sqlLine, &sqlRecord)
	if err != nil {
		log.Fatal(err)
	}

	assert.Equal(t, "SELECT pg_sleep(0.1)", sqlRecord.Query)
	assert.Regexp(t, `^\d+\.\d+ms$`, sqlRecord.Duration)
}

func TestSlogTracerBatchQuery(t *testing.T) {
	buf, slogTracer := getTracer([]tracer.Option{tracer.WithLogAllQueries(), tracer.WithLogValues()})

	dbClient, err := connectToDb(pgkit.Config{
		Database:        "pgkit_test",
		Host:            "localhost",
		Username:        "postgres",
		Password:        "postgres",
		ConnMaxLifetime: "1h",
		Tracer:          tracer.NewSQLTracer(slogTracer),
	})
	require.NoError(t, err)

	defer dbClient.Conn.Close()

	ctx := context.Background()
	err = pgx.BeginFunc(context.Background(), dbClient.Conn, func(tx pgx.Tx) error {
		queries := pgkit.Queries{}

		for i := 0; i < 10; i++ {
			rec := &Account{Name: fmt.Sprintf("user-%d", i)}
			queries.Add(DB.SQL.InsertRecord(rec))
		}

		_, err := dbClient.TxQuery(tx).BatchExec(ctx, queries)
		return err
	})
	assert.NoError(t, err)

	expectedLogs := []LogRecord{
		{
			Msg:   "query start",
			Query: "begin",
			Args:  nil,
		},
		{
			Msg:   "query end",
			Query: "begin",
		},
		{
			Msg:   "query start",
			Query: "INSERT INTO accounts (disabled,name) VALUES (false,'user-0')",
		},
		{
			Msg:   "query end",
			Query: "INSERT INTO accounts (disabled,name) VALUES (false,'user-0')",
		},
		{
			Msg:   "query start",
			Query: "commit",
		},
		{
			Msg:   "query end",
			Query: "commit",
		},
	}

	var sqlRecord LogRecord
	r := bufio.NewReader(buf)
	for _, expectedLog := range expectedLogs {
		sqlLine, _, err := r.ReadLine()
		if err != nil {
			log.Fatal(fmt.Errorf("read line: %w", err))
		}

		err = json.Unmarshal(sqlLine, &sqlRecord)
		if err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, expectedLog.Query, sqlRecord.Query)
		assert.Equal(t, expectedLog.Msg, sqlRecord.Msg)
	}
}

func getTracer(opts []tracer.Option) (*bytes.Buffer, *tracer.LogTracer) {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, nil)
	logger := slog.New(handler)
	slogTracer := tracer.NewLogTracer(logger, opts...)
	return buf, slogTracer
}

func connectToDb(conf pgkit.Config) (*pgkit.DB, error) {
	dbClient, err := pgkit.Connect("pgkit_test", conf)
	if err != nil {
		log.Fatal(fmt.Errorf("failed to connect dbClient: %w", err))
	}

	err = dbClient.Conn.Ping(context.Background())
	if err != nil {
		log.Fatal(fmt.Errorf("failed to ping dbClient: %w", err))
	}
	return dbClient, err
}
