package pgkit_test

import (
	"context"

	"github.com/goware/pgkit/v2"
	"github.com/jackc/pgx/v5"
)

type Database struct {
	*pgkit.DB

	Accounts *accountsTable
	Articles *articlesTable
	Reviews  *reviewsTable
}

func initDB(db *pgkit.DB) *Database {
	return &Database{
		DB:       db,
		Accounts: &accountsTable{Table: &pgkit.Table[Account, *Account, int64]{DB: db, Name: "accounts", IDColumn: "id"}},
		Articles: &articlesTable{Table: &pgkit.Table[Article, *Article, uint64]{DB: db, Name: "articles", IDColumn: "id"}},
		Reviews:  &reviewsTable{Table: &pgkit.Table[Review, *Review, uint64]{DB: db, Name: "reviews", IDColumn: "id"}},
	}
}

func (db *Database) BeginTx(ctx context.Context, fn func(tx *Database) error) error {
	return pgx.BeginFunc(ctx, db.Conn, func(pgTx pgx.Tx) error {
		tx := db.WithTx(pgTx)
		return fn(tx)
	})
}

func (db *Database) WithTx(tx pgx.Tx) *Database {
	pgkitDB := &pgkit.DB{
		Conn:  db.Conn,
		SQL:   db.SQL,
		Query: db.TxQuery(tx),
	}

	return initDB(pgkitDB)
}

func (db *Database) Close() { db.DB.Conn.Close() }

type accountsTable struct {
	*pgkit.Table[Account, *Account, int64]
}

type articlesTable struct {
	*pgkit.Table[Article, *Article, uint64]
}

type reviewsTable struct {
	*pgkit.Table[Review, *Review, uint64]
}
