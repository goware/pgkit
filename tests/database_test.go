package pgkit_test

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/goware/pgkit/v2"
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
		Accounts: &accountsTable{Table: pgkit.NewTable[Account, *Account](db, "accounts")},
		Articles: &articlesTable{Table: pgkit.NewTable[Article, *Article](db, "articles")},
		Reviews:  &reviewsTable{Table: pgkit.NewTable[Review, *Review](db, "reviews")},
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

func (db *Database) Close() {
	db.DB.Conn.Close()
}
