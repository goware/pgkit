pgkit
=====

[![Go Reference](https://pkg.go.dev/badge/github.com/goware/pgkit/v2.svg)](https://pkg.go.dev/github.com/goware/pgkit/v2)
[![Go Report Card](https://goreportcard.com/badge/github.com/goware/pgkit/v2)](https://goreportcard.com/report/github.com/goware/pgkit/v2)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

lightweight PostgreSQL sugar combining the use of..

* github.com/jackc/pgx
* github.com/Masterminds/squirrel
* github.com/georgysavva/scany/v2
* plus, code & ideas from github.com/upper/db

In other words: just enough sugar to make everyday database work quick and
pleasant — without hiding SQL from you or turning into a heavy ORM.


## What problem does it solve?

Writing database code in Go usually means a lot of repetitive boilerplate:

- opening and configuring a connection pool
- building SQL strings by hand and counting `$1, $2, $3...` placeholders
- looping over rows and copying each column into a struct field
- writing the same insert / update / delete for every table
- getting pagination right

pgkit takes care of all of that, so you can write less code and make fewer mistakes.
You still get real SQL and full access to pgx when you need it.


## What pgkit adds

On top of those libraries, pgkit gives you:

- **one-line connect** with sensible pool defaults
- **struct mapping** — turn a Go struct into columns and values using `db:"..."` tags
- **`RawQuery`** — write plain SQL with simple `?` placeholders
- **`Table[T]`** — ready-made Create / Read / Update / Delete for any table
- **pagination** — both page-number (offset) and cursor (keyset) styles
- **transactions**, **batch inserts**, **soft deletes**, and **query tracing/logging**


## Install

```sh
go get github.com/goware/pgkit/v2
```

Requires Go 1.25+ and PostgreSQL.


## Connect

```go
db, err := pgkit.Connect("my-app", pgkit.Config{
    Database: "mydb",
    Host:     "localhost",
    Username: "postgres",
    Password: "postgres",
})
if err != nil {
    log.Fatal(err)
}
defer db.Conn.Close()
```

`db` gives you three things:

- `db.Conn`  — the underlying pgx connection pool
- `db.SQL`   — the query builder
- `db.Query` — runs queries and scans results


## Two ways to use it

You can mix and match these freely. Use the low-level builder for one-off queries,
and `Table[T]` for the usual CRUD on your main tables.

### 1. Low-level: build a query, run it, scan the result

First, describe a row as a struct. The `db:"..."` tags say which column each field maps to:

```go
type Account struct {
    ID       int64  `db:"id,omitempty"`
    Name     string `db:"name"`
    Disabled bool   `db:"disabled"`
}
```

Insert a row:

```go
q := db.SQL.Insert("accounts").Columns("name", "disabled").Values("peter", false)
_, err := db.Query.Exec(ctx, q)
```

Read many rows into a slice:

```go
import sq "github.com/Masterminds/squirrel"

var accounts []*Account
q := db.SQL.Select("*").From("accounts").Where(sq.Eq{"disabled": false})
err := db.Query.GetAll(ctx, q, &accounts)
```

Read a single row:

```go
var acc Account
q := db.SQL.Select("*").From("accounts").Where(sq.Eq{"id": 1})
err := db.Query.GetOne(ctx, q, &acc) // returns pgkit.ErrNoRows if nothing matches
```

Prefer plain SQL? Use `RawQuery`. It turns `?` into `$1, $2, ...` for you:

```go
stmt := pgkit.RawQuery("SELECT * FROM accounts WHERE name IN (?, ?)")

var accounts []*Account
err := db.Query.GetAll(ctx, stmt.Build("peter", "mary"), &accounts)
```

### 2. High-level: `Table[T]` for easy CRUD

For a table you touch a lot, wrap it in a `Table` and get common operations for free.

Your record needs a couple of methods — `GetID()` and `Validate()`:

```go
type Account struct {
    ID        int64     `db:"id,omitempty"`
    Name      string    `db:"name"`
    CreatedAt time.Time `db:"created_at,omitempty"`
    UpdatedAt time.Time `db:"updated_at,omitempty"`
}

func (a *Account) GetID() int64             { return a.ID }
func (a *Account) Validate() error          { /* return an error to block bad data */ return nil }
func (a *Account) SetUpdatedAt(t time.Time) { a.UpdatedAt = t } // optional, filled automatically
```

Now create the table helper once:

```go
accounts := &pgkit.Table[Account, *Account, int64]{
    DB:       db,
    Name:     "accounts",
    IDColumn: "id",
}
```

And use it:

```go
acc := &Account{Name: "Peter"}

err := accounts.Insert(ctx, acc)          // acc.ID is filled in for you
got, err := accounts.GetByID(ctx, acc.ID) // fetch by id

acc.Name = "Peter Pan"
_, err = accounts.Update(ctx, acc)        // update by id

err = accounts.Save(ctx, acc)             // insert if new, update if it has an id

list, err := accounts.List(ctx, sq.Eq{"disabled": false}, nil) // fetch many

_, err = accounts.DeleteByID(ctx, acc.ID) // delete
```

`Insert`, `Update`, and `Save` all accept many records at once, for example
`accounts.Insert(ctx, a, b, c)`.


## Struct tags: `omitempty` and `omitzero`

The `db:"..."` tag controls how a field becomes a column:

- `db:"name"` — always included.
- `db:"name,omitempty"` — skipped when the value is empty/zero. PostgreSQL then uses
  the column's `DEFAULT` (great for `id`, `created_at`, and similar).
- `db:"name,omitzero"` — like `omitempty`, but keeps a non-nil empty slice or map, so you
  can deliberately clear a column to an empty value.

This means a batch insert can mix records that set a field and records that leave it out;
pgkit lines the columns up and fills the gaps with `DEFAULT` automatically.


## Pagination

Page-number style (offset):

```go
page := pgkit.NewPage(20, 1) // 20 per page, page 1
rows, page, err := accounts.ListPaged(ctx, nil, page)
if page.More {
    // there is at least one more page
}
```

Cursor style (keyset) — steadier when data is changing under you:

```go
accounts := accounts.WithMode(pgkit.CursorBased)

rows, page, err := accounts.ListPaged(ctx, nil, &pgkit.Page{})
// fetch the next page using the cursor from the last result:
rows, page, err = accounts.ListPaged(ctx, nil, &pgkit.Page{Cursor: page.NextCursor})
```


## Transactions

Run several statements together, and roll everything back if any step fails:

```go
err := pgx.BeginFunc(ctx, db.Conn, func(tx pgx.Tx) error {
    txAccounts := accounts.WithTx(tx)
    if err := txAccounts.Insert(ctx, &Account{Name: "a"}); err != nil {
        return err // anything non-nil rolls the whole thing back
    }
    return txAccounts.Insert(ctx, &Account{Name: "b"})
})
```


## A few more niceties

- **Soft delete** — add a `SetDeletedAt(time.Time)` method to your record and `DeleteByID`
  marks the row deleted instead of removing it. `RestoreByID` brings it back.
- **Timestamps** — add `SetCreatedAt` / `SetUpdatedAt` methods and pgkit fills them on
  insert/update.
- **Streaming** — `Table.Iter` returns rows one at a time, so you can process huge result
  sets without loading them all into memory.
- **Job queues** — `LockForUpdate` uses `FOR UPDATE SKIP LOCKED` so multiple workers can
  each grab a different row safely.
- **Tracing** — pass a tracer in `Config` (see [`./tracer`](./tracer)) to log queries,
  values, and failures via `log/slog`.


## Full examples

The test suite is the most complete, up-to-date reference:

- [`./tests/pgkit_test.go`](./tests/pgkit_test.go) — low-level queries
- [`./tests/table_test.go`](./tests/table_test.go) — `Table[T]` CRUD, pagination, transactions
- [`./examples/tracing`](./examples/tracing) — query logging


## License

[MIT](./LICENSE)
