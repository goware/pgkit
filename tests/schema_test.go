package pgkit_test

import (
	"time"

	"github.com/goware/pgkit/dbtype"
)

type Account struct {
	ID        int64     `db:"id,omitempty"`
	Name      string    `db:"name"`
	Disabled  bool      `db:"disabled"`
	CreatedAt time.Time `db:"created_at,omitempty"`
}

type Review struct {
	ID        int64     `db:"id,omitempty"`
	Name      string    `db:"name"`
	Comments  string    `db:"comments"`
	CreatedAt time.Time `db:"created_at"`
}

type Log struct {
	ID      int64           `db:"id,omitempty"`
	Message string          `db:"message"`
	Etc     dbtype.JSONBMap `db:"etc"`
}

type Stat struct {
	ID  int64         `db:"id,omitempty"`
	Key string        `db:"key"`
	Num dbtype.BigInt `db:"num"`
}

func (a *Account) DBTableName() string {
	return "accounts"
}
