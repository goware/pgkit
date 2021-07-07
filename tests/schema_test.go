package pgkit_test

import (
	"math/big"
	"time"

	"github.com/jackc/pgtype"
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
	ID      int64        `db:"id,omitempty"`
	Message string       `db:"message"`
	Etc     pgtype.JSONB `db:"etc"`
}

type Stat struct {
	ID  int64    `db:"id,omitempty"`
	Key string   `db:"key"`
	Num *big.Int `db:"num"`
}

func (a *Account) DBTableName() string {
	return "accounts"
}
