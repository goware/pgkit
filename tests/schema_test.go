package pgkit_test

import (
	"time"

	"github.com/goware/pgkit/v2/dbtype"
)

type Account struct {
	ID        int64     `db:"id,omitempty"`
	Name      string    `db:"name"`
	Disabled  bool      `db:"disabled"`
	CreatedAt time.Time `db:"created_at,omitempty"` // ,omitempty will rely on postgres DEFAULT
}

func (a *Account) DBTableName() string {
	return "accounts"
}

type Review struct {
	ID        int64     `db:"id,omitempty"`
	Name      string    `db:"name"`
	Comments  string    `db:"comments"`
	CreatedAt time.Time `db:"created_at"` // if unset, will store Go zero-value
}

type Log struct {
	ID      int64  `db:"id,omitempty"`
	Message string `db:"message"`
	// RawData []byte                 `db:"raw_data"`
	RawData dbtype.HexBytes        `db:"raw_data"`
	Etc     map[string]interface{} `db:"etc"` // using JSONB postgres datatype
}

type Stat struct {
	ID     int64         `db:"id,omitempty"`
	Key    string        `db:"key"`
	Num    dbtype.BigInt `db:"big_num"` // using NUMERIC(78,0) postgres datatype
	Rating dbtype.BigInt `db:"rating"`  // using NUMERIC(78,0) postgres datatype
}

type Article struct {
	ID      int64   `db:"id,omitempty"`
	Author  string  `db:"author"`
	Alias   *string `db:"alias"`
	Content Content `db:"content"` // using JSONB postgres datatype
}

type Content struct {
	Title string `json:"title"`
	Body  string `json:"body"`
	Views int64  `json:"views"`
}
