package pgkit_test

import (
	"database/sql/driver"
	"time"

	"github.com/goware/pgkit/dbtype"
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
	ID      int64           `db:"id,omitempty"`
	Message string          `db:"message"`
	Etc     dbtype.JSONBMap `db:"etc"` // using JSONB postgres datatype
}

type Stat struct {
	ID  int64         `db:"id,omitempty"`
	Key string        `db:"key"`
	Num dbtype.BigInt `db:"big_num"` // using NUMERIC(78,0) postgres datatype
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

func (m Content) Value() (driver.Value, error) {
	return dbtype.JSONBValue(m)
}

func (m *Content) Scan(src interface{}) error {
	return dbtype.ScanJSONB(m, src)
}
