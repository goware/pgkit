package pgkit_test

import (
	"database/sql/driver"
	"time"

	"github.com/goware/pgkit/dbtype"
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
	ID      int64    `db:"id,omitempty"`
	Message string   `db:"message"`
	Etc     JSONBMap `db:"etc"`
}

type Stat struct {
	ID  int64         `db:"id,omitempty"`
	Key string        `db:"key"`
	Num dbtype.BigInt `db:"num"`
}

func (a *Account) DBTableName() string {
	return "accounts"
}

type JSONB struct {
	V interface{}
}

// MarshalJSON encodes the wrapper value as JSON.
func (j JSONB) MarshalJSON() ([]byte, error) {
	t := &pgtype.JSONB{}
	if err := t.Set(j.V); err != nil {
		return nil, err
	}
	return t.MarshalJSON()
}

// UnmarshalJSON decodes the given JSON into the wrapped value.
func (j *JSONB) UnmarshalJSON(b []byte) error {
	t := &pgtype.JSONB{}
	if err := t.UnmarshalJSON(b); err != nil {
		return err
	}
	if j.V == nil {
		j.V = t.Get()
		return nil
	}
	if err := t.AssignTo(&j.V); err != nil {
		return err
	}
	return nil
}

// Scan satisfies the sql.Scanner interface.
func (j *JSONB) Scan(src interface{}) error {
	t := &pgtype.JSONB{}
	if err := t.Scan(src); err != nil {
		return err
	}
	if j.V == nil {
		j.V = t.Get()
		return nil
	}
	if err := t.AssignTo(&j.V); err != nil {
		return err
	}
	return nil
}

// Value satisfies the driver.Valuer interface.
func (j JSONB) Value() (driver.Value, error) {
	t := &pgtype.JSONB{}
	if err := t.Set(j.V); err != nil {
		return nil, err
	}
	return t.Value()
}

// JSONBMap represents a map of interfaces with string keys
// (`map[string]interface{}`) that is compatible with PostgreSQL's JSONB type.
// JSONBMap satisfies sqlbuilder.ScannerValuer.
type JSONBMap map[string]interface{}

// Value satisfies the driver.Valuer interface.
func (m JSONBMap) Value() (driver.Value, error) {
	return JSONBValue(m)
}

// Scan satisfies the sql.Scanner interface.
func (m *JSONBMap) Scan(src interface{}) error {
	*m = map[string]interface{}(nil)
	return ScanJSONB(m, src)
}

// JSONBValue takes an interface and provides a driver.Value that can be
// stored as a JSONB column.
func JSONBValue(i interface{}) (driver.Value, error) {
	v := JSONB{i}
	return v.Value()
}

// ScanJSONB decodes a JSON byte stream into the passed dst value.
func ScanJSONB(dst interface{}, src interface{}) error {
	v := JSONB{dst}
	return v.Scan(src)
}
