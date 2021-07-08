package dbtype

import (
	"database/sql/driver"

	"github.com/jackc/pgtype"
)

// NOTE: thanks to @xiam and github.com/upper/db for the code below.

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

// JSONBArray represents an array of any type (`[]interface{}`) that is
// compatible with PostgreSQL's JSONB type. JSONBArray satisfies
// sqlbuilder.ScannerValuer.
type JSONBArray []interface{}

// Value satisfies the driver.Valuer interface.
func (a JSONBArray) Value() (driver.Value, error) {
	return JSONBValue(a)
}

// Scan satisfies the sql.Scanner interface.
func (a *JSONBArray) Scan(src interface{}) error {
	return ScanJSONB(a, src)
}
