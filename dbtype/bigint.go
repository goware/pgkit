package dbtype

import (
	"database/sql/driver"
	"fmt"
	"math/big"
	"strings"

	"github.com/jackc/pgtype"
	"github.com/kr/pretty"
)

// NullBigInt is a type that represents big.Int that may be null, this type is
// used for JSON/Database marshalling.
//
// For JSON values we encoded NullBigInt's as strings.
//
// For Database values we encoded NullBigInt's as NUMERIC(78).
type NullBigInt struct {
	BigInt big.Int
	Valid  bool
}

func NewNullBigInt(n int64) NullBigInt {
	b := big.NewInt(n)
	return NullBigInt{*b, true}
}

func NewNullBigIntFromString(s string, base int) NullBigInt {
	b := big.NewInt(0)
	b, _ = b.SetString(s, base)
	return NullBigInt{*b, true}
}

func ToNullBigInt(b *big.Int) NullBigInt {
	if b == nil {
		return NullBigInt{}
	}
	return NullBigInt{*b, true}
}

func ToNullBigIntArray(bs []*big.Int) []NullBigInt {
	var pbs []NullBigInt
	for _, b := range bs {
		pbs = append(pbs, ToNullBigInt(b))
	}
	return pbs
}

func ToBigIntArrayFromStringArray(s []string, base int) ([]NullBigInt, error) {
	var pbs []NullBigInt
	for _, v := range s {
		b, ok := (&big.Int{}).SetString(v, base)
		if !ok {
			return nil, fmt.Errorf("invalid number %s", s)
		}
		pbs = append(pbs, ToNullBigInt(b))
	}
	return pbs, nil
}

func ToNullBigIntFromInt64(n int64) NullBigInt {
	return ToNullBigInt(big.NewInt(n))
}

func (b *NullBigInt) SetString(s string, base int) bool {
	v := big.Int(b.BigInt)
	n, ok := v.SetString(s, base)
	if !ok {
		return false
	}
	b.BigInt = *n
	b.Valid = true
	return true
}

func (b NullBigInt) String() string {
	if b.Valid {
		return b.Int().String()
	}
	return ""
}

func (b NullBigInt) Int() *big.Int {
	if b.Valid {
		v := big.Int(b.BigInt)
		return &v
	}
	return nil
}

func (b NullBigInt) Uint64() uint64 {
	if b.Valid {
		return b.Int().Uint64()
	}
	return 0
}

func (b NullBigInt) Int64() int64 {
	if b.Valid {
		return b.Int().Int64()
	}
	return 0
}

func (b *NullBigInt) Add(n *big.Int) {
	z := b.Int().Add(b.Int(), n)
	*b = NullBigInt{*z, true}
}

func (b *NullBigInt) Sub(n *big.Int) {
	z := b.Int().Sub(b.Int(), n)
	*b = NullBigInt{*z, true}
}

func (b NullBigInt) Equals(n *big.Int) bool {
	return b.Int().Cmp(n) == 0
}

func (b NullBigInt) Gt(n *big.Int) bool {
	return b.Int().Cmp(n) == 1
}

func (b NullBigInt) Gte(n *big.Int) bool {
	return b.Int().Cmp(n) == 0 || b.Int().Cmp(n) == 1
}

func (b NullBigInt) Lt(n *big.Int) bool {
	return b.Int().Cmp(n) == -1
}

func (b NullBigInt) Lte(n *big.Int) bool {
	return b.Int().Cmp(n) == 0 || b.Int().Cmp(n) == -1
}

// MarshalText implements encoding.TextMarshaler.
func (b NullBigInt) MarshalText() ([]byte, error) {
	v := fmt.Sprintf("%q", b.String())
	return []byte(v), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (b *NullBigInt) UnmarshalText(text []byte) error {
	t := string(text)
	if len(text) <= 2 || t == "null" || t == "" {
		return nil
	}
	i, _ := big.NewInt(0).SetString(string(text[1:len(text)-1]), 10)
	*b = NullBigInt{*i, true}
	return nil
}

func (b NullBigInt) Value() (driver.Value, error) {
	if b.Valid {
		return b.BigInt.String(), nil
	}
	return nil, nil
}

func (b *NullBigInt) Scan(src interface{}) error {
	b.Valid = false
	if src == nil {
		return nil
	}

	var svalue string
	switch v := src.(type) {
	case string:
		svalue = v
	case []byte:
		svalue = string(v)
	default:
		return fmt.Errorf("NullBigInt.Scan: unexpected type %T", src)
	}

	// pgx driver returns NeX where N is digits and X is exponent
	parts := strings.SplitN(svalue, "e", 2)

	var ok bool
	i := &big.Int{}
	i, ok = i.SetString(parts[0], 10)
	if !ok {
		return fmt.Errorf("NullBigInt.Scan: failed to scan value %q", svalue)
	}

	if len(parts) >= 2 {
		exp := big.NewInt(0)
		exp, ok = exp.SetString(parts[1], 10)
		if !ok {
			return fmt.Errorf("BigInt.Scan failed to scan exp component %q", svalue)
		}
		i = i.Mul(i, big.NewInt(1).Exp(big.NewInt(10), exp, nil))
	}

	b.BigInt = *i
	b.Valid = true

	return nil
}

// MarshalJSON implements json.Marshaler
func (b NullBigInt) MarshalJSON() ([]byte, error) {
	return b.MarshalText()
}

// UnmarshalJSON implements json.Unmarshaler
func (b *NullBigInt) UnmarshalJSON(text []byte) error {
	if string(text) == "null" {
		return nil
	}
	return b.UnmarshalText(text)
}

// func (src *Point) AssignTo(dst interface{}) error {
// 	return fmt.Errorf("cannot assign %v to %T", src, dst)
// }

func (b NullBigInt) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	pretty.Println(src)
	// panic("geez")
	err := b.Scan(src)
	if err != nil {
		panic(err)
	}
	return nil
}

func (dst *NullBigInt) Set(src interface{}) error {
	panic("common")
	// return fmt.Errorf("cannot convert %v to Point", src)
}

func (dst *NullBigInt) Get() interface{} {
	panic("ahh")
	// switch dst.Status {
	// case pgtype.Present:
	// 	return dst
	// case pgtype.Null:
	// 	return nil
	// default:
	// 	return dst.Status
	// }
}

// BigInt pgx custom type assignment
func (src *NullBigInt) AssignTo(dst interface{}) error {
	panic("wee")
}
