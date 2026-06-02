package pgkit_test

import (
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goware/pgkit/v2"
)

// mapFields runs Map and returns a column->value lookup for easier asserts.
func mapFields(t *testing.T, record any) map[string]any {
	t.Helper()
	cols, vals, err := pgkit.Map(record)
	require.NoError(t, err)
	require.Equal(t, len(cols), len(vals))
	out := make(map[string]any, len(cols))
	for i, c := range cols {
		out[c] = vals[i]
	}
	return out
}

func TestMap_OmitEmpty(t *testing.T) {
	type Record struct {
		Bare    string   `db:"bare"`
		Str     string   `db:"str,omitempty"`
		Slice   []string `db:"slice,omitempty"`
		PtrStr  *string  `db:"ptr_str,omitempty"`
		PtrSet  *string  `db:"ptr_set,omitempty"`
		Number  int      `db:"number,omitempty"`
		Filled  []string `db:"filled,omitempty"`
		Boolean bool     `db:"boolean,omitempty"`
	}

	set := "value"
	r := &Record{PtrSet: &set, Filled: []string{"a"}}

	got := mapFields(t, r)

	assert.Contains(t, got, "bare", "bare column always present")
	assert.NotContains(t, got, "str", "zero string skipped")
	assert.NotContains(t, got, "slice", "nil slice skipped")
	assert.NotContains(t, got, "ptr_str", "nil pointer skipped")
	assert.NotContains(t, got, "number", "zero int skipped")
	assert.NotContains(t, got, "boolean", "false bool skipped")
	assert.Contains(t, got, "ptr_set", "set pointer included")
	assert.Equal(t, "value", *(got["ptr_set"].(*string)))
	assert.Contains(t, got, "filled", "populated slice included")
}

func TestMap_OmitEmpty_EmptyNonNilSlice(t *testing.T) {
	// Regression guard: omitempty skips both nil and len-0 slices.
	type Record struct {
		Slice []string `db:"slice,omitempty"`
	}
	got := mapFields(t, &Record{Slice: []string{}})
	assert.NotContains(t, got, "slice", "omitempty drops empty-but-non-nil slice")
}

func TestMap_OmitZero_NilSlice(t *testing.T) {
	// nil slice is the zero value of a slice type: omitzero skips the column
	// so the DB DEFAULT applies on INSERT and the column is left untouched on
	// UPDATE.
	type Record struct {
		Slice []string `db:"slice,omitzero"`
	}
	got := mapFields(t, &Record{Slice: nil})
	assert.NotContains(t, got, "slice", "omitzero drops nil slice")
}

func TestMap_OmitZero_EmptyNonNilSlice(t *testing.T) {
	// The behavioral split versus omitempty: a non-nil empty slice is NOT
	// the zero value, so omitzero includes the column. This is what lets a
	// PATCH-style Update clear a NOT NULL DEFAULT '{}' array column without
	// the silent no-op trap omitempty has.
	type Record struct {
		Slice []string `db:"slice,omitzero"`
	}
	got := mapFields(t, &Record{Slice: []string{}})
	require.Contains(t, got, "slice", "omitzero keeps empty-but-non-nil slice")
	v, ok := got["slice"].([]string)
	require.True(t, ok, "value type preserved")
	assert.Len(t, v, 0)
}

func TestMap_OmitZero_PopulatedSlice(t *testing.T) {
	type Record struct {
		Slice []string `db:"slice,omitzero"`
	}
	got := mapFields(t, &Record{Slice: []string{"a", "b"}})
	require.Contains(t, got, "slice")
	assert.Equal(t, []string{"a", "b"}, got["slice"])
}

func TestMap_OmitZero_PrimitiveZeros(t *testing.T) {
	// Primitive zero values follow the same skip rule as omitempty.
	type Record struct {
		Str     string  `db:"str,omitzero"`
		Number  int     `db:"number,omitzero"`
		Boolean bool    `db:"boolean,omitzero"`
		PtrStr  *string `db:"ptr_str,omitzero"`
	}
	got := mapFields(t, &Record{})
	assert.NotContains(t, got, "str")
	assert.NotContains(t, got, "number")
	assert.NotContains(t, got, "boolean")
	assert.NotContains(t, got, "ptr_str")
}

func TestMap_OmitZero_TimeIsZero(t *testing.T) {
	// time.Time implements IsZero(); omitzero honors the interface like
	// omitempty does, so a zero-valued time is skipped.
	type Record struct {
		At time.Time `db:"at,omitzero"`
	}
	got := mapFields(t, &Record{})
	assert.NotContains(t, got, "at", "zero time skipped via IsZero")

	got = mapFields(t, &Record{At: time.Unix(1, 0)})
	assert.Contains(t, got, "at", "non-zero time included")
}

func TestMap_OmitZero_NilMap(t *testing.T) {
	type Record struct {
		Tags map[string]string `db:"tags,omitzero"`
	}
	got := mapFields(t, &Record{})
	assert.NotContains(t, got, "tags", "nil map skipped")

	got = mapFields(t, &Record{Tags: map[string]string{}})
	assert.Contains(t, got, "tags", "empty-but-non-nil map included")
}

func TestMapWithOptions_OmitZero_IncludeZeroed(t *testing.T) {
	// IncludeZeroed surfaces the skipped column with a SQL DEFAULT marker
	// instead of dropping it; same contract as omitempty.
	type Record struct {
		Slice []string `db:"slice,omitzero"`
	}
	cols, vals, err := pgkit.MapWithOptions(&Record{Slice: nil}, &pgkit.MapOptions{IncludeZeroed: true})
	require.NoError(t, err)
	require.Len(t, cols, 1)
	require.Equal(t, "slice", cols[0])
	assert.Equal(t, sq.Expr("DEFAULT"), vals[0])
}

func TestMapWithOptions_OmitZero_IncludeNil_Pointer(t *testing.T) {
	// IncludeNil surfaces a nil pointer with a DEFAULT marker.
	type Record struct {
		Name *string `db:"name,omitzero"`
	}
	cols, vals, err := pgkit.MapWithOptions(&Record{}, &pgkit.MapOptions{IncludeNil: true})
	require.NoError(t, err)
	require.Len(t, cols, 1)
	require.Equal(t, "name", cols[0])
	assert.Equal(t, sq.Expr("DEFAULT"), vals[0])
}
