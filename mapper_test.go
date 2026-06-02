package pgkit_test

import (
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goware/pgkit/v2"
)

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

func TestMap_OmitEmpty_EmptyNonNilMap(t *testing.T) {
	// Regression guard: legacy omitempty only skipped a nil map. A
	// non-nil empty map stayed, so a "clear my jsonb" UPDATE actually
	// cleared. Keep that.
	type Record struct {
		Tags map[string]string `db:"tags,omitempty"`
	}
	got := mapFields(t, &Record{Tags: nil})
	assert.NotContains(t, got, "tags", "nil map skipped by omitempty")

	got = mapFields(t, &Record{Tags: map[string]string{}})
	require.Contains(t, got, "tags", "empty-but-non-nil map kept by omitempty")
	m, ok := got["tags"].(map[string]string)
	require.True(t, ok)
	assert.Len(t, m, 0)
}

func TestMap_OmitEmpty_AllZeroArray(t *testing.T) {
	// Regression guard for legacy omitempty behavior: pre-omitzero, the
	// array/slice branch only set isZero on Len()==0, which is only true
	// for the degenerate [0]T. The else-if chain blocked the DeepEqual
	// fallback, so a normal-length all-zero array ([16]byte UUIDs,
	// [32]byte hashes) was ALWAYS emitted under omitempty. Preserve that.
	type Record struct {
		Hash [16]byte `db:"hash,omitempty"`
	}
	got := mapFields(t, &Record{})
	assert.Contains(t, got, "hash", "all-zero array kept under omitempty (legacy)")

	got = mapFields(t, &Record{Hash: [16]byte{1}})
	assert.Contains(t, got, "hash", "non-zero array kept under omitempty")
}

func TestMap_OmitZero_AllZeroArray(t *testing.T) {
	// omitzero IS the strict-zero option, so an all-zero fixed-size array
	// is skipped here. Distinct from omitempty's array behavior above.
	type Record struct {
		Hash [16]byte `db:"hash,omitzero"`
	}
	got := mapFields(t, &Record{})
	assert.NotContains(t, got, "hash", "all-zero array skipped under omitzero")

	got = mapFields(t, &Record{Hash: [16]byte{1}})
	assert.Contains(t, got, "hash", "non-zero array kept under omitzero")
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
	// IsZero() interface takes precedence over Kind dispatch, so types
	// owning their own zero rule (time.Time, decimal, etc.) win.
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
	type Record struct {
		Name *string `db:"name,omitzero"`
	}
	cols, vals, err := pgkit.MapWithOptions(&Record{}, &pgkit.MapOptions{IncludeNil: true})
	require.NoError(t, err)
	require.Len(t, cols, 1)
	require.Equal(t, "name", cols[0])
	assert.Equal(t, sq.Expr("DEFAULT"), vals[0])
}

func TestMap_OmitZero_StructWithoutIsZero(t *testing.T) {
	// Struct fields without their own IsZero() fall through to the
	// DeepEqual-against-the-type's-zero arm, matching encoding/json's
	// omitzero semantic for plain structs.
	type Inner struct{ A, B int }
	type Record struct {
		Inner Inner `db:"inner,omitzero"`
	}
	got := mapFields(t, &Record{})
	assert.NotContains(t, got, "inner", "zero struct value skipped")

	got = mapFields(t, &Record{Inner: Inner{A: 1}})
	assert.Contains(t, got, "inner", "non-zero struct kept")
}

func TestMap_BothTags_OmitEmptyWins(t *testing.T) {
	// A field tagged with both options gets the broader omitempty skip
	// (zero-length non-nil slice is dropped) because (isEmpty && omitempty)
	// fires when (isStrictZero && omitzero) wouldn't.
	type Record struct {
		Slice []string `db:"slice,omitempty,omitzero"`
	}
	got := mapFields(t, &Record{Slice: []string{}})
	assert.NotContains(t, got, "slice", "omitempty wins on non-nil empty slice")
}

func TestMap_MapRecord_Unchanged(t *testing.T) {
	// Regression guard: the reflect.Map record path (record IS a map, not
	// a struct field) is independent of the field-walking changes above
	// and must keep emitting every key. The path stores values as
	// reflect.Value for downstream marshaling, which is pre-existing.
	record := map[string]any{"a": 1, "b": "x"}
	cols, vals, err := pgkit.Map(&record)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, cols)
	assert.Len(t, vals, 2)
}
