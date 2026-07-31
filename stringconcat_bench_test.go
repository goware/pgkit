package pgkit_test

import (
	"testing"

	"github.com/goware/pgkit/v2"
	"github.com/stretchr/testify/require"
)

// TestStringConcatOutputUnchanged pins the exact strings produced by the hot
// query-building paths, so the fmt.Sprintf -> concatenation change is proven
// to be output-preserving (must pass before and after the fix).
func TestStringConcatOutputUnchanged(t *testing.T) {
	// Columns are quoted by pgx.Identifier.Sanitize().
	require.Equal(t, `"created_at" DESC`, pgkit.Sort{Column: "created_at", Order: pgkit.Desc}.String())
	require.Equal(t, `"name" ASC`, pgkit.Sort{Column: "name", Order: pgkit.Asc}.String())
	// Unset order defaults to ASC via sanitize.
	require.Equal(t, `"id" ASC`, pgkit.Sort{Column: "id"}.String())

	stmt := pgkit.RawQuery("SELECT * FROM t WHERE a = ? AND b = ? AND c = ?")
	require.NoError(t, stmt.Err())
	require.Equal(t, "SELECT * FROM t WHERE a = $1 AND b = $2 AND c = $3", stmt.GetQuery())
	require.Equal(t, 3, stmt.NumArgs())
}

// BenchmarkSortString measures Sort.String(), called once per ORDER BY column
// on every paginated and keyset query build.
func BenchmarkSortString(b *testing.B) {
	s := pgkit.Sort{Column: "created_at", Order: pgkit.Desc}
	b.ReportAllocs()
	for b.Loop() {
		_ = s.String()
	}
}

// BenchmarkRawQueryPrepare measures RawQuery, which runs the placeholder
// rewrite (?-> $N) in RawSQL.Prepare once per call.
func BenchmarkRawQueryPrepare(b *testing.B) {
	const q = "SELECT * FROM t WHERE a = ? AND b = ? AND c = ? AND d = ? AND e = ?"
	b.ReportAllocs()
	for b.Loop() {
		stmt := pgkit.RawQuery(q)
		if stmt.Err() != nil {
			b.Fatal(stmt.Err())
		}
	}
}
