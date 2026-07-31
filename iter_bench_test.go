package pgkit

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/georgysavva/scany/v2/dbscan"
	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// benchRow is a representative record: a mix of column types so struct field
// mapping has real work to do per row.
type benchRow struct {
	ID     int64   `db:"id"`
	Name   string  `db:"name"`
	Email  string  `db:"email"`
	Age    int32   `db:"age"`
	Score  float64 `db:"score"`
	Active bool    `db:"active"`
}

func (r *benchRow) GetID() int64    { return r.ID }
func (r *benchRow) Validate() error { return nil }

// benchScanAPI mirrors the scan API pgkit builds in ConnectWithPGX so the
// benchmark exercises the same reflection path as production.
func benchScanAPI(tb testing.TB) *pgxscan.API {
	tb.Helper()
	dbScanAPI, err := pgxscan.NewDBScanAPI(dbscan.WithAllowUnknownColumns(true))
	if err != nil {
		tb.Fatal(err)
	}
	api, err := pgxscan.NewAPI(dbScanAPI)
	if err != nil {
		tb.Fatal(err)
	}
	return api
}

func benchTable(tb testing.TB) *Table[benchRow, *benchRow, int64] {
	return &Table[benchRow, *benchRow, int64]{
		DB: &DB{Query: &Querier{Scan: benchScanAPI(tb)}},
	}
}

// makeBenchData builds n identical rows matching benchRow's columns.
func makeBenchData(n int) ([]string, [][]any) {
	cols := []string{"id", "name", "email", "age", "score", "active"}
	data := make([][]any, n)
	for i := range data {
		data[i] = []any{
			int64(i),
			"account name",
			"user@example.com",
			int32(30),
			float64(99.5),
			true,
		}
	}
	return cols, data
}

func BenchmarkTableIter(b *testing.B) {
	for _, n := range []int{100, 1000} {
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			cols, data := makeBenchData(n)
			tbl := benchTable(b)
			b.ReportAllocs()
			for b.Loop() {
				rows := newFakeRows(cols, data)
				var count int
				for _, err := range tbl.iterRows(rows) {
					if err != nil {
						b.Fatal(err)
					}
					count++
				}
				if count != n {
					b.Fatalf("scanned %d rows, want %d", count, n)
				}
			}
		})
	}
}

// fakeRows is an in-memory pgx.Rows used to benchmark the scan loop without a
// live database. Scan copies the current row's values into the positional
// destination pointers via reflection.
type fakeRows struct {
	fields []pgconn.FieldDescription
	data   [][]any
	pos    int // 1-based index of the current row; 0 means before first Next
	err    error
}

func newFakeRows(cols []string, data [][]any) *fakeRows {
	fields := make([]pgconn.FieldDescription, len(cols))
	for i, c := range cols {
		fields[i] = pgconn.FieldDescription{Name: c}
	}
	return &fakeRows{fields: fields, data: data}
}

func (r *fakeRows) Close()                                       {}
func (r *fakeRows) Err() error                                   { return r.err }
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return r.fields }

func (r *fakeRows) Next() bool {
	if r.pos >= len(r.data) {
		return false
	}
	r.pos++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.pos == 0 || r.pos > len(r.data) {
		return fmt.Errorf("Scan called out of range")
	}
	row := r.data[r.pos-1]
	if len(dest) != len(row) {
		return fmt.Errorf("scan target count %d != column count %d", len(dest), len(row))
	}
	for i, d := range dest {
		if d == nil {
			continue
		}
		dv := reflect.ValueOf(d)
		if dv.Kind() != reflect.Pointer || dv.IsNil() {
			return fmt.Errorf("scan dest %d is not a non-nil pointer", i)
		}
		dv.Elem().Set(reflect.ValueOf(row[i]))
	}
	return nil
}

func (r *fakeRows) Values() ([]any, error) {
	if r.pos == 0 || r.pos > len(r.data) {
		return nil, fmt.Errorf("Values called out of range")
	}
	return r.data[r.pos-1], nil
}

func (r *fakeRows) RawValues() [][]byte { return nil }
func (r *fakeRows) Conn() *pgx.Conn     { return nil }
