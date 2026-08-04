package pgkit_test

import (
	"fmt"
	"testing"

	sq "github.com/Masterminds/squirrel"

	"github.com/goware/pgkit/v2"
)

type benchInsertItem struct {
	ID    int64  `db:"id"`
	Name  string `db:"name"`
	Email string `db:"email"`
	Age   int32  `db:"age"`
}

// BenchmarkInsertRecords measures building a multi-row INSERT from a uniform
// batch (every record has the same columns — the common case). The current
// implementation allocates a map[string]any plus a padded []any per row to
// support mixed-shape batches; a uniform batch pays that cost for nothing.
func BenchmarkInsertRecords(b *testing.B) {
	sb := &pgkit.StatementBuilder{StatementBuilderType: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}

	for _, n := range []int{10, 100} {
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			records := make([]benchInsertItem, n)
			for i := range records {
				records[i] = benchInsertItem{ID: int64(i), Name: "account name", Email: "user@example.com", Age: 30}
			}

			b.ReportAllocs()
			for b.Loop() {
				q := sb.InsertRecords(records, "items")
				if q.Err() != nil {
					b.Fatal(q.Err())
				}
				if _, _, err := q.ToSql(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
