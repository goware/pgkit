package pgkit_test

import (
	"testing"
	"time"

	"github.com/goware/pgkit/v2"
)

// benchMapRecord is a representative record: a mix of plain, omitempty, and
// pointer fields so Map has real per-field work (tag parsing, zero checks).
type benchMapRecord struct {
	ID        int64      `db:"id"`
	Name      string     `db:"name"`
	Email     string     `db:"email,omitempty"`
	Age       int32      `db:"age"`
	Score     float64    `db:"score,omitempty"`
	Active    bool       `db:"active"`
	Nickname  *string    `db:"nickname,omitempty"`
	Tags      []string   `db:"tags,omitempty"`
	CreatedAt time.Time  `db:"created_at"`
	UpdatedAt *time.Time `db:"updated_at,omitempty"`
	Ignored   string     // no db tag: must be skipped every call
}

// BenchmarkMap measures the per-record cost of Map, which write paths
// (InsertRecord, UpdateRecord, and once per record in InsertRecords) pay on
// every call. The struct type is fixed, so the tag scan, option parsing, and
// column sort are identical across calls.
func BenchmarkMap(b *testing.B) {
	nick := "nick"
	now := time.Now()
	rec := &benchMapRecord{
		ID:        42,
		Name:      "account name",
		Email:     "user@example.com",
		Age:       30,
		Score:     99.5,
		Active:    true,
		Nickname:  &nick,
		Tags:      []string{"a", "b", "c"},
		CreatedAt: now,
		UpdatedAt: &now,
	}

	b.ReportAllocs()
	for b.Loop() {
		cols, vals, err := pgkit.Map(rec)
		if err != nil {
			b.Fatal(err)
		}
		if len(cols) != len(vals) {
			b.Fatalf("cols=%d vals=%d", len(cols), len(vals))
		}
	}
}
