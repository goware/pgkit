package pgkit_test

import (
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goware/pgkit/v2"
)

func TestInsertRecords_ColumnDriftRejected(t *testing.T) {
	// ,omitzero produces different column shapes for nil vs non-nil empty
	// slices; squirrel would otherwise stitch the mismatched widths into
	// malformed multi-row SQL and surface only at exec time.
	type Item struct {
		ID   int      `db:"id"`
		Tags []string `db:"tags,omitzero"`
	}

	sb := &pgkit.StatementBuilder{StatementBuilderType: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
	records := []Item{
		{ID: 1, Tags: nil},
		{ID: 2, Tags: []string{}},
	}
	b := sb.InsertRecords(records, "items")
	require.Error(t, b.Err())
	assert.Contains(t, b.Err().Error(), "differ from record 0")
}

func TestInsertRecords_UniformShape(t *testing.T) {
	// Sanity: batches with consistent column shape across rows still build.
	type Item struct {
		ID   int      `db:"id"`
		Tags []string `db:"tags,omitzero"`
	}

	sb := &pgkit.StatementBuilder{StatementBuilderType: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
	records := []Item{
		{ID: 1, Tags: []string{"a"}},
		{ID: 2, Tags: []string{"b"}},
	}
	b := sb.InsertRecords(records, "items")
	require.NoError(t, b.Err())
}

func TestInsertRecord_EmptyColumnsRejected(t *testing.T) {
	// All fields tagged ,omitzero (or ,omitempty) and all zero leaves
	// Map with no columns. Squirrel would emit invalid INSERT INTO t
	// VALUES (); fail fast at build time and point at sq.Expr as the
	// escape for the all-default INSERT case. Tracked in goware/pgkit#51.
	type Item struct {
		Tags []string `db:"tags,omitzero"`
	}
	sb := &pgkit.StatementBuilder{StatementBuilderType: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
	b := sb.InsertRecord(&Item{}, "items")
	require.Error(t, b.Err())
	assert.Contains(t, b.Err().Error(), "no columns")
	assert.Contains(t, b.Err().Error(), "sq.Expr")
}

func TestInsertRecords_EmptyColumnsRejected(t *testing.T) {
	type Item struct {
		Tags []string `db:"tags,omitzero"`
	}
	sb := &pgkit.StatementBuilder{StatementBuilderType: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
	records := []Item{{}, {}}
	b := sb.InsertRecords(records, "items")
	require.Error(t, b.Err())
	assert.Contains(t, b.Err().Error(), "no columns")
}

func TestInsertRecords_OmitEmptyMapDriftRejected(t *testing.T) {
	// Latent footgun ,omitzero exposes: legacy ,omitempty on a map already
	// produced shape drift (nil map skipped, non-nil empty map kept via the
	// DeepEqual fallback). The validation catches this case for free.
	type Item struct {
		ID   int               `db:"id"`
		Tags map[string]string `db:"tags,omitempty"`
	}

	sb := &pgkit.StatementBuilder{StatementBuilderType: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
	records := []Item{
		{ID: 1, Tags: nil},
		{ID: 2, Tags: map[string]string{}},
	}
	b := sb.InsertRecords(records, "items")
	require.Error(t, b.Err())
}
