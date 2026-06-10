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

func TestInsertDefaults_PlainSQL(t *testing.T) {
	sb := &pgkit.StatementBuilder{StatementBuilderType: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
	b := sb.InsertDefaults("items")
	require.NoError(t, b.Err())
	sql, args, err := b.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "INSERT INTO items DEFAULT VALUES", sql)
	assert.Empty(t, args)
}

func TestInsertDefaults_WithReturning(t *testing.T) {
	sb := &pgkit.StatementBuilder{StatementBuilderType: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
	b := sb.InsertDefaults("items").Suffix(`RETURNING "id"`)
	require.NoError(t, b.Err())
	sql, _, err := b.ToSql()
	require.NoError(t, err)
	assert.Equal(t, `INSERT INTO items DEFAULT VALUES RETURNING "id"`, sql)
}

func TestInsertDefaults_MultipleSuffix(t *testing.T) {
	sb := &pgkit.StatementBuilder{StatementBuilderType: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
	b := sb.InsertDefaults("items").
		Suffix("ON CONFLICT (id) DO NOTHING").
		Suffix(`RETURNING "id"`)
	sql, _, err := b.ToSql()
	require.NoError(t, err)
	assert.Equal(t, `INSERT INTO items DEFAULT VALUES ON CONFLICT (id) DO NOTHING RETURNING "id"`, sql)
}

func TestInsertDefaults_OnConflictDoUpdateExcluded(t *testing.T) {
	// EXCLUDED-based upsert is the realistic conflict shape: literal SQL,
	// no placeholders. Proves Suffix covers the common case without sq.Expr.
	sb := &pgkit.StatementBuilder{StatementBuilderType: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
	b := sb.InsertDefaults("items").Suffix("ON CONFLICT (id) DO UPDATE SET updated_at = EXCLUDED.updated_at RETURNING id")
	sql, _, err := b.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "INSERT INTO items DEFAULT VALUES ON CONFLICT (id) DO UPDATE SET updated_at = EXCLUDED.updated_at RETURNING id", sql)
}

func TestInsertDefaults_EmptyTableErrors(t *testing.T) {
	// Build-time failure (not exec-time) + raw error (Querier owns the
	// pgkit: prefix; double-wrapping would surface "pgkit: pgkit: ...").
	sb := &pgkit.StatementBuilder{StatementBuilderType: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
	b := sb.InsertDefaults("")
	require.Error(t, b.Err())
	assert.Contains(t, b.Err().Error(), "table")
	_, _, err := b.ToSql()
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "pgkit: pgkit:")
}

func TestInsertDefaults_ZeroValueErrors(t *testing.T) {
	// Direct zero-value construction bypasses the InsertDefaults factory's
	// table check; ToSql must still error rather than emit invalid SQL.
	var b pgkit.DefaultValuesBuilder
	_, _, err := b.ToSql()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "table")
}

func TestInsertDefaults_EmptySuffixIsNoop(t *testing.T) {
	// Conditional-suffix callers (b.Suffix(returningClause) with returningClause
	// occasionally "") shouldn't get a trailing space that breaks SQL-string
	// snapshot tests.
	sb := &pgkit.StatementBuilder{StatementBuilderType: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
	b := sb.InsertDefaults("items").Suffix("")
	sql, _, err := b.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "INSERT INTO items DEFAULT VALUES", sql)
}

func TestInsertRecord_AllDefaultsErrorHintsAtInsertDefaults(t *testing.T) {
	type Item struct {
		Tags []string `db:"tags,omitzero"`
	}
	sb := &pgkit.StatementBuilder{StatementBuilderType: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
	b := sb.InsertRecord(&Item{}, "items")
	require.Error(t, b.Err())
	assert.Contains(t, b.Err().Error(), `SQL.InsertDefaults("items")`)
}

func TestInsertRecords_EmptyColumnsRejected(t *testing.T) {
	// Multi-row INSERT ... DEFAULT VALUES is not valid PG; the batch path
	// rejects all-default records and points at the single-row InsertRecord.
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
