package pgkit_test

import (
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goware/pgkit/v2"
)

func TestInsertRecords_UniformShape(t *testing.T) {
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
	sql, args, err := b.ToSql()
	require.NoError(t, err)
	assert.Equal(t, `INSERT INTO items (id,tags) VALUES ($1,$2),($3,$4)`, sql)
	assert.Equal(t, []any{1, []string{"a"}, 2, []string{"b"}}, args)
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

func TestInsertRecords_MixedShape_UnionsAndDefaults(t *testing.T) {
	// Heterogeneous batch: each row contributes a different column subset.
	// The union becomes the INSERT column list; missing slots become DEFAULT.
	type Item struct {
		ID   int      `db:"id"`
		Name string   `db:"name,omitzero"`
		Tags []string `db:"tags,omitzero"`
	}

	sb := &pgkit.StatementBuilder{StatementBuilderType: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
	records := []Item{
		{ID: 1, Name: "first"},                   // cols = [id, name]
		{ID: 2, Tags: []string{"foo"}},           // cols = [id, tags]
		{ID: 3, Name: "third", Tags: []string{}}, // cols = [id, name, tags] (omitzero keeps non-nil empty)
	}
	b := sb.InsertRecords(records, "items")
	require.NoError(t, b.Err())
	sql, args, err := b.ToSql()
	require.NoError(t, err)
	assert.Equal(t,
		`INSERT INTO items (id,name,tags) VALUES ($1,$2,DEFAULT),($3,DEFAULT,$4),($5,$6,$7)`,
		sql,
	)
	assert.Equal(t, []any{1, "first", 2, []string{"foo"}, 3, "third", []string{}}, args)
}

func TestInsertRecords_OmitZeroMixedSlices(t *testing.T) {
	// #50 used to reject this with a drift error. The union-with-DEFAULT
	// approach makes it valid: ,omitzero distinguishes nil (skipped → DEFAULT)
	// from non-nil empty (included with empty literal).
	type Item struct {
		ID   int      `db:"id"`
		Tags []string `db:"tags,omitzero"`
	}

	sb := &pgkit.StatementBuilder{StatementBuilderType: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
	records := []Item{
		{ID: 1, Tags: nil},        // tags skipped → will become DEFAULT
		{ID: 2, Tags: []string{}}, // tags included → empty array literal
	}
	b := sb.InsertRecords(records, "items")
	require.NoError(t, b.Err())
	sql, args, err := b.ToSql()
	require.NoError(t, err)
	assert.Equal(t, `INSERT INTO items (id,tags) VALUES ($1,DEFAULT),($2,$3)`, sql)
	assert.Equal(t, []any{1, 2, []string{}}, args)
}

func TestInsertRecords_OmitEmptyMixedMaps(t *testing.T) {
	// Legacy footgun resolved: ,omitempty on a map has always treated nil
	// and non-nil empty differently (DeepEqual sees them as distinct).
	// Now the union path handles it instead of rejecting.
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
	require.NoError(t, b.Err())
	sql, _, err := b.ToSql()
	require.NoError(t, err)
	assert.Equal(t, `INSERT INTO items (id,tags) VALUES ($1,DEFAULT),($2,$3)`, sql)
}

func TestInsertRecords_EmptyRowMixedWithNonEmpty(t *testing.T) {
	// A row with all-skipped fields can still appear in a batch: another row
	// contributes the column union, the empty row pads to all DEFAULT.
	type Item struct {
		Name string   `db:"name,omitzero"`
		Tags []string `db:"tags,omitzero"`
	}

	sb := &pgkit.StatementBuilder{StatementBuilderType: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
	records := []Item{
		{},                                    // empty → all DEFAULT
		{Name: "second", Tags: []string{"a"}}, // contributes the union
	}
	b := sb.InsertRecords(records, "items")
	require.NoError(t, b.Err())
	sql, args, err := b.ToSql()
	require.NoError(t, err)
	assert.Equal(t, `INSERT INTO items (name,tags) VALUES (DEFAULT,DEFAULT),($1,$2)`, sql)
	assert.Equal(t, []any{"second", []string{"a"}}, args)
}

func TestInsertRecords_AllRowsEmpty_Rejected(t *testing.T) {
	// Whole-batch empty union: no row contributed any column. Genuinely
	// out of InsertRecords' scope — caller wants InsertDefaults per row.
	type Item struct {
		Name string   `db:"name,omitzero"`
		Tags []string `db:"tags,omitzero"`
	}

	sb := &pgkit.StatementBuilder{StatementBuilderType: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
	records := []Item{{}, {}}
	b := sb.InsertRecords(records, "items")
	require.Error(t, b.Err())
	assert.Contains(t, b.Err().Error(), "no columns")
	assert.Contains(t, b.Err().Error(), `SQL.InsertDefaults("items")`)
}

func TestInsertRecords_MapRecords(t *testing.T) {
	// Map accepts records as map[string]any (mapper.go's reflect.Map case).
	// Heterogeneous map batches should union just like struct batches do.
	sb := &pgkit.StatementBuilder{StatementBuilderType: sq.StatementBuilder.PlaceholderFormat(sq.Dollar)}
	records := []map[string]any{
		{"id": 1, "name": "first"},
		{"id": 2, "tags": "foo"},
	}
	b := sb.InsertRecords(records, "items")
	require.NoError(t, b.Err())
	sql, _, err := b.ToSql()
	require.NoError(t, err)
	// Column order is lexical (Map sorts deterministically).
	assert.Equal(t, `INSERT INTO items (id,name,tags) VALUES ($1,$2,DEFAULT),($3,DEFAULT,$4)`, sql)
}
