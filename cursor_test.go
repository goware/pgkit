package pgkit_test

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/goware/pgkit/v2"
	"github.com/stretchr/testify/require"
)

type rowCursor struct {
	ID string `json:"id"`
}

type row struct {
	ID string
}

func applyIDCursor(q sq.SelectBuilder, c rowCursor) sq.SelectBuilder {
	return q.Where(sq.Lt{"id": c.ID})
}

func cursorFromRow(r row) (rowCursor, error) {
	return rowCursor{ID: r.ID}, nil
}

func TestEncodeDecodeCursorRoundTrip(t *testing.T) {
	encoded, err := pgkit.EncodeCursor(rowCursor{ID: "row_1"})
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	decoded, err := pgkit.DecodeCursor[rowCursor](encoded)
	require.NoError(t, err)
	require.NotNil(t, decoded)
	require.Equal(t, "row_1", decoded.ID)
}

func TestDecodeCursorEmptyReturnsNil(t *testing.T) {
	decoded, err := pgkit.DecodeCursor[rowCursor]("")
	require.NoError(t, err)
	require.Nil(t, decoded)
}

func TestDecodeCursorInvalidBase64(t *testing.T) {
	_, err := pgkit.DecodeCursor[rowCursor]("!!!not-base64!!!")
	require.Error(t, err)
	require.True(t, errors.Is(err, pgkit.ErrInvalidCursor))
}

func TestDecodeCursorInvalidJSON(t *testing.T) {
	// Valid base64, invalid JSON payload.
	encoded, err := pgkit.EncodeCursor("not a struct")
	require.NoError(t, err)

	_, err = pgkit.DecodeCursor[rowCursor](encoded)
	require.Error(t, err)
	require.True(t, errors.Is(err, pgkit.ErrInvalidCursor))
}

func TestCursorPaginatorFirstPage(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor](
		pgkit.WithDefaultSize(2),
		pgkit.WithMaxSize(5),
	)
	page := &pgkit.Page{}

	result, q, err := paginator.PrepareQuery(sq.Select("*").From("t"), page, applyIDCursor)
	require.NoError(t, err)
	require.Len(t, result, 0)
	require.Equal(t, 3, cap(result))

	sql, args, err := q.ToSql()
	require.NoError(t, err)
	require.Equal(t, "SELECT * FROM t LIMIT 3", sql)
	require.Empty(t, args)
}

func TestCursorPaginatorWithCursor(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor](pgkit.WithDefaultSize(2))
	encoded, err := pgkit.EncodeCursor(rowCursor{ID: "row_5"})
	require.NoError(t, err)
	page := &pgkit.Page{Cursor: encoded}

	_, q, err := paginator.PrepareQuery(sq.Select("*").From("t"), page, applyIDCursor)
	require.NoError(t, err)

	sql, args, err := q.ToSql()
	require.NoError(t, err)
	require.Equal(t, "SELECT * FROM t WHERE id < ? LIMIT 3", sql)
	require.Equal(t, []any{"row_5"}, args)
}

func TestCursorPaginatorInvalidCursor(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor]()
	page := &pgkit.Page{Cursor: "!!!not-base64!!!"}

	_, _, err := paginator.PrepareQuery(sq.Select("*").From("t"), page, applyIDCursor)
	require.Error(t, err)
	require.True(t, errors.Is(err, pgkit.ErrInvalidCursor))
}

func TestCursorPaginatorPrepareResultNoMore(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor](pgkit.WithDefaultSize(3))
	page := &pgkit.Page{}
	_, _, err := paginator.PrepareQuery(sq.Select("*").From("t"), page, applyIDCursor)
	require.NoError(t, err)

	result, err := paginator.PrepareResult([]row{{ID: "1"}, {ID: "2"}}, page, cursorFromRow)
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.False(t, page.More)
	require.Empty(t, page.NextCursor)
	require.Equal(t, uint32(3), page.Size)
}

func TestCursorPaginatorPrepareResultHasMore(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor](pgkit.WithDefaultSize(2))
	page := &pgkit.Page{}
	_, _, err := paginator.PrepareQuery(sq.Select("*").From("t"), page, applyIDCursor)
	require.NoError(t, err)

	// Three rows returned, limit was 2 — the third signals "more".
	result, err := paginator.PrepareResult(
		[]row{{ID: "3"}, {ID: "2"}, {ID: "1"}},
		page,
		cursorFromRow,
	)
	require.NoError(t, err)
	require.Equal(t, []row{{ID: "3"}, {ID: "2"}}, result)
	require.True(t, page.More)
	require.NotEmpty(t, page.NextCursor)

	// NextCursor must round-trip back to the last surviving row.
	decoded, err := pgkit.DecodeCursor[rowCursor](page.NextCursor)
	require.NoError(t, err)
	require.NotNil(t, decoded)
	require.Equal(t, "2", decoded.ID)
}

func TestCursorPaginatorDefaultsFromNilPage(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor]()
	_, q, err := paginator.PrepareQuery(sq.Select("*").From("t"), nil, applyIDCursor)
	require.NoError(t, err)

	sql, _, err := q.ToSql()
	require.NoError(t, err)
	// Default page size is 10 → LIMIT 11.
	require.Equal(t, "SELECT * FROM t LIMIT 11", sql)
}

func TestCursorPaginatorCapsAtMaxSize(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor](
		pgkit.WithDefaultSize(5),
		pgkit.WithMaxSize(10),
	)
	page := &pgkit.Page{Size: 999}

	_, q, err := paginator.PrepareQuery(sq.Select("*").From("t"), page, applyIDCursor)
	require.NoError(t, err)

	sql, _, err := q.ToSql()
	require.NoError(t, err)
	require.Equal(t, "SELECT * FROM t LIMIT 11", sql)
	require.Equal(t, uint32(10), page.Size)
}

func TestCursorPaginatorMaxSizeBelowDefaultIsLifted(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor](
		pgkit.WithDefaultSize(20),
		pgkit.WithMaxSize(5),
	)
	page := &pgkit.Page{}

	_, q, err := paginator.PrepareQuery(sq.Select("*").From("t"), page, applyIDCursor)
	require.NoError(t, err)

	sql, _, err := q.ToSql()
	require.NoError(t, err)
	// MaxSize is lifted to DefaultSize, so DefaultSize wins → LIMIT 21.
	require.Equal(t, "SELECT * FROM t LIMIT 21", sql)
}

func TestCursorPaginatorWalksPages(t *testing.T) {
	// End-to-end: paginate a fixed 5-row dataset in pages of 2 and
	// verify every row surfaces exactly once across three pages.
	paginator := pgkit.NewCursorPaginator[row, rowCursor](pgkit.WithDefaultSize(2))
	all := []row{{ID: "5"}, {ID: "4"}, {ID: "3"}, {ID: "2"}, {ID: "1"}}

	var (
		page = &pgkit.Page{}
		seen []row
	)
	for step := 0; step < 5; step++ {
		_, q, err := paginator.PrepareQuery(sq.Select("*").From("t"), page, applyIDCursor)
		require.NoError(t, err)

		fetched := fetch(t, all, q)
		got, err := paginator.PrepareResult(fetched, page, cursorFromRow)
		require.NoError(t, err)

		seen = append(seen, got...)
		if !page.More {
			break
		}
		page.Cursor = page.NextCursor
		page.NextCursor = ""
	}
	require.Equal(t, all, seen)
	require.False(t, page.More)
}

func TestCursorPaginatorPrepareResultPropagatesCursorError(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor](pgkit.WithDefaultSize(1))
	page := &pgkit.Page{}
	_, _, err := paginator.PrepareQuery(sq.Select("*").From("t"), page, applyIDCursor)
	require.NoError(t, err)

	sentinel := errors.New("boom")
	_, err = paginator.PrepareResult(
		[]row{{ID: "2"}, {ID: "1"}},
		page,
		func(row) (rowCursor, error) { return rowCursor{}, sentinel },
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, sentinel))
}

func TestCursorPaginatorPanicsOnNilApplyCursor(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor]()
	require.PanicsWithValue(
		t,
		"pgkit: CursorPaginator.PrepareQuery: applyCursor must not be nil",
		func() { _, _, _ = paginator.PrepareQuery(sq.Select("*").From("t"), &pgkit.Page{}, nil) },
	)
}

func TestCursorPaginatorPanicsOnNilCursorFromRow(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor]()
	page := &pgkit.Page{}
	_, _, err := paginator.PrepareQuery(sq.Select("*").From("t"), page, applyIDCursor)
	require.NoError(t, err)
	require.PanicsWithValue(
		t,
		"pgkit: CursorPaginator.PrepareResult: cursorFromRow must not be nil",
		func() { _, _ = paginator.PrepareResult([]row{{ID: "1"}}, page, nil) },
	)
}

// In-memory stand-in so the pagination walk exercises encode/decode without a real database.
func fetch(t *testing.T, all []row, q sq.SelectBuilder) []row {
	t.Helper()
	sql, args, err := q.ToSql()
	require.NoError(t, err)

	limit, err := strconv.Atoi(sql[strings.LastIndex(sql, " ")+1:])
	require.NoError(t, err)

	cutoff := ""
	if len(args) == 1 {
		cutoff = args[0].(string)
	}

	out := make([]row, 0, limit)
	for _, r := range all {
		if cutoff != "" && r.ID >= cutoff {
			continue
		}
		out = append(out, r)
		if len(out) == limit {
			break
		}
	}
	return out
}
