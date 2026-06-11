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

type row struct {
	ID string
}

type rowCursor struct {
	ID string `json:"id"`
}

func (c *rowCursor) Apply(q sq.SelectBuilder) sq.SelectBuilder {
	return q.Where(sq.Lt{"id": c.ID})
}

func (c *rowCursor) OrderBy() []pgkit.Sort {
	return []pgkit.Sort{{Column: "id", Order: pgkit.Desc}}
}

func (c *rowCursor) From(r row) error {
	c.ID = r.ID
	return nil
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
	encoded, err := pgkit.EncodeCursor("not a struct")
	require.NoError(t, err)

	_, err = pgkit.DecodeCursor[rowCursor](encoded)
	require.Error(t, err)
	require.True(t, errors.Is(err, pgkit.ErrInvalidCursor))
}

func TestCursorPaginatorFirstPage(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor, *rowCursor](
		pgkit.WithDefaultSize(2),
		pgkit.WithMaxSize(5),
	)
	page := &pgkit.Page{}

	result, q, err := paginator.PrepareQuery(sq.Select("*").From("t"), page)
	require.NoError(t, err)
	require.Len(t, result, 0)
	require.Equal(t, 3, cap(result))

	sql, args, err := q.ToSql()
	require.NoError(t, err)
	require.Equal(t, `SELECT * FROM t ORDER BY "id" DESC LIMIT 3`, sql)
	require.Empty(t, args)
}

func TestCursorPaginatorWithCursor(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor, *rowCursor](pgkit.WithDefaultSize(2))
	encoded, err := pgkit.EncodeCursor(rowCursor{ID: "row_5"})
	require.NoError(t, err)
	page := &pgkit.Page{Cursor: encoded}

	_, q, err := paginator.PrepareQuery(sq.Select("*").From("t"), page)
	require.NoError(t, err)

	sql, args, err := q.ToSql()
	require.NoError(t, err)
	require.Equal(t, `SELECT * FROM t WHERE id < ? ORDER BY "id" DESC LIMIT 3`, sql)
	require.Equal(t, []any{"row_5"}, args)
}

func TestCursorPaginatorRejectsPreorderedQuery(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor, *rowCursor]()

	_, _, err := paginator.PrepareQuery(sq.Select("*").From("t").OrderBy("name"), &pgkit.Page{})
	require.ErrorIs(t, err, pgkit.ErrCursorQueryOrdered)
}

func TestCursorPaginatorAllowsMatchingPageOrder(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor, *rowCursor]()

	tests := []struct {
		name string
		page *pgkit.Page
	}{
		{
			name: "sort",
			page: &pgkit.Page{Sort: []pgkit.Sort{{Column: "id", Order: pgkit.Desc}}},
		},
		{
			name: "column",
			page: &pgkit.Page{Column: "-id"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, q, err := paginator.PrepareQuery(sq.Select("*").From("t"), tt.page)
			require.NoError(t, err)

			sql, args, err := q.ToSql()
			require.NoError(t, err)
			require.Equal(t, `SELECT * FROM t ORDER BY "id" DESC LIMIT 11`, sql)
			require.Empty(t, args)
		})
	}
}

func TestCursorPaginatorRejectsMismatchedPageOrder(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor, *rowCursor]()

	tests := []struct {
		name string
		page *pgkit.Page
	}{
		{
			name: "sort",
			page: &pgkit.Page{Sort: []pgkit.Sort{{Column: "name", Order: pgkit.Asc}}},
		},
		{
			name: "column",
			page: &pgkit.Page{Column: "name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := paginator.PrepareQuery(sq.Select("*").From("t"), tt.page)
			require.ErrorIs(t, err, pgkit.ErrCursorPageOrdered)
		})
	}
}

func TestCursorPaginatorInvalidCursor(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor, *rowCursor]()
	page := &pgkit.Page{Cursor: "!!!not-base64!!!"}

	_, _, err := paginator.PrepareQuery(sq.Select("*").From("t"), page)
	require.Error(t, err)
	require.True(t, errors.Is(err, pgkit.ErrInvalidCursor))
}

func TestCursorPaginatorPrepareResultNoMore(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor, *rowCursor](pgkit.WithDefaultSize(3))
	page := &pgkit.Page{}
	_, _, err := paginator.PrepareQuery(sq.Select("*").From("t"), page)
	require.NoError(t, err)

	result, err := paginator.PrepareResult([]row{{ID: "1"}, {ID: "2"}}, page)
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.False(t, page.More)
	require.Empty(t, page.NextCursor)
	require.Equal(t, uint32(3), page.Size)
}

func TestCursorPaginatorPrepareResultHasMore(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor, *rowCursor](pgkit.WithDefaultSize(2))
	page := &pgkit.Page{}
	_, _, err := paginator.PrepareQuery(sq.Select("*").From("t"), page)
	require.NoError(t, err)

	result, err := paginator.PrepareResult(
		[]row{{ID: "3"}, {ID: "2"}, {ID: "1"}},
		page,
	)
	require.NoError(t, err)
	require.Equal(t, []row{{ID: "3"}, {ID: "2"}}, result)
	require.True(t, page.More)
	require.NotEmpty(t, page.NextCursor)

	decoded, err := pgkit.DecodeCursor[rowCursor](page.NextCursor)
	require.NoError(t, err)
	require.NotNil(t, decoded)
	require.Equal(t, "2", decoded.ID)
}

func TestCursorPaginatorDefaultsFromNilPage(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor, *rowCursor]()
	_, q, err := paginator.PrepareQuery(sq.Select("*").From("t"), nil)
	require.NoError(t, err)

	sql, _, err := q.ToSql()
	require.NoError(t, err)
	require.Equal(t, `SELECT * FROM t ORDER BY "id" DESC LIMIT 11`, sql)
}

func TestCursorPaginatorCapsAtMaxSize(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor, *rowCursor](
		pgkit.WithDefaultSize(5),
		pgkit.WithMaxSize(10),
	)
	page := &pgkit.Page{Size: 999}

	_, q, err := paginator.PrepareQuery(sq.Select("*").From("t"), page)
	require.NoError(t, err)

	sql, _, err := q.ToSql()
	require.NoError(t, err)
	require.Equal(t, `SELECT * FROM t ORDER BY "id" DESC LIMIT 11`, sql)
	require.Equal(t, uint32(10), page.Size)
}

func TestCursorPaginatorMaxSizeBelowDefaultIsLifted(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor, *rowCursor](
		pgkit.WithDefaultSize(20),
		pgkit.WithMaxSize(5),
	)
	page := &pgkit.Page{}

	_, q, err := paginator.PrepareQuery(sq.Select("*").From("t"), page)
	require.NoError(t, err)

	sql, _, err := q.ToSql()
	require.NoError(t, err)
	require.Equal(t, `SELECT * FROM t ORDER BY "id" DESC LIMIT 21`, sql)
}

func TestCursorPaginatorWalksPages(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, rowCursor, *rowCursor](pgkit.WithDefaultSize(2))
	all := []row{{ID: "5"}, {ID: "4"}, {ID: "3"}, {ID: "2"}, {ID: "1"}}

	var (
		page = &pgkit.Page{}
		seen []row
	)
	for step := 0; step < 5; step++ {
		_, q, err := paginator.PrepareQuery(sq.Select("*").From("t"), page)
		require.NoError(t, err)

		fetched := fetch(t, all, q)
		got, err := paginator.PrepareResult(fetched, page)
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

type failingRowCursor struct {
	ID string `json:"id"`
}

func (c *failingRowCursor) Apply(q sq.SelectBuilder) sq.SelectBuilder {
	return q.Where(sq.Lt{"id": c.ID})
}

func (c *failingRowCursor) OrderBy() []pgkit.Sort {
	return []pgkit.Sort{{Column: "id", Order: pgkit.Desc}}
}

var errBoom = errors.New("boom")

func (c *failingRowCursor) From(row) error {
	return errBoom
}

func TestCursorPaginatorPrepareResultPropagatesCursorError(t *testing.T) {
	paginator := pgkit.NewCursorPaginator[row, failingRowCursor, *failingRowCursor](pgkit.WithDefaultSize(1))
	page := &pgkit.Page{}
	_, _, err := paginator.PrepareQuery(sq.Select("*").From("t"), page)
	require.NoError(t, err)

	_, err = paginator.PrepareResult([]row{{ID: "2"}, {ID: "1"}}, page)
	require.Error(t, err)
	require.True(t, errors.Is(err, errBoom))
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
