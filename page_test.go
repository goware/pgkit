package pgkit_test

import (
	"strings"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/goware/pgkit/v2"
	"github.com/stretchr/testify/require"
)

type T struct{}

func TestPagination(t *testing.T) {
	const (
		DefaultSize = 2
		MaxSize     = 5
		Sort        = "ID"
	)
	paginator := pgkit.NewPaginator[T](
		pgkit.WithColumnFunc(strings.ToLower),
		pgkit.WithDefaultSize(DefaultSize),
		pgkit.WithMaxSize(MaxSize),
		pgkit.WithSort(Sort),
	)
	page := pgkit.NewPage(0, 0)
	result, query := paginator.PrepareQuery(sq.Select("*").From("t"), page)
	require.Len(t, result, 0)
	require.Equal(t, &pgkit.Page{Page: 1, Size: DefaultSize}, page)

	sql, args, err := query.ToSql()
	require.NoError(t, err)
	require.Equal(t, `SELECT * FROM t ORDER BY "id" ASC LIMIT 3 OFFSET 0`, sql)
	require.Empty(t, args)
	// Verify page.Column and page.Sort are not modified
	require.Empty(t, page.Column)
	require.Len(t, page.Sort, 0)

	result = paginator.PrepareResult(make([]T, 0), page)
	require.Len(t, result, 0)
	require.Equal(t, &pgkit.Page{Page: 1, Size: DefaultSize}, page)

	result = paginator.PrepareResult(make([]T, DefaultSize), page)
	require.Len(t, result, DefaultSize)
	require.Equal(t, &pgkit.Page{Page: 1, Size: DefaultSize}, page)

	result = paginator.PrepareResult(make([]T, DefaultSize+2), page)
	require.Len(t, result, DefaultSize)
	require.Equal(t, &pgkit.Page{Page: 1, Size: DefaultSize, More: true}, page)
}

func TestInvalidSort(t *testing.T) {
	paginator := pgkit.NewPaginator[T]()
	page := pgkit.NewPage(0, 0)
	page.Sort = []pgkit.Sort{
		{Column: "ID; DROP TABLE users;", Order: pgkit.Asc},
		{Column: "name", Order: pgkit.Desc},
	}

	_, query := paginator.PrepareQuery(sq.Select("*").From("t"), page)

	sql, args, err := query.ToSql()
	require.NoError(t, err)
	require.Equal(t, "SELECT * FROM t ORDER BY \"ID; DROP TABLE users;\" ASC, \"name\" DESC LIMIT 11 OFFSET 0", sql)
	require.Empty(t, args)
	// Verify columns in page.Sort are not quoted
	require.Equal(t, "ID; DROP TABLE users;", page.Sort[0].Column)
	require.Equal(t, "name", page.Sort[1].Column)
}

func TestPageColumnInjection(t *testing.T) {
	paginator := pgkit.NewPaginator[T]()
	page := pgkit.NewPage(0, 0)
	page.Column = "id; DROP TABLE users;--"

	_, query := paginator.PrepareQuery(sq.Select("*").From("t"), page)

	sql, args, err := query.ToSql()
	require.NoError(t, err)
	require.Equal(t, "SELECT * FROM t ORDER BY \"id; DROP TABLE users;--\" ASC LIMIT 11 OFFSET 0", sql)
	require.Empty(t, args)
	// Verify column in page is not quoted
	require.Equal(t, "id; DROP TABLE users;--", page.Column)
}

func TestPageColumnSpaces(t *testing.T) {
	paginator := pgkit.NewPaginator[T]()
	page := pgkit.NewPage(0, 0)
	page.Column = "id, name"

	_, query := paginator.PrepareQuery(sq.Select("*").From("t"), page)

	sql, args, err := query.ToSql()
	require.NoError(t, err)
	require.Equal(t, "SELECT * FROM t ORDER BY \"id\" ASC, \"name\" ASC LIMIT 11 OFFSET 0", sql)
	require.Empty(t, args)
	// Verify column in page is not quoted
	require.Equal(t, "id, name", page.Column)
}

func TestSortOrderInjection(t *testing.T) {
	paginator := pgkit.NewPaginator[T]()
	page := pgkit.NewPage(0, 0)
	page.Sort = []pgkit.Sort{
		{Column: "id", Order: pgkit.Order("DESC; DROP TABLE users;--")},
		{Column: "name", Order: pgkit.Order("desc")},
		{Column: "created_at", Order: pgkit.Order(" ASC ")},
	}

	_, query := paginator.PrepareQuery(sq.Select("*").From("t"), page)

	sql, args, err := query.ToSql()
	require.NoError(t, err)
	require.Equal(t, "SELECT * FROM t ORDER BY \"id\" ASC, \"name\" DESC, \"created_at\" ASC LIMIT 11 OFFSET 0", sql)
	require.Empty(t, args)
	// Verify columns in page.Sort are not quoted
	require.Equal(t, "id", page.Sort[0].Column)
	require.Equal(t, "name", page.Sort[1].Column)
	require.Equal(t, "created_at", page.Sort[2].Column)
}

func TestPaginationEdgeCases(t *testing.T) {
	// Test case 1: nil options, NewPage with zeros
	paginator1 := pgkit.NewPaginator[T]()
	page1 := pgkit.NewPage(0, 0)
	result1, query1 := paginator1.PrepareQuery(sq.Select("*").From("t"), page1)
	require.Len(t, result1, 0)
	require.Equal(t, &pgkit.Page{Page: 1, Size: 10}, page1)

	sql1, _, err1 := query1.ToSql()
	require.NoError(t, err1)
	require.Equal(t, "SELECT * FROM t LIMIT 11 OFFSET 0", sql1)

	// Test case 2: nil options, empty struct assignment
	paginator2 := pgkit.NewPaginator[T]()
	page2 := &pgkit.Page{}
	result2, query2 := paginator2.PrepareQuery(sq.Select("*").From("t"), page2)
	require.Len(t, result2, 0)
	require.Equal(t, &pgkit.Page{Page: 1, Size: 10}, page2)

	sql2, _, err2 := query2.ToSql()
	require.NoError(t, err2)
	require.Equal(t, "SELECT * FROM t LIMIT 11 OFFSET 0", sql2)

	// Test case 3: empty options, NewPage
	paginator3 := pgkit.NewPaginator[T]()
	page3 := pgkit.NewPage(0, 0)
	result3, query3 := paginator3.PrepareQuery(sq.Select("*").From("t"), page3)
	require.Len(t, result3, 0)
	require.Equal(t, &pgkit.Page{Page: 1, Size: 10}, page3)

	sql3, _, err3 := query3.ToSql()
	require.NoError(t, err3)
	require.Equal(t, "SELECT * FROM t LIMIT 11 OFFSET 0", sql3)

	// Test case 4: max size lower than default size
	paginator4 := pgkit.NewPaginator[T](pgkit.WithDefaultSize(20), pgkit.WithMaxSize(5))
	page4 := &pgkit.Page{}
	result4, query4 := paginator4.PrepareQuery(sq.Select("*").From("t"), page4)
	require.Len(t, result4, 0)
	require.Equal(t, &pgkit.Page{Page: 1, Size: 20}, page4)

	sql4, _, err4 := query4.ToSql()
	require.NoError(t, err4)
	require.Equal(t, "SELECT * FROM t LIMIT 21 OFFSET 0", sql4)
}

func TestColumnFunc(t *testing.T) {
	fn := func(column string) string {
		switch column {
		case "id":
			return "ID"
		case "name":
			return "NAME"
		default:
			return column
		}
	}
	paginator := pgkit.NewPaginator[T](
		pgkit.WithColumnFunc(fn),
	)
	page := &pgkit.Page{
		Page: 1,
		Size: 10,
		Sort: []pgkit.Sort{
			{Column: "id", Order: pgkit.Asc},
			{Column: "name", Order: pgkit.Desc},
			{Column: "created_at", Order: pgkit.Asc},
		},
	}
	_, query := paginator.PrepareQuery(sq.Select("*").From("t"), page)

	sql, args, err := query.ToSql()
	require.NoError(t, err)
	require.Equal(t, `SELECT * FROM t ORDER BY "ID" ASC, "NAME" DESC, "created_at" ASC LIMIT 11 OFFSET 0`, sql)
	require.Empty(t, args)
	// Verify columns in page.Sort are not quoted
	require.Equal(t, "id", page.Sort[0].Column)
	require.Equal(t, "name", page.Sort[1].Column)
	require.Equal(t, "created_at", page.Sort[2].Column)
}

func TestColumnFallbackUsesColumnFunc(t *testing.T) {
	paginator := pgkit.NewPaginator[T](
		pgkit.WithColumnFunc(strings.ToUpper),
		pgkit.WithSort("id"),
	)
	page := &pgkit.Page{
		Page:   1,
		Size:   10,
		Column: "name",
	}

	_, query := paginator.PrepareQuery(sq.Select("*").From("t"), page)

	sql, args, err := query.ToSql()
	require.NoError(t, err)
	require.Equal(t, `SELECT * FROM t ORDER BY "NAME" ASC LIMIT 11 OFFSET 0`, sql)
	require.Empty(t, args)
	// Verify column in page is not quoted or transformed
	require.Equal(t, "name", page.Column)
}

func TestSortTakesPrecedenceOverColumn(t *testing.T) {
	paginator := pgkit.NewPaginator[T]()
	page := &pgkit.Page{
		Page:   1,
		Size:   10,
		Column: "name",
		Sort: []pgkit.Sort{
			{Column: "id", Order: pgkit.Desc},
		},
	}

	_, query := paginator.PrepareQuery(sq.Select("*").From("t"), page)

	sql, args, err := query.ToSql()
	require.NoError(t, err)
	require.Equal(t, `SELECT * FROM t ORDER BY "id" DESC LIMIT 11 OFFSET 0`, sql)
	require.Empty(t, args)
	// Verify sort column in page is not quoted
	require.Equal(t, "id", page.Sort[0].Column)
}

func TestPaginationOffsetAndPageRecompute(t *testing.T) {
	paginator := pgkit.NewPaginator[T]()
	page := &pgkit.Page{
		Page: 3,
		Size: 2,
	}

	_, query := paginator.PrepareQuery(sq.Select("*").From("t"), page)

	sql, args, err := query.ToSql()
	require.NoError(t, err)
	require.Equal(t, "SELECT * FROM t LIMIT 3 OFFSET 4", sql)
	require.Empty(t, args)

	result := paginator.PrepareResult(make([]T, 3), page)
	require.Len(t, result, 2)
	require.Equal(t, &pgkit.Page{Page: 3, Size: 2, More: true}, page)
}
