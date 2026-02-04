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
	o := &pgkit.PaginatorOptions{
		ColumnFunc:  strings.ToLower,
		DefaultSize: 2,
		MaxSize:     5,
		Sort:        []string{"ID"},
	}
	paginator := pgkit.NewPaginator[T](o)
	page := pgkit.NewPage(0, 0)
	result, query := paginator.PrepareQuery(sq.Select("*").From("t"), page)
	require.Len(t, result, 0)
	require.Equal(t, &pgkit.Page{Page: 1, Size: o.MaxSize}, page)

	sql, args, err := query.ToSql()
	require.NoError(t, err)
	require.Equal(t, "SELECT * FROM t ORDER BY id ASC LIMIT 6 OFFSET 0", sql)
	require.Empty(t, args)

	result = paginator.PrepareResult(make([]T, 0), page)
	require.Len(t, result, 0)
	require.Equal(t, &pgkit.Page{Page: 1, Size: o.MaxSize}, page)

	result = paginator.PrepareResult(make([]T, o.MaxSize), page)
	require.Len(t, result, int(o.MaxSize))
	require.Equal(t, &pgkit.Page{Page: 1, Size: o.MaxSize}, page)

	result = paginator.PrepareResult(make([]T, o.MaxSize+2), page)
	require.Len(t, result, int(o.MaxSize))
	require.Equal(t, &pgkit.Page{Page: 1, Size: o.MaxSize, More: true}, page)
}

func TestInvalidSort(t *testing.T) {
	paginator := pgkit.NewPaginator[T](nil)
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
}

func TestPageColumnInjection(t *testing.T) {
	paginator := pgkit.NewPaginator[T](nil)
	page := pgkit.NewPage(0, 0)
	page.Column = "id; DROP TABLE users;--"

	_, query := paginator.PrepareQuery(sq.Select("*").From("t"), page)

	sql, args, err := query.ToSql()
	require.NoError(t, err)
	require.Equal(t, "SELECT * FROM t ORDER BY \"id; DROP TABLE users;--\" ASC LIMIT 11 OFFSET 0", sql)
	require.Empty(t, args)
}

func TestPageColumnSpaces(t *testing.T) {
	paginator := pgkit.NewPaginator[T](nil)
	page := pgkit.NewPage(0, 0)
	page.Column = "id, name"

	_, query := paginator.PrepareQuery(sq.Select("*").From("t"), page)

	sql, args, err := query.ToSql()
	require.NoError(t, err)
	require.Equal(t, "SELECT * FROM t ORDER BY \"id\" ASC, \"name\" ASC LIMIT 11 OFFSET 0", sql)
	require.Empty(t, args)
}

func TestSortOrderInjection(t *testing.T) {
	paginator := pgkit.NewPaginator[T](nil)
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
}

func TestPaginationEdgeCases(t *testing.T) {
	// Test case 1: nil options, NewPage with zeros
	paginator1 := pgkit.NewPaginator[T](nil)
	page1 := pgkit.NewPage(0, 0)
	result1, query1 := paginator1.PrepareQuery(sq.Select("*").From("t"), page1)
	require.Len(t, result1, 0)
	require.Equal(t, &pgkit.Page{Page: 1, Size: 10}, page1)

	sql1, _, err1 := query1.ToSql()
	require.NoError(t, err1)
	require.Equal(t, "SELECT * FROM t LIMIT 11 OFFSET 0", sql1)

	// Test case 2: nil options, empty struct assignment
	paginator2 := pgkit.NewPaginator[T](nil)
	page2 := &pgkit.Page{}
	result2, query2 := paginator2.PrepareQuery(sq.Select("*").From("t"), page2)
	require.Len(t, result2, 0)
	require.Equal(t, &pgkit.Page{Page: 1, Size: 10}, page2)

	sql2, _, err2 := query2.ToSql()
	require.NoError(t, err2)
	require.Equal(t, "SELECT * FROM t LIMIT 11 OFFSET 0", sql2)

	// Test case 3: empty options, NewPage
	paginator3 := pgkit.NewPaginator[T](&pgkit.PaginatorOptions{})
	page3 := pgkit.NewPage(0, 0)
	result3, query3 := paginator3.PrepareQuery(sq.Select("*").From("t"), page3)
	require.Len(t, result3, 0)
	require.Equal(t, &pgkit.Page{Page: 1, Size: 10}, page3)

	sql3, _, err3 := query3.ToSql()
	require.NoError(t, err3)
	require.Equal(t, "SELECT * FROM t LIMIT 11 OFFSET 0", sql3)

	// Test case 4: options with defaults, struct assignment
	paginator4 := pgkit.Paginator[T]{pgkit.PaginatorOptions{DefaultSize: 5, MaxSize: 20}}
	page4 := &pgkit.Page{}
	result4, query4 := paginator4.PrepareQuery(sq.Select("*").From("t"), page4)
	require.Len(t, result4, 0)
	require.Equal(t, &pgkit.Page{Page: 1, Size: 5}, page4)

	sql4, _, err4 := query4.ToSql()
	require.NoError(t, err4)
	require.Equal(t, "SELECT * FROM t LIMIT 6 OFFSET 0", sql4)

	// Test case 5: max size lower than default size
	paginator5 := pgkit.NewPaginator[T](&pgkit.PaginatorOptions{DefaultSize: 20, MaxSize: 5})
	page5 := &pgkit.Page{}
	result5, query5 := paginator5.PrepareQuery(sq.Select("*").From("t"), page5)
	require.Len(t, result5, 0)
	require.Equal(t, &pgkit.Page{Page: 1, Size: 20}, page5)

	sql5, _, err5 := query5.ToSql()
	require.NoError(t, err5)
	require.Equal(t, "SELECT * FROM t LIMIT 21 OFFSET 0", sql5)
}
