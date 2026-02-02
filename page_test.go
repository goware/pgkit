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
	paginator := pgkit.NewPaginator(
		pgkit.WithColumnFunc[T](strings.ToLower),
		pgkit.WithDefaultSize[T](DefaultSize),
		pgkit.WithMaxSize[T](MaxSize),
		pgkit.WithSort[T](Sort),
	)
	page := pgkit.NewPage(0, 0)
	result, query := paginator.PrepareQuery(sq.Select("*").From("t"), page)
	require.Len(t, result, 0)
	require.Equal(t, &pgkit.Page{Page: 1, Size: MaxSize}, page)

	sql, args, err := query.ToSql()
	require.NoError(t, err)
	require.Equal(t, "SELECT * FROM t ORDER BY id ASC LIMIT 6 OFFSET 0", sql)
	require.Empty(t, args)

	result = paginator.PrepareResult(make([]T, 0), page)
	require.Len(t, result, 0)
	require.Equal(t, &pgkit.Page{Page: 1, Size: MaxSize}, page)

	result = paginator.PrepareResult(make([]T, MaxSize), page)
	require.Len(t, result, MaxSize)
	require.Equal(t, &pgkit.Page{Page: 1, Size: MaxSize}, page)

	result = paginator.PrepareResult(make([]T, MaxSize+2), page)
	require.Len(t, result, MaxSize)
	require.Equal(t, &pgkit.Page{Page: 1, Size: MaxSize, More: true}, page)
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
}
