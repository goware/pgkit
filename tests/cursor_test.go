package pgkit_test

import (
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/goware/pgkit/v2"
	"github.com/stretchr/testify/require"
)

type articleCursor struct {
	ID uint64 `json:"id"`
}

func (c *articleCursor) Apply(q sq.SelectBuilder) sq.SelectBuilder {
	return q.Where(sq.Lt{"id": c.ID})
}

func (c *articleCursor) OrderBy() []pgkit.Sort {
	return []pgkit.Sort{{Column: "id", Order: pgkit.Desc}}
}

func (c *articleCursor) From(article *Article) error {
	c.ID = article.ID
	return nil
}

func TestCursorPaginatorPaginateReturnsPage(t *testing.T) {
	ctx := t.Context()
	db := initDB(DB)

	account := &Account{Name: "CursorPaginatorPaginate Account"}
	err := db.Accounts.Save(ctx, account)
	require.NoError(t, err)

	for range 5 {
		err := db.Articles.Save(ctx, &Article{
			AccountID: account.ID,
			Author:    "Cursor Author",
		})
		require.NoError(t, err)
	}

	paginator := pgkit.NewCursorPaginator[*Article, articleCursor, *articleCursor](
		pgkit.WithDefaultSize(2),
	)
	q := db.SQL.Select("*").
		From("articles").
		Where(sq.Eq{"account_id": account.ID})

	first, firstPage, err := paginator.Paginate(ctx, db.Query, q, nil)
	require.NoError(t, err)
	require.Len(t, first, 2)
	require.NotNil(t, firstPage)
	require.Equal(t, uint32(2), firstPage.Size)
	require.True(t, firstPage.More)
	require.NotEmpty(t, firstPage.NextCursor)

	page := &pgkit.Page{
		Cursor: firstPage.NextCursor,
	}
	second, secondPage, err := paginator.Paginate(ctx, db.Query, q, page)
	require.NoError(t, err)
	require.Len(t, second, 2)
	require.Same(t, page, secondPage)
	require.True(t, secondPage.More)
	require.NotEmpty(t, secondPage.NextCursor)

	for _, a := range first {
		for _, b := range second {
			require.NotEqual(t, a.ID, b.ID, "cursor pages should not overlap")
		}
	}
}

func TestPaginatorPaginateReturnsPage(t *testing.T) {
	ctx := t.Context()
	db := initDB(DB)

	account := &Account{Name: "PaginatorPaginate Account"}
	err := db.Accounts.Save(ctx, account)
	require.NoError(t, err)

	for range 5 {
		err := db.Articles.Save(ctx, &Article{
			AccountID: account.ID,
			Author:    "Offset Author",
		})
		require.NoError(t, err)
	}

	paginator := pgkit.NewPaginator[*Article](pgkit.WithDefaultSize(2))
	q := db.SQL.Select("*").
		From("articles").
		Where(sq.Eq{"account_id": account.ID})

	first, firstPage, err := paginator.Paginate(ctx, db.Query, q, nil)
	require.NoError(t, err)
	require.Len(t, first, 2)
	require.NotNil(t, firstPage)
	require.Equal(t, uint32(2), firstPage.Size)
	require.Equal(t, uint32(1), firstPage.Page)
	require.True(t, firstPage.More)

	page := &pgkit.Page{Page: 2}
	second, secondPage, err := paginator.Paginate(ctx, db.Query, q, page)
	require.NoError(t, err)
	require.Len(t, second, 2)
	require.Same(t, page, secondPage)
	require.Equal(t, uint32(2), secondPage.Size)
	require.Equal(t, uint32(2), secondPage.Page)
	require.True(t, secondPage.More)

	for _, a := range first {
		for _, b := range second {
			require.NotEqual(t, a.ID, b.ID, "offset pages should not overlap")
		}
	}
}
