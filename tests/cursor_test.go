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

	page.Cursor = page.NextCursor
	third, thirdPage, err := paginator.Paginate(ctx, db.Query, q, page)
	require.NoError(t, err)
	require.Len(t, third, 1)
	require.False(t, thirdPage.More)
	require.Empty(t, thirdPage.NextCursor, "final page must not leak a stale cursor")
}

func TestTableListPagedCursor(t *testing.T) {
	ctx := t.Context()
	db := initDB(DB)

	account := &Account{Name: "ListPagedCursor Account"}
	require.NoError(t, db.Accounts.Save(ctx, account))

	for range 5 {
		require.NoError(t, db.Articles.Save(ctx, &Article{
			AccountID: account.ID,
			Author:    "Cursor Author",
		}))
	}
	where := sq.Eq{"account_id": account.ID}

	// pageAll starts from the given page and walks every page by NextCursor, collecting ids in server order.
	pageAll := func(t *testing.T, page *pgkit.Page) []uint64 {
		t.Helper()
		var ids []uint64
		for {
			rows, p, err := db.Articles.ListPaged(ctx, where, page)
			require.NoError(t, err)
			require.LessOrEqual(t, len(rows), 2)
			for _, r := range rows {
				ids = append(ids, r.ID)
			}
			if !p.More {
				require.Empty(t, p.NextCursor)
				break
			}
			require.NotEmpty(t, p.NextCursor)
			page = &pgkit.Page{Size: 2, Cursor: p.NextCursor}
		}
		return ids
	}

	descPage := func() *pgkit.Page {
		return &pgkit.Page{Size: 2, Sort: []pgkit.Sort{{Column: "id", Order: pgkit.Desc}}}
	}

	t.Run("Desc walks newest first without gaps or overlap", func(t *testing.T) {
		ids := pageAll(t, descPage())
		require.Len(t, ids, 5)
		for i := 1; i < len(ids); i++ {
			require.Greater(t, ids[i-1], ids[i], "ids must strictly descend across pages")
		}
	})

	t.Run("default order walks oldest first without gaps or overlap", func(t *testing.T) {
		ids := pageAll(t, &pgkit.Page{Size: 2})
		require.Len(t, ids, 5)
		for i := 1; i < len(ids); i++ {
			require.Less(t, ids[i-1], ids[i], "ids must strictly ascend across pages")
		}
	})

	t.Run("round-tripping the returned page continues the walk", func(t *testing.T) {
		page := descPage()
		var ids []uint64
		for {
			rows, p, err := db.Articles.ListPaged(ctx, where, page)
			require.NoError(t, err)
			for _, r := range rows {
				ids = append(ids, r.ID)
			}
			if !p.More {
				require.Empty(t, p.NextCursor, "final page must not leak a stale cursor")
				break
			}
			p.Cursor = p.NextCursor
			page = p
		}
		require.Len(t, ids, 5)
	})

	t.Run("non-id order emits no cursor", func(t *testing.T) {
		_, p, err := db.Articles.ListPaged(ctx, where, &pgkit.Page{Size: 2, Sort: []pgkit.Sort{{Column: "author"}}})
		require.NoError(t, err)
		require.True(t, p.More)
		require.Empty(t, p.NextCursor)
	})

	t.Run("cursor with a conflicting page order errors", func(t *testing.T) {
		_, first, err := db.Articles.ListPaged(ctx, where, descPage())
		require.NoError(t, err)
		_, _, err = db.Articles.ListPaged(ctx, where, &pgkit.Page{Cursor: first.NextCursor, Sort: []pgkit.Sort{{Column: "id", Order: pgkit.Asc}}})
		require.ErrorIs(t, err, pgkit.ErrCursorPageOrdered)
		_, _, err = db.Articles.ListPaged(ctx, where, &pgkit.Page{Cursor: first.NextCursor, Sort: []pgkit.Sort{{Column: "author"}}})
		require.ErrorIs(t, err, pgkit.ErrCursorPageOrdered)
	})

	t.Run("cursor with a page number errors", func(t *testing.T) {
		_, first, err := db.Articles.ListPaged(ctx, where, &pgkit.Page{Size: 2})
		require.NoError(t, err)
		_, _, err = db.Articles.ListPaged(ctx, where, &pgkit.Page{Page: 2, Cursor: first.NextCursor})
		require.ErrorIs(t, err, pgkit.ErrCursorPaged)
	})

	t.Run("rejects an undecodable cursor", func(t *testing.T) {
		_, _, err := db.Articles.ListPaged(ctx, where, &pgkit.Page{Cursor: "not-a-cursor"})
		require.ErrorIs(t, err, pgkit.ErrInvalidCursor)
	})

	t.Run("rejects a forged cursor order", func(t *testing.T) {
		type forgedCursor struct {
			ID    uint64      `json:"id"`
			Order pgkit.Order `json:"order"`
		}
		for _, order := range []pgkit.Order{"sideways", "asc", ""} {
			forged, err := pgkit.EncodeCursor(forgedCursor{ID: 1, Order: order})
			require.NoError(t, err)
			_, _, err = db.Articles.ListPaged(ctx, where, &pgkit.Page{Cursor: forged})
			require.ErrorIs(t, err, pgkit.ErrInvalidCursor, "order %q must be rejected", order)
		}
	})
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
