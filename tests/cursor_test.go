package pgkit_test

import (
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/goware/pgkit/v2"
	"github.com/jackc/pgx/v5"
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
	cur := db.Articles.WithMode(pgkit.CursorBased) // mode is the table's property, not the page's

	// walk follows NextCursor from the given first page to the end, collecting ids in server order.
	walk := func(t *testing.T, first *pgkit.Page) []uint64 {
		t.Helper()
		var ids []uint64
		page := first
		for {
			rows, p, err := cur.ListPaged(ctx, where, page)
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

	t.Run("cursor table walks ascending by default", func(t *testing.T) {
		ids := walk(t, &pgkit.Page{Size: 2})
		require.Len(t, ids, 5)
		for i := 1; i < len(ids); i++ {
			require.Less(t, ids[i-1], ids[i], "ids must strictly ascend across pages")
		}
	})

	t.Run("cursor table walks descending when the first page asks for it", func(t *testing.T) {
		ids := walk(t, &pgkit.Page{Size: 2, Sort: []pgkit.Sort{{Column: "id", Order: pgkit.Desc}}})
		require.Len(t, ids, 5)
		for i := 1; i < len(ids); i++ {
			require.Greater(t, ids[i-1], ids[i], "ids must strictly descend across pages")
		}
	})

	t.Run("round-tripping the returned page continues the walk", func(t *testing.T) {
		page := &pgkit.Page{Size: 2, Sort: []pgkit.Sort{{Column: "id", Order: pgkit.Desc}}}
		var ids []uint64
		for {
			rows, p, err := cur.ListPaged(ctx, where, page)
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

	t.Run("offset table never mints a cursor, even ordered by id", func(t *testing.T) {
		_, p, err := db.Articles.ListPaged(ctx, where, &pgkit.Page{Size: 2, Sort: []pgkit.Sort{{Column: "id", Order: pgkit.Desc}}})
		require.NoError(t, err)
		require.True(t, p.More)
		require.Empty(t, p.NextCursor)
		require.Equal(t, uint32(1), p.Page)
	})

	t.Run("offset table rejects a cursor page", func(t *testing.T) {
		_, first, err := cur.ListPaged(ctx, where, &pgkit.Page{Size: 2})
		require.NoError(t, err)
		_, _, err = db.Articles.ListPaged(ctx, where, &pgkit.Page{Cursor: first.NextCursor})
		require.ErrorIs(t, err, pgkit.ErrPageKindMismatch)
	})

	t.Run("cursor table rejects a page number", func(t *testing.T) {
		_, _, err := cur.ListPaged(ctx, where, &pgkit.Page{Page: 2})
		require.ErrorIs(t, err, pgkit.ErrPageKindMismatch)
	})

	t.Run("a cursor combined with a page number errors", func(t *testing.T) {
		_, first, err := cur.ListPaged(ctx, where, &pgkit.Page{Size: 2})
		require.NoError(t, err)
		_, _, err = cur.ListPaged(ctx, where, &pgkit.Page{Page: 2, Cursor: first.NextCursor})
		require.ErrorIs(t, err, pgkit.ErrCursorPaged)
	})

	t.Run("cursor table rejects a non-id order", func(t *testing.T) {
		_, _, err := cur.ListPaged(ctx, where, &pgkit.Page{Size: 2, Sort: []pgkit.Sort{{Column: "author"}}})
		require.ErrorIs(t, err, pgkit.ErrCursorPageOrdered)
	})

	t.Run("continuation with a conflicting page order errors", func(t *testing.T) {
		_, first, err := cur.ListPaged(ctx, where, &pgkit.Page{Size: 2, Sort: []pgkit.Sort{{Column: "id", Order: pgkit.Desc}}})
		require.NoError(t, err)
		_, _, err = cur.ListPaged(ctx, where, &pgkit.Page{Cursor: first.NextCursor, Sort: []pgkit.Sort{{Column: "id", Order: pgkit.Asc}}})
		require.ErrorIs(t, err, pgkit.ErrCursorPageOrdered)
	})

	t.Run("WithTx keeps Mode", func(t *testing.T) {
		require.NoError(t, pgx.BeginFunc(ctx, DB.Conn, func(pgTx pgx.Tx) error {
			_, p, err := cur.WithTx(pgTx).ListPaged(ctx, where, &pgkit.Page{Size: 2})
			require.NoError(t, err)
			require.True(t, p.More)
			require.NotEmpty(t, p.NextCursor, "keyset mode must survive WithTx")
			return nil
		}))
	})

	t.Run("first page takes direction from an id default sort", func(t *testing.T) {
		ct := db.Articles.WithPaginator(pgkit.WithSort("-id")).WithMode(pgkit.CursorBased)
		rows, p, err := ct.ListPaged(ctx, where, &pgkit.Page{Size: 2})
		require.NoError(t, err)
		require.NotEmpty(t, p.NextCursor)
		for i := 1; i < len(rows); i++ {
			require.Greater(t, rows[i-1].ID, rows[i].ID, "default -id sort must walk descending")
		}
	})

	t.Run("non-id default sort is ignored for keyset", func(t *testing.T) {
		// A default sort like "author" is offset-mode config; keyset must fall
		// back to id ASC instead of erroring on config the caller never sent.
		ct := db.Articles.WithPaginator(pgkit.WithSort("author")).WithMode(pgkit.CursorBased)
		rows, _, err := ct.ListPaged(ctx, where, &pgkit.Page{Size: 2})
		require.NoError(t, err)
		for i := 1; i < len(rows); i++ {
			require.Less(t, rows[i-1].ID, rows[i].ID, "keyset falls back to id ASC")
		}
	})

	t.Run("continuation ignores the table's default sort", func(t *testing.T) {
		// Cursor table with an ASC default sort, walked DESC. A bare {Cursor}
		// continuation must trust the cursor's direction, not inject the default
		// (which would spuriously conflict and return ErrCursorPageOrdered).
		ct := db.Articles.WithPaginator(pgkit.WithSort("id")).WithMode(pgkit.CursorBased)
		_, first, err := ct.ListPaged(ctx, where, &pgkit.Page{Size: 2, Sort: []pgkit.Sort{{Column: "id", Order: pgkit.Desc}}})
		require.NoError(t, err)
		require.NotEmpty(t, first.NextCursor)
		rows, _, err := ct.ListPaged(ctx, where, &pgkit.Page{Size: 2, Cursor: first.NextCursor})
		require.NoError(t, err)
		for i := 1; i < len(rows); i++ {
			require.Greater(t, rows[i-1].ID, rows[i].ID, "continuation must keep descending")
		}
	})

	t.Run("rejects an undecodable cursor", func(t *testing.T) {
		_, _, err := cur.ListPaged(ctx, where, &pgkit.Page{Cursor: "not-a-cursor"})
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
			_, _, err = cur.ListPaged(ctx, where, &pgkit.Page{Cursor: forged})
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
