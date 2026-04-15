package pgkit_test

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/goware/pgkit/v2"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTable(t *testing.T) {
	truncateAllTables(t)

	ctx := t.Context()
	db := initDB(DB)

	t.Run("Simple CRUD", func(t *testing.T) {
		account := &Account{
			Name: "Save Account",
		}

		// Create.
		err := db.Accounts.Save(ctx, account)
		require.NoError(t, err, "Create failed")
		require.NotZero(t, account.ID, "ID should be set")
		require.NotZero(t, account.CreatedAt, "CreatedAt should be set")
		require.NotZero(t, account.UpdatedAt, "UpdatedAt should be set")

		// Check count.
		count, err := db.Accounts.Count(ctx, nil)
		require.NoError(t, err, "FindAll failed")
		require.Equal(t, uint64(1), count, "Expected 1 account")

		// Read from DB & check for equality.
		accountCheck, err := db.Accounts.GetByID(ctx, account.ID)
		require.NoError(t, err, "FindByID failed")
		require.Equal(t, account.ID, accountCheck.ID, "account ID should match")
		require.Equal(t, account.Name, accountCheck.Name, "account name should match")

		// Update.
		account.Name = "Updated account"
		err = db.Accounts.Save(ctx, account)
		require.NoError(t, err, "Save failed")

		// Read from DB & check for equality again.
		accountCheck, err = db.Accounts.GetByID(ctx, account.ID)
		require.NoError(t, err, "FindByID failed")
		require.Equal(t, account.ID, accountCheck.ID, "account ID should match")
		require.Equal(t, account.Name, accountCheck.Name, "account name should match")

		// Check count again.
		count, err = db.Accounts.Count(ctx, nil)
		require.NoError(t, err, "FindAll failed")
		require.Equal(t, uint64(1), count, "Expected 1 account")

		// Iterate all accounts.
		iter, err := db.Accounts.Iter(ctx, nil, nil)
		require.NoError(t, err, "Iter failed")
		var accounts []Account
		for account, err := range iter {
			require.NoError(t, err, "Iter error")
			accounts = append(accounts, *account)
		}
	})

	t.Run("Save external alias updated", func(t *testing.T) {
		t.Parallel()
		account := &Account{Name: "Alias Account"}
		err := db.Accounts.Save(ctx, account)
		require.NoError(t, err)

		// Take aliases BEFORE Save — simulates real-world usage where callers
		// keep a reference to the original pointer expecting it to be mutated.
		slice := []*Article{
			{Author: "AliasFirst", AccountID: account.ID},
			{Author: "AliasSecond", AccountID: account.ID},
		}
		alias0 := slice[0]
		alias1 := slice[1]

		err = db.Articles.Save(ctx, slice...)
		require.NoError(t, err)

		require.NotZero(t, slice[0].ID)
		require.NotZero(t, slice[1].ID)

		// The alias pointer must be the same object as the slice element.
		require.Same(t, slice[0], alias0, "alias0 should still point to the same struct as slice[0]")
		require.Same(t, slice[1], alias1, "alias1 should still point to the same struct as slice[1]")

		// And it must carry the DB-assigned ID.
		require.Equal(t, slice[0].ID, alias0.ID, "alias0.ID should reflect the DB-assigned value")
		require.Equal(t, slice[1].ID, alias1.ID, "alias1.ID should reflect the DB-assigned value")
	})

	t.Run("Save multiple", func(t *testing.T) {
		t.Parallel()
		// Create account.
		account := &Account{Name: "Save Multiple Account"}
		err := db.Accounts.Save(ctx, account)
		require.NoError(t, err, "Create account failed")
		articles := []*Article{
			{Author: "FirstNew", AccountID: account.ID},
			{Author: "SecondNew", AccountID: account.ID},
			{ID: 10001, Author: "FirstOld", AccountID: account.ID},
			{ID: 10002, Author: "SecondOld", AccountID: account.ID},
		}
		err = db.Articles.Save(ctx, articles...)
		require.NoError(t, err, "Save articles")
		require.NotZero(t, articles[0].ID, "ID should be set")
		require.NotZero(t, articles[1].ID, "ID should be set")
		require.Equal(t, uint64(10001), articles[2].ID, "ID should be same")
		require.Equal(t, uint64(10002), articles[3].ID, "ID should be same")
		// test update for multiple records
		updateArticles := []*Article{
			articles[0],
			articles[1],
		}
		updateArticles[0].Author = "Updated Author Name 1"
		updateArticles[1].Author = "Updated Author Name 2"
		err = db.Articles.Save(ctx, updateArticles...)
		require.NoError(t, err, "Save articles")
		updateArticle0, err := db.Articles.GetByID(ctx, articles[0].ID)
		require.NoError(t, err, "Get By ID")
		require.Equal(t, updateArticles[0].Author, updateArticle0.Author, "Author should be same")
		updateArticle1, err := db.Articles.GetByID(ctx, articles[1].ID)
		require.NoError(t, err, "Get By ID")
		require.Equal(t, updateArticles[1].Author, updateArticle1.Author, "Author should be same")
	})

	t.Run("Complex Transaction", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		err := db.BeginTx(ctx, func(tx *Database) error {
			// Create account.
			account := &Account{Name: "Complex Transaction Account"}
			err := tx.Accounts.Save(ctx, account)
			require.NoError(t, err, "Create account failed")

			articles := []*Article{
				{Author: "First", AccountID: account.ID},
				{Author: "Second", AccountID: account.ID},
				{Author: "Third", AccountID: account.ID},
			}

			// Save articles (3x insert).
			err = tx.Articles.Save(ctx, articles...)
			require.NoError(t, err, "Save failed")

			for _, article := range articles {
				require.NotZero(t, article.ID, "ID should be set")
				require.NotZero(t, article.CreatedAt, "CreatedAt should be set")
				require.NotZero(t, article.UpdatedAt, "UpdatedAt should be set")
			}

			firstArticle := articles[0]

			// Save articles (3x update, 1x insert).
			articles = append(articles, &Article{Author: "Fourth", AccountID: account.ID})
			err = tx.Articles.Save(ctx, articles...)
			require.NoError(t, err, "Save failed")

			for _, article := range articles {
				require.NotZero(t, article.ID, "ID should be set")
				require.NotZero(t, article.CreatedAt, "CreatedAt should be set")
				require.NotZero(t, article.UpdatedAt, "UpdatedAt should be set")
			}
			require.Equal(t, firstArticle.ID, articles[0].ID, "First article ID should be the same")

			// Verify we can load all articles with .GetById()
			for _, article := range articles {
				articleCheck, err := tx.Articles.GetByID(ctx, article.ID)
				require.NoError(t, err, "GetByID failed")
				require.Equal(t, article.ID, articleCheck.ID, "Article ID should match")
				require.Equal(t, article.Author, articleCheck.Author, "Article Author should match")
				require.Equal(t, article.AccountID, articleCheck.AccountID, "Article AccountID should match")
				require.Equal(t, article.CreatedAt, articleCheck.CreatedAt, "Article CreatedAt should match")
				// require.Equal(t, article.UpdatedAt, articleCheck.UpdatedAt, "Article UpdatedAt should match")
				// require.NotEqual(t, article.UpdatedAt, articleCheck.UpdatedAt, "Article UpdatedAt shouldn't match") // The .Save() aboe updates the timestamp.
				require.Equal(t, article.DeletedAt, articleCheck.DeletedAt, "Article DeletedAt should match")
			}

			// Verify we can load all articles with .ListByIDs()
			articleIDs := make([]uint64, 0, len(articles))
			for _, article := range articles {
				articleIDs = append(articleIDs, article.ID)
			}
			articlesCheck, err := tx.Articles.ListByIDs(ctx, articleIDs)
			require.NoError(t, err, "ListByIDs failed")
			require.Equal(t, len(articles), len(articlesCheck), "Number of articles should match")
			for i := range articlesCheck {
				require.Equal(t, articles[i].ID, articlesCheck[i].ID, "Article ID should match")
				require.Equal(t, articles[i].Author, articlesCheck[i].Author, "Article Author should match")
				require.Equal(t, articles[i].AccountID, articlesCheck[i].AccountID, "Article AccountID should match")
				require.Equal(t, articles[i].CreatedAt, articlesCheck[i].CreatedAt, "Article CreatedAt should match")
				// require.Equal(t, articles[i].UpdatedAt, articlesCheck[i].UpdatedAt, "Article UpdatedAt should match")
				require.Equal(t, articles[i].DeletedAt, articlesCheck[i].DeletedAt, "Article DeletedAt should match")
			}

			// Soft-delete first article.
			ok, err := tx.Articles.DeleteByID(ctx, firstArticle.ID)
			require.NoError(t, err, "DeleteByID failed")
			require.True(t, ok, "DeleteByID should return true for existing record")

			// Check if article is soft-deleted.
			article, err := tx.Articles.GetByID(ctx, firstArticle.ID)
			require.NoError(t, err, "GetByID failed")
			require.Equal(t, firstArticle.ID, article.ID, "DeletedAt should be set")
			require.NotNil(t, article.DeletedAt, "DeletedAt should be set")

			// Restore first article.
			err = tx.Articles.RestoreByID(ctx, firstArticle.ID)
			require.NoError(t, err, "RestoreByID failed")

			// Check if article is restored.
			article, err = tx.Articles.GetByID(ctx, firstArticle.ID)
			require.NoError(t, err, "GetByID failed after restore")
			require.Nil(t, article.DeletedAt, "DeletedAt should be nil after restore")

			// Soft-delete again for the hard-delete test.
			ok, err = tx.Articles.DeleteByID(ctx, firstArticle.ID)
			require.NoError(t, err, "DeleteByID failed")
			require.True(t, ok, "DeleteByID should return true for existing record")

			// Hard-delete first article.
			ok, err = tx.Articles.HardDeleteByID(ctx, firstArticle.ID)
			require.NoError(t, err, "HardDeleteByID failed")
			require.True(t, ok, "HardDeleteByID should return true for existing record")

			// Check if article is hard-deleted.
			article, err = tx.Articles.GetByID(ctx, firstArticle.ID)
			require.Error(t, err, "article was not hard-deleted")
			require.Nil(t, article, "article is not nil")

			return nil
		})
		require.NoError(t, err, "SaveTx transaction failed")
	})

	t.Run("ListPaged", func(t *testing.T) {
		ctx := t.Context()

		account := &Account{Name: "ListPaged Account"}
		err := db.Accounts.Save(ctx, account)
		require.NoError(t, err)

		// Create 15 articles.
		for i := range 15 {
			err := db.Articles.Save(ctx, &Article{
				AccountID: account.ID,
				Author:    fmt.Sprintf("Author %02d", i),
			})
			require.NoError(t, err)
		}

		// Default paginator (page size 10).
		page := pgkit.NewPage(0, 1)
		results, retPage, err := db.Articles.ListPaged(ctx, sq.Eq{"account_id": account.ID}, page)
		require.NoError(t, err)
		require.Len(t, results, 10)
		require.True(t, retPage.More, "should have more pages")

		// Second page.
		page2 := pgkit.NewPage(0, 2)
		results2, retPage2, err := db.Articles.ListPaged(ctx, sq.Eq{"account_id": account.ID}, page2)
		require.NoError(t, err)
		require.Len(t, results2, 5)
		require.False(t, retPage2.More, "should not have more pages")

		// No overlap between pages.
		for _, r1 := range results {
			for _, r2 := range results2 {
				require.NotEqual(t, r1.ID, r2.ID, "pages should not overlap")
			}
		}
	})

	t.Run("WithPaginator", func(t *testing.T) {
		ctx := t.Context()

		account := &Account{Name: "WithPaginator Account"}
		err := db.Accounts.Save(ctx, account)
		require.NoError(t, err)

		for i := range 10 {
			err := db.Articles.Save(ctx, &Article{
				AccountID: account.ID,
				Author:    fmt.Sprintf("PagAuthor %02d", i),
			})
			require.NoError(t, err)
		}

		// Use a custom paginator with page size 3.
		pagedTable := db.Articles.Table.WithPaginator(pgkit.WithDefaultSize(3), pgkit.WithMaxSize(5))

		page := pgkit.NewPage(0, 1)
		results, retPage, err := pagedTable.ListPaged(ctx, sq.Eq{"account_id": account.ID}, page)
		require.NoError(t, err)
		require.Len(t, results, 3, "should return 3 records with custom paginator")
		require.True(t, retPage.More)

		// Request size larger than max should be capped.
		bigPage := pgkit.NewPage(100, 1)
		results, _, err = pagedTable.ListPaged(ctx, sq.Eq{"account_id": account.ID}, bigPage)
		require.NoError(t, err)
		require.Len(t, results, 5, "should be capped at max size 5")
	})

	t.Run("WithTx preserves Paginator", func(t *testing.T) {
		ctx := t.Context()

		account := &Account{Name: "WithTx Paginator Account"}
		err := db.Accounts.Save(ctx, account)
		require.NoError(t, err)

		for i := range 5 {
			err := db.Articles.Save(ctx, &Article{
				AccountID: account.ID,
				Author:    fmt.Sprintf("TxPag %02d", i),
			})
			require.NoError(t, err)
		}

		pagedTable := db.Articles.Table.WithPaginator(pgkit.WithDefaultSize(2))

		err = pgx.BeginFunc(ctx, db.Conn, func(pgTx pgx.Tx) error {
			txTable := pagedTable.WithTx(pgTx)
			page := pgkit.NewPage(0, 1)
			results, retPage, err := txTable.ListPaged(ctx, sq.Eq{"account_id": account.ID}, page)
			require.NoError(t, err)
			require.Len(t, results, 2, "paginator should be preserved through WithTx")
			require.True(t, retPage.More)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("WithTx keeps IDColumn", func(t *testing.T) {
		ctx := t.Context()

		account := &Account{Name: "WithTx IDColumn Account"}
		err := db.Accounts.Save(ctx, account)
		require.NoError(t, err, "create account failed")

		article := &Article{AccountID: account.ID, Author: "WithTx author"}
		err = db.Articles.Save(ctx, article)
		require.NoError(t, err, "create article failed")

		err = pgx.BeginFunc(ctx, db.Conn, func(pgTx pgx.Tx) error {
			txTable := db.Articles.Table.WithTx(pgTx)
			ok, err := txTable.HardDeleteByID(ctx, article.ID)
			if err != nil {
				return err
			}
			require.True(t, ok, "HardDeleteByID should return true for existing record")

			_, err = txTable.GetByID(ctx, article.ID)
			require.Error(t, err, "article should be deleted inside tx")

			return nil
		})
		require.NoError(t, err, "WithTx HardDeleteByID failed")

		_, err = db.Articles.GetByID(ctx, article.ID)
		require.Error(t, err, "article should be deleted")
	})
}

func TestInsert(t *testing.T) {
	truncateAllTables(t)

	ctx := t.Context()
	db := initDB(DB)

	t.Run("Insert single", func(t *testing.T) {
		account := &Account{Name: "Insert Account"}
		err := db.Accounts.Insert(ctx, account)
		require.NoError(t, err)
		require.NotZero(t, account.ID, "ID should be set after insert")
		require.NotZero(t, account.UpdatedAt, "UpdatedAt should be set")

		// Verify in DB.
		got, err := db.Accounts.GetByID(ctx, account.ID)
		require.NoError(t, err)
		require.Equal(t, account.Name, got.Name)
	})

	t.Run("Insert multiple", func(t *testing.T) {
		account := &Account{Name: "Insert Multiple Account"}
		err := db.Accounts.Insert(ctx, account)
		require.NoError(t, err)

		articles := []*Article{
			{Author: "Author A", AccountID: account.ID},
			{Author: "Author B", AccountID: account.ID},
			{Author: "Author C", AccountID: account.ID},
		}
		err = db.Articles.Insert(ctx, articles...)
		require.NoError(t, err)

		for _, a := range articles {
			require.NotZero(t, a.ID, "ID should be set after bulk insert")
			require.NotZero(t, a.UpdatedAt, "UpdatedAt should be set")
		}

		// Verify all in DB.
		for _, a := range articles {
			got, err := db.Articles.GetByID(ctx, a.ID)
			require.NoError(t, err)
			require.Equal(t, a.Author, got.Author)
		}
	})

	t.Run("Insert nil record", func(t *testing.T) {
		err := db.Accounts.Insert(ctx, nil)
		require.Error(t, err)
	})

	t.Run("Insert invalid record", func(t *testing.T) {
		err := db.Accounts.Insert(ctx, &Account{Name: ""})
		require.Error(t, err, "should fail validation")
	})

	t.Run("Insert zero records", func(t *testing.T) {
		err := db.Accounts.Insert(ctx)
		require.NoError(t, err, "inserting zero records should be a no-op")
	})
}

func TestUpdate(t *testing.T) {
	truncateAllTables(t)

	ctx := t.Context()
	db := initDB(DB)

	t.Run("Update single", func(t *testing.T) {
		account := &Account{Name: "Before Update"}
		err := db.Accounts.Insert(ctx, account)
		require.NoError(t, err)

		account.Name = "After Update"
		ok, err := db.Accounts.Update(ctx, account)
		require.NoError(t, err)
		require.True(t, ok, "should return true for existing record")

		got, err := db.Accounts.GetByID(ctx, account.ID)
		require.NoError(t, err)
		require.Equal(t, "After Update", got.Name)
	})

	t.Run("Update multiple", func(t *testing.T) {
		account := &Account{Name: "Update Multiple Account"}
		err := db.Accounts.Insert(ctx, account)
		require.NoError(t, err)

		articles := []*Article{
			{Author: "Original A", AccountID: account.ID},
			{Author: "Original B", AccountID: account.ID},
		}
		err = db.Articles.Insert(ctx, articles...)
		require.NoError(t, err)

		articles[0].Author = "Updated A"
		articles[1].Author = "Updated B"
		ok, err := db.Articles.Update(ctx, articles...)
		require.NoError(t, err)
		require.True(t, ok, "should return true when records exist")

		for _, a := range articles {
			got, err := db.Articles.GetByID(ctx, a.ID)
			require.NoError(t, err)
			require.Equal(t, a.Author, got.Author)
		}
	})

	t.Run("Update with zero ID fails", func(t *testing.T) {
		_, err := db.Accounts.Update(ctx, &Account{Name: "No ID"})
		require.Error(t, err, "should fail with zero ID")
	})

	t.Run("Update multiple with zero ID fails", func(t *testing.T) {
		account := &Account{Name: "Update Zero ID Account"}
		err := db.Accounts.Insert(ctx, account)
		require.NoError(t, err)

		_, err = db.Accounts.Update(ctx, account, &Account{Name: "No ID"})
		require.Error(t, err, "should fail when any record has zero ID")
	})

	t.Run("Update nil record", func(t *testing.T) {
		_, err := db.Accounts.Update(ctx, nil)
		require.Error(t, err)
	})

	t.Run("Update invalid record", func(t *testing.T) {
		account := &Account{Name: "Valid"}
		err := db.Accounts.Insert(ctx, account)
		require.NoError(t, err)

		account.Name = ""
		_, err = db.Accounts.Update(ctx, account)
		require.Error(t, err, "should fail validation")
	})

	t.Run("Update zero records", func(t *testing.T) {
		ok, err := db.Accounts.Update(ctx)
		require.NoError(t, err, "updating zero records should be a no-op")
		require.False(t, ok, "should return false for zero records")
	})

	t.Run("Update non-existent record returns false", func(t *testing.T) {
		account := &Account{ID: 999999, Name: "Ghost"}
		ok, err := db.Accounts.Update(ctx, account)
		require.NoError(t, err)
		require.False(t, ok, "should return false for non-existent record")
	})
}

func TestDeleteByID(t *testing.T) {
	truncateAllTables(t)

	ctx := t.Context()
	db := initDB(DB)

	account := &Account{Name: "DeleteByID Account"}
	err := db.Accounts.Insert(ctx, account)
	require.NoError(t, err)

	t.Run("soft delete existing returns true", func(t *testing.T) {
		article := &Article{Author: "Author", AccountID: account.ID}
		err := db.Articles.Insert(ctx, article)
		require.NoError(t, err)

		ok, err := db.Articles.DeleteByID(ctx, article.ID)
		require.NoError(t, err)
		require.True(t, ok)

		got, err := db.Articles.GetByID(ctx, article.ID)
		require.NoError(t, err)
		require.NotNil(t, got.DeletedAt)
	})

	t.Run("soft delete missing returns false", func(t *testing.T) {
		ok, err := db.Articles.DeleteByID(ctx, 999999)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("soft delete already-deleted returns false", func(t *testing.T) {
		article := &Article{Author: "Double Delete", AccountID: account.ID}
		err := db.Articles.Insert(ctx, article)
		require.NoError(t, err)

		ok, err := db.Articles.DeleteByID(ctx, article.ID)
		require.NoError(t, err)
		require.True(t, ok)

		// Hard-delete so GetByID returns ErrNoRows.
		_, err = db.Articles.HardDeleteByID(ctx, article.ID)
		require.NoError(t, err)

		ok, err = db.Articles.DeleteByID(ctx, article.ID)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("hard delete existing returns true", func(t *testing.T) {
		// Account has no SetDeletedAt — DeleteByID falls through to hard delete.
		acct := &Account{Name: "HardDelete via DeleteByID"}
		err := db.Accounts.Insert(ctx, acct)
		require.NoError(t, err)

		ok, err := db.Accounts.DeleteByID(ctx, acct.ID)
		require.NoError(t, err)
		require.True(t, ok)

		_, err = db.Accounts.GetByID(ctx, acct.ID)
		require.Error(t, err)
	})

	t.Run("hard delete missing returns false", func(t *testing.T) {
		ok, err := db.Accounts.DeleteByID(ctx, 999999)
		require.NoError(t, err)
		require.False(t, ok)
	})
}

func TestHardDeleteByID(t *testing.T) {
	truncateAllTables(t)

	ctx := t.Context()
	db := initDB(DB)

	t.Run("existing record returns true", func(t *testing.T) {
		account := &Account{Name: "HardDelete Found"}
		err := db.Accounts.Insert(ctx, account)
		require.NoError(t, err)

		ok, err := db.Accounts.HardDeleteByID(ctx, account.ID)
		require.NoError(t, err)
		require.True(t, ok)

		_, err = db.Accounts.GetByID(ctx, account.ID)
		require.Error(t, err)
	})

	t.Run("missing record returns false", func(t *testing.T) {
		ok, err := db.Accounts.HardDeleteByID(ctx, 999999)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("hard delete on soft-deletable record", func(t *testing.T) {
		account := &Account{Name: "HardDelete Soft-Deletable"}
		err := db.Accounts.Insert(ctx, account)
		require.NoError(t, err)

		article := &Article{Author: "Soft-Deletable", AccountID: account.ID}
		err = db.Articles.Insert(ctx, article)
		require.NoError(t, err)

		// HardDeleteByID bypasses soft delete even on soft-deletable records.
		ok, err := db.Articles.HardDeleteByID(ctx, article.ID)
		require.NoError(t, err)
		require.True(t, ok)

		_, err = db.Articles.GetByID(ctx, article.ID)
		require.Error(t, err, "should be permanently deleted")
	})

	t.Run("double hard delete returns false on second call", func(t *testing.T) {
		account := &Account{Name: "Double HardDelete"}
		err := db.Accounts.Insert(ctx, account)
		require.NoError(t, err)

		ok, err := db.Accounts.HardDeleteByID(ctx, account.ID)
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = db.Accounts.HardDeleteByID(ctx, account.ID)
		require.NoError(t, err)
		require.False(t, ok)
	})
}

func TestIter(t *testing.T) {
	truncateAllTables(t)

	ctx := t.Context()
	db := initDB(DB)

	account := &Account{Name: "Iter Account"}
	err := db.Accounts.Insert(ctx, account)
	require.NoError(t, err)

	const total = 100
	for i := range total {
		err := db.Articles.Insert(ctx, &Article{AccountID: account.ID, Author: fmt.Sprintf("Author %03d", i)})
		require.NoError(t, err)
	}

	iter, err := db.Articles.Iter(ctx, sq.Eq{"account_id": account.ID}, []string{"id"})
	require.NoError(t, err)

	var count int
	for article, err := range iter {
		require.NoError(t, err)
		require.NotNil(t, article)
		count++
	}
	require.Equal(t, total, count, "Iter should yield all rows")
}

func TestLockForUpdates(t *testing.T) {
	truncateAllTables(t)

	ctx := t.Context()
	db := initDB(DB)
	worker := &Worker{DB: db}

	t.Run("TestLockForUpdates", func(t *testing.T) {
		// Create account.
		account := &Account{Name: "LockForUpdates Account"}
		err := db.Accounts.Save(ctx, account)
		require.NoError(t, err, "Create account failed")

		// Create article.
		article := &Article{AccountID: account.ID, Author: "Author", Content: Content{Title: "Title", Body: "Body"}}
		err = db.Articles.Save(ctx, article)
		require.NoError(t, err, "Create article failed")

		// Create 1000 reviews.
		reviews := make([]*Review, 100)
		for i := range 100 {
			reviews[i] = &Review{
				Comment:   fmt.Sprintf("Test comment %d", i),
				AccountID: account.ID,
				ArticleID: article.ID,
				Status:    ReviewStatusPending,
			}
		}
		err = db.Reviews.Save(ctx, reviews...)
		require.NoError(t, err, "create review")

		var mu sync.Mutex
		var allIDs []uint64
		var wg sync.WaitGroup

		for range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()

				reviews, err := db.Reviews.DequeueForProcessing(ctx, 10)
				assert.NoError(t, err, "dequeue reviews")

				var localIDs []uint64
				for _, review := range reviews {
					localIDs = append(localIDs, review.ID)
					worker.wg.Add(1)
					go worker.ProcessReview(ctx, review)
				}

				mu.Lock()
				allIDs = append(allIDs, localIDs...)
				mu.Unlock()
			}()
		}
		wg.Wait()

		// Ensure that all reviews were picked up for processing exactly once.
		uniqueIDs := slices.Clone(allIDs)
		slices.Sort(uniqueIDs)
		uniqueIDs = slices.Compact(uniqueIDs)
		require.Equal(t, 100, len(uniqueIDs), "number of unique reviews picked up for processing should be 100")

		// Wait for all reviews to be processed asynchronously.
		worker.Wait()

		// Double check there's no reviews stuck in "processing" status.
		count, err := db.Reviews.Count(ctx, sq.Eq{"status": ReviewStatusProcessing})
		require.NoError(t, err, "count reviews")
		require.Zero(t, count, "there should be no reviews stuck in 'processing' status")
	})
}
