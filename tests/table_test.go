package pgkit_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
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
			err = tx.Articles.SaveAll(ctx, articles)
			require.NoError(t, err, "SaveAll failed")

			for _, article := range articles {
				require.NotZero(t, article.ID, "ID should be set")
				require.NotZero(t, article.CreatedAt, "CreatedAt should be set")
				require.NotZero(t, article.UpdatedAt, "UpdatedAt should be set")
			}

			firstArticle := articles[0]

			// Save articles (3x update, 1x insert).
			articles = append(articles, &Article{Author: "Fourth", AccountID: account.ID})
			err = tx.Articles.SaveAll(ctx, articles)
			require.NoError(t, err, "SaveAll failed")

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
				//require.Equal(t, article.UpdatedAt, articleCheck.UpdatedAt, "Article UpdatedAt should match")
				//require.NotEqual(t, article.UpdatedAt, articleCheck.UpdatedAt, "Article UpdatedAt shouldn't match") // The .SaveAll() aboe updates the timestamp.
				require.Equal(t, article.DeletedAt, articleCheck.DeletedAt, "Article DeletedAt should match")
			}

			// Verify we can load all articles with .GetByIDs()
			articleIDs := make([]uint64, len(articles))
			for _, article := range articles {
				articleIDs = append(articleIDs, article.ID)
			}
			articlesCheck, err := tx.Articles.GetByIDs(ctx, articleIDs)
			require.NoError(t, err, "GetByIDs failed")
			require.Equal(t, len(articles), len(articlesCheck), "Number of articles should match")
			for i, _ := range articlesCheck {
				require.Equal(t, articles[i].ID, articlesCheck[i].ID, "Article ID should match")
				require.Equal(t, articles[i].Author, articlesCheck[i].Author, "Article Author should match")
				require.Equal(t, articles[i].AccountID, articlesCheck[i].AccountID, "Article AccountID should match")
				require.Equal(t, articles[i].CreatedAt, articlesCheck[i].CreatedAt, "Article CreatedAt should match")
				//require.Equal(t, articles[i].UpdatedAt, articlesCheck[i].UpdatedAt, "Article UpdatedAt should match")
				require.Equal(t, articles[i].DeletedAt, articlesCheck[i].DeletedAt, "Article DeletedAt should match")
			}

			// Soft-delete first article.
			err = tx.Articles.DeleteByID(ctx, firstArticle.ID)
			require.NoError(t, err, "DeleteByID failed")

			// Check if article is soft-deleted.
			article, err := tx.Articles.GetByID(ctx, firstArticle.ID)
			require.NoError(t, err, "GetByID failed")
			require.Equal(t, firstArticle.ID, article.ID, "DeletedAt should be set")
			require.NotNil(t, article.DeletedAt, "DeletedAt should be set")

			// Hard-delete first article.
			err = tx.Articles.HardDeleteByID(ctx, firstArticle.ID)
			require.NoError(t, err, "HardDeleteByID failed")

			// Check if article is hard-deleted.
			article, err = tx.Articles.GetByID(ctx, firstArticle.ID)
			require.Error(t, err, "article was not hard-deleted")
			require.Nil(t, article, "article is not nil")

			return nil
		})
		require.NoError(t, err, "SaveTx transaction failed")
	})
}

func TestLockForUpdate(t *testing.T) {
	truncateAllTables(t)

	ctx := t.Context()
	db := initDB(DB)

	t.Run("TestLockForUpdate", func(t *testing.T) {
		// Create account.
		account := &Account{Name: "LockForUpdate Account"}
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
		err = db.Reviews.SaveAll(ctx, reviews)
		require.NoError(t, err, "create review")

		cond := sq.Eq{
			"status":     ReviewStatusPending,
			"deleted_at": nil,
		}
		orderBy := []string{"created_at ASC"}

		var uniqueIDs [][]uint64 = make([][]uint64, 10)
		var wg sync.WaitGroup

		for range 10 {
			wg.Go(func() {

				err := db.Reviews.LockForUpdate(ctx, cond, orderBy, 10, func(reviews []*Review) {
					for _, review := range reviews {
						review.Status = ReviewStatusProcessing
						go processReviewAsynchronously(ctx, db, review)
					}
				})
				require.NoError(t, err, "lock for update")

			})
		}
		wg.Wait()

		ids := slices.Concat(uniqueIDs...)
		slices.Sort(ids)
		ids = slices.Compact(ids)

		require.Equal(t, 100, len(ids), "number of processed unique reviews should be 100")
	})
}

// TODO: defer() save status (success/failure) or put back to queue for processing.
func processReviewAsynchronously(ctx context.Context, db *Database, review *Review) {
	time.Sleep(1 * time.Second)
	review.Status = ReviewStatusApproved
	db.Reviews.Save(ctx, review)
}
