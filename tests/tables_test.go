package pgkit_test

import (
	"context"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/goware/pgkit/v2"
)

type accountsTable struct {
	*pgkit.Table[Account, *Account, int64]
}

type articlesTable struct {
	*pgkit.Table[Article, *Article, uint64]
}

type reviewsTable struct {
	*pgkit.Table[Review, *Review, uint64]
}

func (w *reviewsTable) DequeueForProcessing(ctx context.Context, limit uint64) ([]*Review, error) {
	var dequeued []*Review
	where := sq.Eq{
		"status":     ReviewStatusPending,
		"deleted_at": nil,
	}
	orderBy := []string{
		"created_at ASC",
	}

	err := w.LockForUpdates(ctx, where, orderBy, limit, func(reviews []*Review) {
		now := time.Now().UTC()
		for _, review := range reviews {
			review.Status = ReviewStatusProcessing
			review.ProcessedAt = &now
		}
		dequeued = reviews
	})
	if err != nil {
		return nil, fmt.Errorf("lock for updates: %w", err)
	}

	return dequeued, nil
}
