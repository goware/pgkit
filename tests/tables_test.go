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

func (t *reviewsTable) DequeueForProcessing(ctx context.Context, limit uint64) ([]*Review, error) {
	where := sq.Eq{
		"status":     ReviewStatusPending,
		"deleted_at": nil,
	}
	orderBy := []string{
		"created_at ASC",
	}

	now := time.Now().UTC()
	dequeued, err := t.ClaimForUpdate(ctx, where, orderBy, limit, func(review *Review) error {
		review.Status = ReviewStatusProcessing
		review.ProcessedAt = &now
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("claim for update: %w", err)
	}

	return dequeued, nil
}
