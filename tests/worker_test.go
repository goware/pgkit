package pgkit_test

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
)

type Worker struct {
	DB *Database

	wg sync.WaitGroup
}

func (w *Worker) Wait() {
	w.wg.Wait()
}

func (w *Worker) ProcessReview(ctx context.Context, review *Review) (err error) {
	w.wg.Add(1)
	defer w.wg.Done()

	defer func() {
		// Always update review status to "approved", "rejected" or "failed".
		noCtx := context.Background()
		err = w.DB.Reviews.LockForUpdate(noCtx, sq.Eq{"id": review.ID}, []string{"id DESC"}, func(update *Review) {
			now := time.Now().UTC()
			update.ProcessedAt = &now
			if err != nil {
				update.Status = ReviewStatusFailed
				return
			}
			update.Status = review.Status
		})
		if err != nil {
			log.Printf("failed to save review: %v", err)
		}
	}()

	// Simulate long-running work.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(1 * time.Second):
	}

	// Simulate external API call to an LLM.
	if rand.Intn(2) == 0 {
		return fmt.Errorf("failed to process review: <some underlying error>")
	}

	review.Status = ReviewStatusApproved
	if rand.Intn(2) == 0 {
		review.Status = ReviewStatusRejected
	}
	now := time.Now().UTC()
	review.ProcessedAt = &now
	return nil
}
