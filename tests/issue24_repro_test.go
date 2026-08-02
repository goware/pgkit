package pgkit_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInsertMultiPopulatesRecords_Issue24 covers
// https://github.com/goware/pgkit/issues/24: a batch insert where some records
// set the ,omitempty created_at and some leave it zero.
//
// Two guarantees:
//  1. It must not fail with SQLSTATE 42601 (VALUES lists of differing length) —
//     the row that skips created_at falls back to the column DEFAULT.
//  2. Every caller record must receive its DB-generated id and defaults, the
//     same as a single Insert and as Save. Passing records as individual
//     variadic args (not a spread slice) is the path that did not populate back.
func TestInsertMultiPopulatesRecords_Issue24(t *testing.T) {
	truncateTable(t, "accounts")
	ctx := context.Background()
	db := initDB(DB)

	fixed := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	withTime := &Account{Name: "with-created-at", CreatedAt: fixed}
	withoutTime := &Account{Name: "default-created-at"} // created_at zero -> omitempty -> DEFAULT

	err := db.Accounts.Insert(ctx, withTime, withoutTime)
	require.NoError(t, err, "batch insert with mixed ,omitempty created_at")

	require.NotZero(t, withTime.ID, "id populated on caller record")
	require.NotZero(t, withoutTime.ID, "id populated on caller record")
	assert.True(t, withTime.CreatedAt.Equal(fixed), "explicit created_at preserved")
	assert.False(t, withoutTime.CreatedAt.IsZero(), "skipped created_at filled by DB DEFAULT")

	// The rows actually landed and are addressable by their generated ids.
	got, err := db.Accounts.GetByID(ctx, withoutTime.ID)
	require.NoError(t, err)
	assert.Equal(t, "default-created-at", got.Name)
}
