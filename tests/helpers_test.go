package pgkit_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func truncateAllTables(t *testing.T) {
	truncateTable(t, "accounts")
	truncateTable(t, "reviews")
	truncateTable(t, "logs")
}

func truncateTable(t *testing.T, tableName string) {
	_, err := DB.Exec(context.Background(), fmt.Sprintf(`TRUNCATE TABLE %q CASCADE`, tableName))
	assert.NoError(t, err)
}

func measureCall(fn func() error) (time.Duration, error) {
	t0 := time.Now()
	err := fn()
	return time.Since(t0), err
}

func measureCalls(n int, fn func() error) (time.Duration, error) {
	t0 := time.Now()
	for i := 0; i < n; i++ {
		err := fn()
		if err != nil {
			return time.Since(t0), err
		}
	}
	return time.Since(t0), nil
}
