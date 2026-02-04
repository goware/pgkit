package pgkit_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func truncateTable(t *testing.T, tableName string) {
	_, err := DB.Conn.Exec(context.Background(), fmt.Sprintf(`TRUNCATE TABLE %q CASCADE`, tableName))
	assert.NoError(t, err)
}
