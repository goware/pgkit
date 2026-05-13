package pgkit_test

import (
	"testing"

	"github.com/goware/pgkit/v2"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubTx satisfies pgx.Tx via interface embedding; method calls would panic,
// but InTx only stores the reference, so the stub is enough to verify wiring.
type stubTx struct{ pgx.Tx }

func TestDBInTx(t *testing.T) {
	t.Run("wires tx into a fresh Querier", func(t *testing.T) {
		db := &pgkit.DB{
			SQL:   &pgkit.StatementBuilder{},
			Query: &pgkit.Querier{},
		}
		tx := &stubTx{}

		inTx := db.InTx(tx)

		require.NotNil(t, inTx)
		assert.Same(t, db.SQL, inTx.SQL, "SQL should be shared with parent")
		assert.NotSame(t, db.Query, inTx.Query, "Query should be a fresh tx-scoped Querier")
		assert.Equal(t, pgx.Tx(tx), inTx.Query.Tx, "Querier.Tx should hold the input tx")
	})
	t.Run("parent Query is untouched", func(t *testing.T) {
		parentQuery := &pgkit.Querier{}
		db := &pgkit.DB{
			SQL:   &pgkit.StatementBuilder{},
			Query: parentQuery,
		}

		_ = db.InTx(&stubTx{})

		assert.Same(t, parentQuery, db.Query, "parent DB.Query reference must not change")
		assert.Nil(t, parentQuery.Tx, "parent Querier.Tx must stay nil")
	})
}
