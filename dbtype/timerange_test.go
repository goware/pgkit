package dbtype_test

import (
	"testing"
	"time"

	"github.com/goware/pgkit/v2/dbtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimeRange_UUIDv7_BothBounds(t *testing.T) {
	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 3, 27, 0, 0, 0, 0, time.UTC)
	r := dbtype.UUIDv7Range("id", &since, &until)

	sql, args, err := r.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "id BETWEEN ? AND ?", sql)
	assert.Len(t, args, 2)
}

func TestTimeRange_UUIDv7_SinceOnly(t *testing.T) {
	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r := dbtype.UUIDv7Range("id", &since, nil)

	sql, args, err := r.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "id >= ?", sql)
	assert.Len(t, args, 1)
}

func TestTimeRange_UUIDv7_UntilOnly(t *testing.T) {
	until := time.Date(2026, 3, 27, 0, 0, 0, 0, time.UTC)
	r := dbtype.UUIDv7Range("id", nil, &until)

	sql, args, err := r.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "id <= ?", sql)
	assert.Len(t, args, 1)
}

func TestTimeRange_UUIDv7_NeitherBound(t *testing.T) {
	r := dbtype.UUIDv7Range("id", nil, nil)

	sql, args, err := r.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "1=1", sql)
	assert.Empty(t, args)
}

func TestTimeRange_Int64ID_BothBounds(t *testing.T) {
	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 3, 27, 0, 0, 0, 0, time.UTC)
	r := dbtype.Int64IDRange("id", &since, &until)

	sql, args, err := r.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "id BETWEEN ? AND ?", sql)
	assert.Len(t, args, 2)
}

func TestTimeRange_FloorCeil_Accessors(t *testing.T) {
	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r := dbtype.UUIDv7Range("id", &since, nil)

	_, ok := r.Floor()
	assert.True(t, ok)
	_, ok = r.Ceil()
	assert.False(t, ok)
}

func TestTimeRange_UUIDv7_BoundValues(t *testing.T) {
	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 3, 27, 0, 0, 0, 0, time.UTC)
	r := dbtype.UUIDv7Range("id", &since, &until)

	_, args, err := r.ToSql()
	require.NoError(t, err)

	assert.Equal(t, dbtype.FloorUUIDv7(since).String(), args[0])
	assert.Equal(t, dbtype.CeilUUIDv7(until).String(), args[1])
}

func TestTimeRange_Int64ID_BoundValues(t *testing.T) {
	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 3, 27, 0, 0, 0, 0, time.UTC)
	r := dbtype.Int64IDRange("id", &since, &until)

	_, args, err := r.ToSql()
	require.NoError(t, err)

	assert.Equal(t, dbtype.FloorInt64ID(since), args[0])
	assert.Equal(t, dbtype.CeilInt64ID(until), args[1])
}
