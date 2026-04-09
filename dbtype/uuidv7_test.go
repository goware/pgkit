package dbtype_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goware/pgkit/v2/dbtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFloorUUIDv7_VersionAndVariant(t *testing.T) {
	u := dbtype.FloorUUIDv7(time.Now())
	assert.Equal(t, uuid.Version(7), u.Version())
	assert.Equal(t, uuid.RFC4122, u.Variant())
}

func TestCeilUUIDv7_VersionAndVariant(t *testing.T) {
	u := dbtype.CeilUUIDv7(time.Now())
	assert.Equal(t, uuid.Version(7), u.Version())
	assert.Equal(t, uuid.RFC4122, u.Variant())
}

func TestFloorUUIDv7_LessThanNewV7(t *testing.T) {
	now := time.Now()
	floor := dbtype.FloorUUIDv7(now)
	for range 100 {
		v7, err := uuid.NewV7()
		require.NoError(t, err)
		assert.LessOrEqual(t, floor.String(), v7.String())
	}
}

func TestCeilUUIDv7_GreaterThanFloor(t *testing.T) {
	now := time.Now()
	floor := dbtype.FloorUUIDv7(now)
	ceil := dbtype.CeilUUIDv7(now)
	assert.Greater(t, ceil.String(), floor.String())
}

func TestFloorCeilUUIDv7_BracketRealID(t *testing.T) {
	v7, err := uuid.NewV7()
	require.NoError(t, err)
	ts := dbtype.UUIDv7Time(v7)

	floor := dbtype.FloorUUIDv7(ts)
	ceil := dbtype.CeilUUIDv7(ts)
	assert.LessOrEqual(t, floor.String(), v7.String())
	assert.GreaterOrEqual(t, ceil.String(), v7.String())
}

func TestFloorUUIDv7_TimestampRoundTrip(t *testing.T) {
	ts := time.Date(2026, 3, 27, 12, 0, 0, 0, time.UTC)
	floor := dbtype.FloorUUIDv7(ts)
	got := dbtype.UUIDv7Time(floor)
	assert.Equal(t, ts.UnixMilli(), got.UnixMilli())
}

func TestUUIDv7Time_ExtractsTimestamp(t *testing.T) {
	v7, err := uuid.NewV7()
	require.NoError(t, err)
	got := dbtype.UUIDv7Time(v7)
	assert.False(t, got.IsZero())
	assert.WithinDuration(t, time.Now(), got, time.Second)
}
