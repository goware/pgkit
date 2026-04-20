package dbtype_test

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/goware/pgkit/v2/dbtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestUUIDv7 generates a valid UUIDv7 for testing without external deps.
func newTestUUIDv7() [16]byte {
	var u [16]byte
	ms := uint64(time.Now().UnixMilli())
	u[0] = byte(ms >> 40)
	u[1] = byte(ms >> 32)
	u[2] = byte(ms >> 24)
	u[3] = byte(ms >> 16)
	u[4] = byte(ms >> 8)
	u[5] = byte(ms)
	_, _ = rand.Read(u[6:])
	u[6] = (u[6] & 0x0f) | 0x70 // version 7
	u[8] = (u[8] & 0x3f) | 0x80 // variant RFC4122
	return u
}

func TestFloorUUIDv7_VersionAndVariant(t *testing.T) {
	u := dbtype.FloorUUIDv7(time.Now())
	assert.Equal(t, byte(0x70), u[6]&0xf0, "version nibble")
	assert.Equal(t, byte(0x80), u[8]&0xc0, "variant bits")
}

func TestCeilUUIDv7_VersionAndVariant(t *testing.T) {
	u := dbtype.CeilUUIDv7(time.Now())
	assert.Equal(t, byte(0x70), u[6]&0xf0, "version nibble")
	assert.Equal(t, byte(0x80), u[8]&0xc0, "variant bits")
}

func TestFloorUUIDv7_LessThanNewV7(t *testing.T) {
	now := time.Now()
	floor := dbtype.FormatUUID(dbtype.FloorUUIDv7(now))
	for range 100 {
		v7 := newTestUUIDv7()
		assert.LessOrEqual(t, floor, dbtype.FormatUUID(v7))
	}
}

func TestCeilUUIDv7_GreaterThanFloor(t *testing.T) {
	now := time.Now()
	floor := dbtype.FormatUUID(dbtype.FloorUUIDv7(now))
	ceil := dbtype.FormatUUID(dbtype.CeilUUIDv7(now))
	assert.Greater(t, ceil, floor)
}

func TestFloorCeilUUIDv7_BracketRealID(t *testing.T) {
	v7 := newTestUUIDv7()
	ts := dbtype.UUIDv7Time(v7)

	floor := dbtype.FormatUUID(dbtype.FloorUUIDv7(ts))
	ceil := dbtype.FormatUUID(dbtype.CeilUUIDv7(ts))
	id := dbtype.FormatUUID(v7)
	assert.LessOrEqual(t, floor, id)
	assert.GreaterOrEqual(t, ceil, id)
}

func TestFloorUUIDv7_TimestampRoundTrip(t *testing.T) {
	ts := time.Date(2026, 3, 27, 12, 0, 0, 0, time.UTC)
	floor := dbtype.FloorUUIDv7(ts)
	got := dbtype.UUIDv7Time(floor)
	assert.Equal(t, ts.UnixMilli(), got.UnixMilli())
}

func TestUUIDv7Time_ExtractsTimestamp(t *testing.T) {
	v7 := newTestUUIDv7()
	got := dbtype.UUIDv7Time(v7)
	require.False(t, got.IsZero())
	assert.WithinDuration(t, time.Now(), got, time.Second)
}
