package dbtype_test

import (
	"crypto/rand"
	"encoding/binary"
	"testing"
	"time"

	"github.com/goware/pgkit/v2/dbtype"
	"github.com/stretchr/testify/assert"
)

// newTestInt64ID generates an Int64 ID matching typeid's bit layout for testing.
func newTestInt64ID() int64 {
	ms := time.Now().UnixMilli()
	var rb [2]byte
	_, _ = rand.Read(rb[:])
	r := int64(binary.BigEndian.Uint16(rb[:]) & 0x7FFF)
	return ms<<dbtype.Int64IDBits | r
}

func TestFloorInt64ID_LessThanNew(t *testing.T) {
	now := time.Now()
	floor := dbtype.FloorInt64ID(now)
	for range 100 {
		id := newTestInt64ID()
		assert.LessOrEqual(t, floor, id)
	}
}

func TestCeilInt64ID_GreaterThanFloor(t *testing.T) {
	now := time.Now()
	floor := dbtype.FloorInt64ID(now)
	ceil := dbtype.CeilInt64ID(now)
	assert.Greater(t, ceil, floor)
}

func TestFloorCeilInt64ID_BracketRealID(t *testing.T) {
	id := newTestInt64ID()
	ts := dbtype.Int64IDTime(id)

	floor := dbtype.FloorInt64ID(ts)
	ceil := dbtype.CeilInt64ID(ts)
	assert.LessOrEqual(t, floor, id)
	assert.GreaterOrEqual(t, ceil, id)
}

func TestFloorInt64ID_TimestampRoundTrip(t *testing.T) {
	ts := time.Date(2026, 3, 27, 12, 0, 0, 0, time.UTC)
	floor := dbtype.FloorInt64ID(ts)
	got := dbtype.Int64IDTime(floor)
	assert.Equal(t, ts.UnixMilli(), got.UnixMilli())
}

func TestInt64IDTime_ExtractsTimestamp(t *testing.T) {
	id := newTestInt64ID()
	got := dbtype.Int64IDTime(id)
	assert.False(t, got.IsZero())
	assert.WithinDuration(t, time.Now(), got, time.Second)
}
