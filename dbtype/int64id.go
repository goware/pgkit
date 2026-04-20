package dbtype

import "time"

// Int64IDBits is the number of random bits in a typeid Int64.
// Bit layout: [48-bit unix ms timestamp][15-bit crypto/rand] = 63 bits, always positive.
const Int64IDBits = 15

// Int64IDTime extracts the millisecond-precision creation timestamp
// from an Int64 ID that uses the typeid bit layout.
func Int64IDTime(id int64) time.Time {
	return time.UnixMilli(id >> Int64IDBits)
}

// FloorInt64ID returns the lowest possible Int64 ID for timestamp t.
// All 15 random bits are zero.
func FloorInt64ID(t time.Time) int64 {
	return t.UnixMilli() << Int64IDBits
}

// CeilInt64ID returns the highest possible Int64 ID for timestamp t.
// All 15 random bits are one.
func CeilInt64ID(t time.Time) int64 {
	return t.UnixMilli()<<Int64IDBits | 0x7FFF
}
