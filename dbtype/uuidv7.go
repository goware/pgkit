package dbtype

import (
	"encoding/hex"
	"time"
)

// UUIDv7Time extracts the millisecond-precision creation timestamp
// embedded in a UUIDv7 (bytes 0-5, big-endian uint48 ms since epoch).
func UUIDv7Time(u [16]byte) time.Time {
	ms := int64(u[0])<<40 | int64(u[1])<<32 | int64(u[2])<<24 |
		int64(u[3])<<16 | int64(u[4])<<8 | int64(u[5])
	return time.UnixMilli(ms)
}

// FormatUUID formats a [16]byte as a standard UUID string
// (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).
func FormatUUID(u [16]byte) string {
	var buf [36]byte
	hex.Encode(buf[0:8], u[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], u[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], u[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], u[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], u[10:16])
	return string(buf[:])
}

func setUUIDv7Timestamp(u *[16]byte, t time.Time) {
	ms := uint64(t.UnixMilli())
	u[0] = byte(ms >> 40)
	u[1] = byte(ms >> 32)
	u[2] = byte(ms >> 24)
	u[3] = byte(ms >> 16)
	u[4] = byte(ms >> 8)
	u[5] = byte(ms)
}

// FloorUUIDv7 returns the lowest valid UUIDv7 for timestamp t.
// Version and variant bits are set; all random bits are zero.
func FloorUUIDv7(t time.Time) [16]byte {
	var u [16]byte
	setUUIDv7Timestamp(&u, t)
	u[6] = 0x70 // version 7, rand_a = 0
	u[8] = 0x80 // variant 10xxxxxx, rand_b = 0
	return u
}

// CeilUUIDv7 returns the highest valid UUIDv7 for timestamp t.
// Version and variant bits are set; all random bits are one.
func CeilUUIDv7(t time.Time) [16]byte {
	var u [16]byte
	setUUIDv7Timestamp(&u, t)
	u[6] = 0x7f // version 7, rand_a high nibble all 1s
	u[7] = 0xff // rand_a low byte all 1s
	u[8] = 0xbf // variant 10, 6 bits all 1s
	for i := 9; i < 16; i++ {
		u[i] = 0xff
	}
	return u
}
