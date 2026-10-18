// Package uuid generates version 7 UUIDs (RFC 9562): a millisecond timestamp
// followed by random bits, so IDs are unique and roughly ordered by time.
package uuid

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"time"
)

// NewV7 returns a new version 7 UUID in its canonical text form.
func NewV7() string {
	return newV7(time.Now())
}

func newV7(now time.Time) string {
	var u [16]byte
	_, _ = rand.Read(u[6:]) // never returns an error

	var ms [8]byte
	binary.BigEndian.PutUint64(ms[:], uint64(now.UnixMilli()))
	copy(u[:6], ms[2:])     // the low 48 bits
	u[6] = u[6]&0x0f | 0x70 // version 7
	u[8] = u[8]&0x3f | 0x80 // variant 10

	var b [36]byte
	hex.Encode(b[0:8], u[0:4])
	b[8] = '-'
	hex.Encode(b[9:13], u[4:6])
	b[13] = '-'
	hex.Encode(b[14:18], u[6:8])
	b[18] = '-'
	hex.Encode(b[19:23], u[8:10])
	b[23] = '-'
	hex.Encode(b[24:], u[10:])
	return string(b[:])
}
