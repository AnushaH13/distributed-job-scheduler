// Package idgen creates random identifiers. Go 1.24 has no UUID package in
// the standard library, so version 4 UUIDs are built from crypto/rand.
package idgen

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// UUID returns a random version 4 UUID such as
// "3f2b8c1e-9a4d-4e6f-8b2a-1c3d5e7f9a0b".
func UUID() string {
	var b [16]byte
	// crypto/rand.Read never returns an error on supported platforms (Go 1.24+).
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// New returns prefix + "_" + a UUID, for example "job_3f2b...".
func New(prefix string) string {
	return prefix + "_" + UUID()
}

// Token returns 16 random bytes as hex. Used for claim tokens.
func Token() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
