package migrations

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// released pins the checksums of the migrations that a release includes.
// Installations have already run them, so they must never change: a schema
// change is a new migration. Add each migration here in the release that
// first includes it.
var released = map[string]string{
	"0001_initial.sql": "9796a9732926451bae00ce9a37630ffa21873799ff0d4e991d1350916d9a355b", // v0.1.0
}

func TestReleasedMigrationsAreUnchanged(t *testing.T) {
	t.Parallel()
	for name, want := range released {
		data, err := files.ReadFile(name)
		if err != nil {
			t.Fatalf("released migration %s: %v", name, err)
		}
		sum := sha256.Sum256(data)
		if got := hex.EncodeToString(sum[:]); got != want {
			t.Errorf("released migration %s changed: add a new migration instead", name)
		}
	}
}
