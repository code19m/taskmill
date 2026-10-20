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
	"0001_initial.sql": "3a0a0d47b40ad87b2787efdddf05dfad252bef78339eff98eb0328fbbf45a19d", // v0.1.0
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
