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
	"0001_initial.sql":       "06870e1f69b1b32fbb34f3148c4745bb525340b1b4629b754a6043240109a834", // v0.1.0
	"0002_queue_lag.sql":     "83a3d0a44fb06bcebd9b4bfb7f66ac0cba42c834b511a7487b50be433a8dec22", // v0.2.0
	"0003_halted_groups.sql": "e9a804c9da62620bb3289aed8ddb488af9efcd21b7456272aa26cd8d6bbce179", // v0.3.0
	"0004_stuck_work.sql":    "08ee354a2c82b5e1d947993f2a00ea3c675daa7bebc5b95f6b03fc8192b30e3c", // v0.4.0
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
