package migrations

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"os"
	"strings"
	"testing"
)

// released pins the checksums of the migrations that a release includes.
// Installations have already run them, so they must never change: a schema
// change is a new migration. Add each migration here in the release that
// first includes it.
var released = map[string]string{
	"0001_initial.sql":                      "06870e1f69b1b32fbb34f3148c4745bb525340b1b4629b754a6043240109a834", // v0.1.0
	"0002_queue_lag.sql":                    "83a3d0a44fb06bcebd9b4bfb7f66ac0cba42c834b511a7487b50be433a8dec22", // v0.2.0
	"0003_halted_groups.sql":                "e9a804c9da62620bb3289aed8ddb488af9efcd21b7456272aa26cd8d6bbce179", // v0.3.0
	"0004_stuck_work.sql":                   "adef98ac74985a723a654a3578835384cb7c797c9dfe36d27ca5aba227116864", // v0.4.0
	"0005_worker_leases_and_freed_keys.sql": "59aa9494bab43407bbb56f9d2781bd6b9af701cb689544b616c0beecb40be076", // v0.7.0
	"0006_refused_rows.sql":                 "22ab57c76172bf16de16d23e28e8622985ba950899161acdc807efa03cbeb710", // v0.8.0
	"0007_created_order.sql":                "2eb5a0f91341c9a7aaac2e0ed5966a5f311001a0cc50b468f0b78a10ff83748e", // v0.12.0
	"0008_worker_schedules.sql":             "e1c81a1e52f0e226d9307a1140335fe18c8d8f7c39985f2d8a5971622d72b795", // v0.13.0
	"0009_group_heads.sql":                  "97555a623fade20e9d5f39753da12b6422f2d1d95e6fc4e01eee9020983e6b6e", // v0.14.0
	"0010_activity_writes.sql":              "034888796668da6e8196ce5d6f5c575156dad3d58f7dbf7c61905d01b7cbb5cb", // v0.18.1
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

// TestReleasePinsEveryMigration checks that a release pins all its
// migrations. A release moves the entries under [Unreleased] in CHANGELOG.md
// to a section of its own, so an empty [Unreleased] means that a release is
// being prepared, or was just made: then every migration must be pinned.
// Between releases, [Unreleased] lists the changes, and a new migration may
// still change until its release.
func TestReleasePinsEveryMigration(t *testing.T) {
	t.Parallel()

	changelog, err := os.ReadFile("../../CHANGELOG.md")
	if err != nil {
		t.Fatal(err)
	}
	if !unreleasedIsEmpty(string(changelog)) {
		t.Skip("CHANGELOG.md lists unreleased changes: a release pins its migrations")
	}

	names, err := fs.Glob(files, "*.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if _, ok := released[name]; !ok {
			t.Errorf("migration %s is not pinned, and CHANGELOG.md has no unreleased changes: "+
				"add its checksum to released, with the release that first includes it", name)
		}
	}
}

// unreleasedIsEmpty reports whether the [Unreleased] section of a changelog
// holds nothing.
func unreleasedIsEmpty(changelog string) bool {
	_, section, found := strings.Cut(changelog, "## [Unreleased]")
	if !found {
		return false
	}
	section, _, _ = strings.Cut(section, "\n## [")
	return strings.TrimSpace(section) == ""
}
