package migrations

import (
	"strings"
	"testing"
)

func TestAllRendersEveryMigrationInOrder(t *testing.T) {
	t.Parallel()
	all, err := All("billing")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != Latest() || len(all) == 0 {
		t.Fatalf("got %d migrations, Latest says %d", len(all), Latest())
	}
	for i, m := range all {
		if m.Version != i+1 {
			t.Fatalf("migration %d has version %d", i, m.Version)
		}
		if strings.Contains(m.SQL, "__SCHEMA__") {
			t.Fatalf("migration %d still has the schema placeholder", m.Version)
		}
	}
}

// Every migration after the first changes tables that may be busy. A lock
// timeout makes one that waits for a lock fail, instead of blocking the
// statements that queue up behind it (CONTRIBUTING.md).
func TestMigrationsAfterTheFirstSetALockTimeout(t *testing.T) {
	t.Parallel()
	all, err := All("billing")
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range all[1:] {
		if !strings.Contains(m.SQL, "SET LOCAL lock_timeout") {
			t.Errorf("migration %04d_%s sets no lock timeout", m.Version, m.Name)
		}
	}
}

func TestParseName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		file    string
		version int
		name    string
		ok      bool
	}{
		{"0001_initial.sql", 1, "initial", true},
		{"0012_add_index.sql", 12, "add_index", true},
		{"1_initial.sql", 0, "", false},
		{"0001.sql", 0, "", false},
		{"0001_.sql", 0, "", false},
		{"0000_zero.sql", 0, "", false},
		{"abcd_name.sql", 0, "", false},
		{"0001_initial.txt", 0, "", false},
	}
	for _, tt := range tests {
		version, name, ok := parseName(tt.file)
		if version != tt.version || name != tt.name || ok != tt.ok {
			t.Errorf("parseName(%q) = %d, %q, %v; want %d, %q, %v", tt.file, version, name, ok, tt.version, tt.name, tt.ok)
		}
	}
}
