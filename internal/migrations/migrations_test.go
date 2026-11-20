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
// statements that queue up behind it. It is the migration's first
// statement, so that it holds for every lock the migration waits for, and it
// is the 5 seconds that docs/production/deploys.md promises.
func TestMigrationsAfterTheFirstSetALockTimeout(t *testing.T) {
	t.Parallel()
	all, err := All("billing")
	if err != nil {
		t.Fatal(err)
	}

	for _, m := range all[1:] {
		first := ""
		for line := range strings.Lines(m.SQL) {
			if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "--") {
				first = line
				break
			}
		}

		if first != "SET LOCAL lock_timeout = '5s';" {
			t.Errorf("migration %04d_%s starts with %q, want a lock timeout of 5 seconds before any other statement",
				m.Version, m.Name, first)
		}
	}
}

// A migration that changes rows of a table, and then alters the table, asks
// for the table's strongest lock while it holds the rows that it changed. A
// writer that waits for one of those rows holds the table against that lock:
// the two deadlock, and PostgreSQL ends one of them. Between the two
// statements, a writer can also add a row that the alteration refuses. So
// such a migration locks the table before it changes a row, in the mode that
// ALTER TABLE takes. The tests that migrate a schema pass without the lock:
// only a writer at the wrong moment shows that it is missing.
func TestMigrationsLockATableBeforeTheyChangeItsRowsAndAlterIt(t *testing.T) {
	t.Parallel()
	all, err := All("billing")
	if err != nil {
		t.Fatal(err)
	}

	for _, m := range all[1:] {
		locked := make(map[string]bool)  // tables that the migration holds in ACCESS EXCLUSIVE mode
		changed := make(map[string]bool) // tables whose rows it changed without that lock
		for _, words := range statements(m.SQL) {
			switch sql := strings.Join(words, " "); {
			case strings.HasPrefix(sql, "LOCK TABLE ") && strings.HasSuffix(sql, " IN ACCESS EXCLUSIVE MODE"):
				locked[words[2]] = true
			case strings.HasPrefix(sql, "UPDATE "):
				changed[words[1]] = changed[words[1]] || !locked[words[1]]
			case strings.HasPrefix(sql, "DELETE FROM "), strings.HasPrefix(sql, "INSERT INTO "):
				changed[words[2]] = changed[words[2]] || !locked[words[2]]
			case strings.HasPrefix(sql, "ALTER TABLE ") && changed[words[2]]:
				t.Errorf("migration %04d_%s changes rows of %s and then alters it: "+
					"lock the table first, with LOCK TABLE %[3]s IN ACCESS EXCLUSIVE MODE", m.Version, m.Name, words[2])
			}
		}
	}
}

// statements returns the words of each statement of a migration, without
// its comments.
func statements(sql string) [][]string {
	var code strings.Builder
	for line := range strings.Lines(sql) {
		if !strings.HasPrefix(strings.TrimSpace(line), "--") {
			code.WriteString(line)
		}
	}

	var out [][]string
	for stmt := range strings.SplitSeq(code.String(), ";") {
		if words := strings.Fields(stmt); len(words) > 0 {
			out = append(out, words)
		}
	}
	return out
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
