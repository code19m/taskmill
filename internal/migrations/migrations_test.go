package migrations

import (
	"regexp"
	"slices"
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
// is the 5 seconds that docs/production/deploys.md promises. An online step
// runs outside a transaction, where SET LOCAL does nothing, and blocks no
// writes while it waits.
func TestMigrationsAfterTheFirstSetALockTimeout(t *testing.T) {
	t.Parallel()
	all, err := All("billing")
	if err != nil {
		t.Fatal(err)
	}

	for _, m := range all[1:] {
		if m.Online {
			continue
		}
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
		for _, stmt := range statements(m.SQL) {
			words := strings.Fields(stmt)
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

// lastBlocking is the last migration that may build an index that blocks
// writes to a table that it did not create. Teams that upgrade from a
// release before it still run those: MigrateConfig.MaxBlockingIndexRows
// guards them.
const lastBlocking = 9

// An index that blocks writes while it reads a busy table makes enqueues and
// lease renewals wait, about half a second for each million rows, and a
// renewal that waits longer than a lease loses its task's attempt. So every
// migration after lastBlocking builds its indexes in an online step.
func TestNewMigrationsBuildIndexesOnline(t *testing.T) {
	t.Parallel()
	all, err := All("billing")
	if err != nil {
		t.Fatal(err)
	}

	for _, m := range all[lastBlocking:] {
		for _, stmt := range mayBlock(m) {
			t.Errorf("migration %04d_%s may build or rebuild an index that blocks writes: %q. "+
				"Build it with CREATE INDEX CONCURRENTLY in an online step, a migration whose first line is %q (see CONTRIBUTING.md)",
				m.Version, m.Name, stmt, onlineMarker)
		}
	}
}

// indexWords are the words of a statement that may build an index, or
// rebuild one.
var indexWords = regexp.MustCompile(`(?i)\b(INDEX|UNIQUE|PRIMARY|EXCLUDE|REINDEX)\b`)

// alterTable reads the table of an ALTER TABLE statement.
var alterTable = regexp.MustCompile(`(?i)^ALTER TABLE (?:IF EXISTS )?(?:ONLY )?([a-z0-9_]+\.[a-z0-9_]+)\b`)

// mayBlock returns the statements of m that may build or rebuild an index
// with a lock that blocks writes, unless m is online. It fails closed: a
// statement that names an index or a key may, unless it uses an index that
// exists, comments, or works on a table that m itself creates, which is
// empty. A table created IF NOT EXISTS may hold rows.
func mayBlock(m Migration) []string {
	if m.Online {
		return nil
	}

	var out []string
	created := make(map[string]bool)
	for _, stmt := range statements(m.SQL) {
		words := wordsOf(stmt)
		table := ""
		if match := createIndex.FindStringSubmatch(words); match != nil {
			table = match[1]
		} else if match := alterTable.FindStringSubmatch(words); match != nil {
			table = match[1]
		}

		switch upper := strings.ToUpper(words); {
		case strings.HasPrefix(upper, "CREATE TABLE "):
			if match := createTable.FindStringSubmatch(words); match != nil {
				created[match[1]] = true
			}
		case !indexWords.MatchString(words), usingIndex.MatchString(words), strings.HasPrefix(upper, "COMMENT ON "):
		case table != "" && created[table]:
		default:
			out = append(out, words)
		}
	}
	return out
}

// The rule of TestNewMigrationsBuildIndexesOnline catches every statement
// that builds an index with a lock that blocks writes, also through a key, a
// DO block, or a table created IF NOT EXISTS, and lets the others pass,
// whatever their comments and quoted text say.
func TestTheOnlineRuleFailsClosed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		body    string
		blocked int // how many statements the rule refuses
	}{
		{"REINDEX TABLE __SCHEMA__.tasks", 1},
		{"REINDEX INDEX __SCHEMA__.tasks_fetch_idx", 1},
		{"CREATE INDEX tasks_kind_idx ON __SCHEMA__.tasks (kind)", 1},
		{"DROP INDEX __SCHEMA__.tasks_fetch_idx", 1},
		{"ALTER TABLE __SCHEMA__.tasks ADD COLUMN x integer UNIQUE", 1},
		{"ALTER TABLE __SCHEMA__.tasks ADD COLUMN y integer PRIMARY KEY", 1},
		{"ALTER TABLE __SCHEMA__.workers ADD CONSTRAINT workers_excl EXCLUDE USING gist (id WITH =)", 1},
		{"DO $$ BEGIN CREATE INDEX tasks_kind_idx ON __SCHEMA__.tasks (kind); END $$", 1},
		{"CREATE TABLE IF NOT EXISTS __SCHEMA__.t (a integer PRIMARY KEY); CREATE INDEX t_a_idx ON __SCHEMA__.t (a)", 1},
		{"CREATE TABLE __SCHEMA__.t (a integer PRIMARY KEY); CREATE UNIQUE INDEX t_a_idx ON __SCHEMA__.t (a);" +
			"ALTER TABLE __SCHEMA__.t ADD COLUMN b integer UNIQUE", 0},
		{"ALTER TABLE __SCHEMA__.tasks ADD CONSTRAINT tasks_k_key UNIQUE USING INDEX tasks_k_idx", 0},
		{"COMMENT ON COLUMN __SCHEMA__.tasks.kind IS 'an INDEX, built concurrently; a PRIMARY key'", 0},
		{"-- CREATE INDEX tasks_kind_idx ON __SCHEMA__.tasks (kind);\nUPDATE __SCHEMA__.queues /* REINDEX */ SET name = 'unique'", 0},
	}

	for _, tt := range tests {
		m, err := Parse("0010_test.sql", "SET LOCAL lock_timeout = '5s';\n"+tt.body, "billing")
		if err != nil {
			t.Errorf("%q: %v", tt.body, err)
			continue
		}
		if got := mayBlock(m); len(got) != tt.blocked {
			t.Errorf("%q: the rule refuses %q, want %d statements", tt.body, got, tt.blocked)
		}
	}
}

func TestParse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		body     string
		blocking []string
		online   []Statement
		err      string // a part of the error, if Parse fails
	}{
		{
			name:     "an index on a table that the migration does not create blocks",
			body:     "SET LOCAL lock_timeout = '5s';\nCREATE INDEX t_a_idx ON __SCHEMA__.t (a);\nCREATE UNIQUE INDEX IF NOT EXISTS ON ONLY __SCHEMA__.h (b);",
			blocking: []string{"s.t", "s.h"},
		},
		{
			name:     "a unique or primary key constraint blocks, unless it uses an index",
			body:     "ALTER TABLE __SCHEMA__.t ADD CONSTRAINT t_a_key UNIQUE (a);\nALTER TABLE ONLY __SCHEMA__.u ADD PRIMARY KEY (id);\nALTER TABLE __SCHEMA__.v ADD CONSTRAINT v_key UNIQUE USING INDEX v_idx;",
			blocking: []string{"s.t", "s.u"},
		},
		{
			name: "an index on a table that the migration creates does not block",
			body: "CREATE TABLE __SCHEMA__.t (a int);\nCREATE INDEX t_a_idx ON __SCHEMA__.t (a);",
		},
		{
			name: "comments are no statements",
			body: "-- CREATE INDEX t_a_idx ON __SCHEMA__.t (a);\nSELECT 1;",
		},
		{
			name: "an online step builds and drops indexes",
			body: "-- online\n-- Why.\nCREATE INDEX CONCURRENTLY t_a_idx ON __SCHEMA__.t (a)\n    WHERE a > 0;\ndrop index concurrently if exists __SCHEMA__.t_old_idx;",
			online: []Statement{
				{SQL: "CREATE INDEX CONCURRENTLY t_a_idx ON s.t (a)\n    WHERE a > 0", Index: "s.t_a_idx", Table: "s.t"},
				{SQL: "drop index concurrently if exists s.t_old_idx", Index: "s.t_old_idx"},
			},
		},
		{
			name: "an online step holds nothing else",
			body: "-- online\nCREATE INDEX CONCURRENTLY t_a_idx ON __SCHEMA__.t (a);\nUPDATE __SCHEMA__.t SET a = 1;",
			err:  "must be CREATE INDEX CONCURRENTLY",
		},
		{
			name: "an online build names its index, and never skips one that exists",
			body: "-- online\nCREATE INDEX CONCURRENTLY IF NOT EXISTS t_a_idx ON __SCHEMA__.t (a);",
			err:  "must be CREATE INDEX CONCURRENTLY",
		},
		{
			name: "an online step has a statement",
			body: "-- online\n-- Nothing.",
			err:  "has no statement",
		},
		{
			name: "CONCURRENTLY runs only in an online step",
			body: "SET LOCAL lock_timeout = '5s';\nCREATE INDEX CONCURRENTLY t_a_idx ON __SCHEMA__.t (a);",
			err:  "put the statement in an online step",
		},
	}

	for _, tt := range tests {
		m, err := Parse("0010_test.sql", tt.body, "s")
		switch {
		case tt.err != "":
			if err == nil || !strings.Contains(err.Error(), tt.err) {
				t.Errorf("%s: got %v, want an error with %q", tt.name, err, tt.err)
			}
		case err != nil:
			t.Errorf("%s: %v", tt.name, err)
		case !slices.Equal(m.Blocking, tt.blocking) || !slices.Equal(m.Statements, tt.online) || m.Online != (tt.online != nil):
			t.Errorf("%s: got blocking %q, online %v %+v; want %q, %+v", tt.name, m.Blocking, m.Online, m.Statements, tt.blocking, tt.online)
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
