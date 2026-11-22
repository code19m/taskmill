// Package migrations holds taskmill's versioned schema migrations.
//
// Each migration is a file named NNNN_name.sql. The placeholder __SCHEMA__ in
// a file stands for the schema name, which the caller validates. A file whose
// first line is "-- online" is an online step: see [Migration.Online].
package migrations

import (
	"embed"
	"fmt"
	"io/fs"
	"regexp"
	"strconv"
	"strings"
)

//go:embed *.sql
var files embed.FS

// Migration is one versioned schema change.
type Migration struct {
	Version int
	Name    string
	SQL     string

	// Online marks an online step: statements that build or drop an index
	// with CONCURRENTLY, which PostgreSQL runs only outside a transaction,
	// one at a time, without blocking writes. Statements holds them, in
	// order.
	Online     bool
	Statements []Statement

	// Blocking names the tables, schema-qualified, on which a migration
	// that is not online builds an index that blocks writes while it reads
	// the table: CREATE INDEX, or a unique or primary key constraint. Tables
	// that the migration creates itself are left out: they are empty.
	Blocking []string
}

// Statement is one statement of an online step.
type Statement struct {
	SQL   string
	Index string // the index that it builds or drops, schema-qualified
	Table string // the table that it indexes, schema-qualified; empty for a drop
}

// onlineMarker is the first line of an online step.
const onlineMarker = "-- online"

// The statements that the migrations are read for, with their words joined
// by single spaces. Names are schema-qualified, as __SCHEMA__.name.
var (
	createOnline = regexp.MustCompile(`(?i)^CREATE (?:UNIQUE )?INDEX CONCURRENTLY ([a-z0-9_]+) ON (([a-z0-9_]+)\.[a-z0-9_]+)\b`)
	dropOnline   = regexp.MustCompile(`(?i)^DROP INDEX CONCURRENTLY (?:IF EXISTS )?([a-z0-9_]+\.[a-z0-9_]+)$`)
	createIndex  = regexp.MustCompile(`(?i)^CREATE (?:UNIQUE )?INDEX (?:IF NOT EXISTS )?(?:[a-z0-9_]+ )?ON (?:ONLY )?([a-z0-9_]+\.[a-z0-9_]+)\b`)
	addKey       = regexp.MustCompile(`(?i)^ALTER TABLE (?:IF EXISTS )?(?:ONLY )?([a-z0-9_]+\.[a-z0-9_]+) .*\bADD (?:CONSTRAINT [a-z0-9_]+ )?(?:UNIQUE|PRIMARY KEY)\b`)
	createTable  = regexp.MustCompile(`(?i)^CREATE TABLE ([a-z0-9_]+\.[a-z0-9_]+)\b`) // not IF NOT EXISTS: that table may hold rows
	concurrently = regexp.MustCompile(`(?i)\bCONCURRENTLY\b`)
	usingIndex   = regexp.MustCompile(`(?i)\bUSING INDEX\b`)
)

// All returns every migration for schema, in version order.
func All(schema string) ([]Migration, error) {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}

	all := make([]Migration, 0, len(entries))
	for _, e := range entries {
		body, err := fs.ReadFile(files, e.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", e.Name(), err)
		}

		m, err := Parse(e.Name(), string(body), schema)
		if err != nil {
			return nil, err
		}
		all = append(all, m)
	}

	// ReadDir sorts by name, and names start with zero-padded versions.
	for i, m := range all {
		if m.Version != i+1 {
			return nil, fmt.Errorf("migration versions must be consecutive from 1, found %d at position %d", m.Version, i+1)
		}
	}
	return all, nil
}

// Parse returns the migration that the file named file holds, with the text
// body, for schema.
func Parse(file, body, schema string) (Migration, error) {
	version, name, ok := parseName(file)
	if !ok {
		return Migration{}, fmt.Errorf("invalid migration file name %q", file)
	}
	m := Migration{Version: version, Name: name, SQL: strings.ReplaceAll(body, "__SCHEMA__", schema)}
	first, _, _ := strings.Cut(m.SQL, "\n")
	m.Online = strings.TrimSpace(first) == onlineMarker

	created := make(map[string]bool)
	for _, stmt := range Split(m.SQL) {
		words := wordsOf(stmt)
		switch {
		case m.Online:
			s, ok := onlineStatement(stmt, words)
			if !ok {
				return Migration{}, fmt.Errorf("migration %s is online, so each of its statements must be "+
					"CREATE INDEX CONCURRENTLY name ON schema.table, or DROP INDEX CONCURRENTLY schema.name, not %q", file, words)
			}
			m.Statements = append(m.Statements, s)
		case concurrently.MatchString(words):
			return Migration{}, fmt.Errorf("migration %s uses CONCURRENTLY, which runs only outside a transaction: "+
				"put the statement in an online step, a migration whose first line is %q", file, onlineMarker)
		}

		if match := createTable.FindStringSubmatch(words); match != nil {
			created[match[1]] = true
		}
		table := ""
		if match := createIndex.FindStringSubmatch(words); match != nil {
			table = match[1]
		} else if match := addKey.FindStringSubmatch(words); match != nil && !usingIndex.MatchString(words) {
			table = match[1]
		}
		if table != "" && !created[table] {
			m.Blocking = append(m.Blocking, table)
		}
	}

	if m.Online && len(m.Statements) == 0 {
		return Migration{}, fmt.Errorf("migration %s is online, and has no statement", file)
	}
	return m, nil
}

// onlineStatement reads stmt, a statement of an online step, whose words
// are words.
func onlineStatement(stmt, words string) (Statement, bool) {
	if match := createOnline.FindStringSubmatch(words); match != nil {
		return Statement{SQL: stmt, Index: match[3] + "." + match[1], Table: match[2]}, true
	}
	if match := dropOnline.FindStringSubmatch(words); match != nil {
		return Statement{SQL: stmt, Index: match[1]}, true
	}
	return Statement{}, false
}

// Split returns the statements of a migration, without its comments. It
// splits at each semicolon outside quoted text, also inside the body of a DO
// block or a function: the parts still name what the body does, but only
// the statements of an online step run one by one.
func Split(sql string) []string {
	var out []string
	var stmt strings.Builder
	end := func() {
		if s := strings.TrimSpace(stmt.String()); s != "" {
			out = append(out, s)
		}
		stmt.Reset()
	}

	for i := 0; i < len(sql); i++ {
		switch rest := sql[i:]; {
		case rest[0] == '\'':
			n := quoted(rest)
			stmt.WriteString(rest[:n])
			i += n - 1
		case strings.HasPrefix(rest, "--"):
			n := strings.IndexByte(rest, '\n')
			if n < 0 {
				n = len(rest)
			}
			i += n - 1
		case strings.HasPrefix(rest, "/*"):
			n := strings.Index(rest, "*/")
			if n < 0 {
				n = len(rest) - 2
			}
			stmt.WriteByte(' ')
			i += n + 1
		case rest[0] == ';':
			end()
		default:
			stmt.WriteByte(rest[0])
		}
	}
	end()
	return out
}

// quoted returns the length of the quoted text that s starts with, quotes
// included. Two quotes in a row stand for one inside it.
func quoted(s string) int {
	for i := 1; i < len(s); i++ {
		if s[i] != '\'' {
			continue
		}
		if i+1 < len(s) && s[i+1] == '\'' {
			i++
			continue
		}
		return i + 1
	}
	return len(s)
}

// wordsOf returns the words of stmt, joined by single spaces, with each
// quoted text emptied: what the statement does, not what its text says.
func wordsOf(stmt string) string {
	var code strings.Builder
	for i := 0; i < len(stmt); i++ {
		if stmt[i] == '\'' {
			i += quoted(stmt[i:]) - 1
			code.WriteString("''")
			continue
		}
		code.WriteByte(stmt[i])
	}
	return strings.Join(strings.Fields(code.String()), " ")
}

// Latest returns the highest migration version.
func Latest() int {
	all, err := All("taskmill")
	if err != nil {
		return 0
	}
	return len(all)
}

// parseName splits "0001_initial.sql" into 1 and "initial".
func parseName(file string) (version int, name string, ok bool) {
	base, found := strings.CutSuffix(file, ".sql")
	if !found {
		return 0, "", false
	}

	num, name, found := strings.Cut(base, "_")
	if !found || len(num) != 4 || name == "" {
		return 0, "", false
	}

	version, err := strconv.Atoi(num)
	if err != nil || version < 1 {
		return 0, "", false
	}
	return version, name, true
}
