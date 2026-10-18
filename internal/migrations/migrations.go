// Package migrations holds taskmill's versioned schema migrations.
//
// Each migration is a file named NNNN_name.sql. The placeholder __SCHEMA__ in
// a file stands for the schema name, which the caller validates.
package migrations

import (
	"embed"
	"fmt"
	"io/fs"
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
}

// All returns every migration for schema, in version order.
func All(schema string) ([]Migration, error) {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}

	all := make([]Migration, 0, len(entries))
	for _, e := range entries {
		version, name, ok := parseName(e.Name())
		if !ok {
			return nil, fmt.Errorf("invalid migration file name %q", e.Name())
		}
		body, err := fs.ReadFile(files, e.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", e.Name(), err)
		}
		all = append(all, Migration{
			Version: version,
			Name:    name,
			SQL:     strings.ReplaceAll(string(body), "__SCHEMA__", schema),
		})
	}

	// ReadDir sorts by name, and names start with zero-padded versions.
	for i, m := range all {
		if m.Version != i+1 {
			return nil, fmt.Errorf("migration versions must be consecutive from 1, found %d at position %d", m.Version, i+1)
		}
	}
	return all, nil
}

// Latest returns the highest migration version.
func Latest() int {
	all, err := All("")
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
