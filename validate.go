package taskmill

import "regexp"

// This file holds the checks of names and values that come from callers,
// before any statement runs with them.

// schemaPattern is what a schema name must match, so that it can be used in
// statements and as a notification channel without quoting.
var schemaPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// validateSchema checks a schema name and applies the default.
func validateSchema(schema string) (string, error) {
	if schema == "" {
		return defaultSchema, nil
	}
	if !schemaPattern.MatchString(schema) {
		return "", invalidArgument("schema %q must match %s", schema, schemaPattern)
	}
	return schema, nil
}
