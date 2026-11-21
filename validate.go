package taskmill

import (
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

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

// maxFilterValues is the most values of one filter, such as the queues of a
// TaskFilter. A call of Groups reads up to a page of each queue.
const maxFilterValues = 100

// checkFilter checks that a filter has at most maxFilterValues values, which
// are valid names, and returns them as a slice that is never nil: NULL would
// match nothing.
func checkFilter(what string, values []string, maxLen int) ([]string, error) {
	if len(values) > maxFilterValues {
		return nil, invalidArgument("%d values of %s, more than the limit of %d", len(values), what, maxFilterValues)
	}
	out := make([]string, 0, len(values))
	for _, v := range values {
		if err := checkName(what, v, maxLen); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// PostgreSQL stores the times from minTime up to, but not including,
// endTime. The driver does not check: it sends a later time as a number
// that wraps around, which PostgreSQL reads as a time in the past, or
// rejects.
var (
	minTime = time.Date(-4713, time.November, 24, 0, 0, 0, 0, time.UTC) // 4714 BC
	endTime = time.Date(294277, time.January, 1, 0, 0, 0, 0, time.UTC)
)

// checkTime checks that PostgreSQL can store t. what names the time in the
// error.
func checkTime(what string, t time.Time) error {
	if t.Before(minTime) || !t.Before(endTime) {
		return invalidArgument("%s %s is outside the range that PostgreSQL stores, 4713 BC to 294276 AD",
			what, t.UTC().Format(time.RFC3339))
	}
	return nil
}

// checkName validates a queue name, kind, or group key.
func checkName(what, s string, maxLen int) error {
	switch {
	case s == "":
		return invalidArgument("%s is empty", what)
	case !utf8.ValidString(s):
		return invalidArgument("%s is not valid UTF-8", what)
	case strings.ContainsRune(s, 0):
		return invalidArgument("%s contains a NUL character", what)
	case utf8.RuneCountInString(s) > maxLen:
		return invalidArgument("%s that starts with %q is longer than %d characters", what, prefix(s), maxLen)
	}
	return nil
}

// prefix returns the first 32 bytes of s, at most, cut at a character's
// start: an error quotes only that much of a value that may be megabytes
// long.
func prefix(s string) string {
	for i := range s {
		if i > 32-utf8.UTFMax {
			return s[:i]
		}
	}
	return s
}
