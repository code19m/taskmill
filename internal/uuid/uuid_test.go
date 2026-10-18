package uuid

import (
	"regexp"
	"testing"
	"time"
)

var v7 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestNewV7(t *testing.T) {
	t.Parallel()
	seen := make(map[string]bool)
	for range 10_000 {
		id := NewV7()
		if !v7.MatchString(id) {
			t.Fatalf("%q is not a version 7 UUID", id)
		}
		if seen[id] {
			t.Fatalf("duplicate %q", id)
		}
		seen[id] = true
	}
}

func TestNewV7IsOrderedByTime(t *testing.T) {
	t.Parallel()
	earlier := newV7(time.UnixMilli(1_700_000_000_000))
	later := newV7(time.UnixMilli(1_700_000_000_001))
	if earlier[:13] >= later[:13] {
		t.Fatalf("%s does not sort before %s", earlier, later)
	}
	if got := newV7(time.UnixMilli(0x0123456789ab))[:13]; got != "01234567-89ab" {
		t.Fatalf("timestamp encoded as %s", got)
	}
}
