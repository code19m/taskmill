package cron

import (
	"testing"
	"time"
)

func TestNext(t *testing.T) {
	t.Parallel()
	utc := time.UTC
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("time zone database unavailable")
	}
	berlin, _ := time.LoadLocation("Europe/Berlin")
	sydney, _ := time.LoadLocation("Australia/Sydney")
	auckland, _ := time.LoadLocation("Pacific/Auckland")
	tests := []struct {
		expr string
		from time.Time
		want time.Time
	}{
		{"* * * * *", time.Date(2026, 1, 1, 10, 0, 30, 0, utc), time.Date(2026, 1, 1, 10, 1, 0, 0, utc)},
		{"*/15 * * * *", time.Date(2026, 1, 1, 10, 7, 0, 0, utc), time.Date(2026, 1, 1, 10, 15, 0, 0, utc)},
		{"0 3 * * *", time.Date(2026, 1, 1, 3, 0, 0, 0, utc), time.Date(2026, 1, 2, 3, 0, 0, 0, utc)},
		{"30 9 * * MON-FRI", time.Date(2026, 1, 2, 10, 0, 0, 0, utc), time.Date(2026, 1, 5, 9, 30, 0, 0, utc)},
		{"0 0 1 * *", time.Date(2026, 1, 15, 0, 0, 0, 0, utc), time.Date(2026, 2, 1, 0, 0, 0, 0, utc)},
		{"0 0 29 2 *", time.Date(2026, 3, 1, 0, 0, 0, 0, utc), time.Date(2028, 2, 29, 0, 0, 0, 0, utc)},
		{"0 12 * * 7", time.Date(2026, 1, 1, 0, 0, 0, 0, utc), time.Date(2026, 1, 4, 12, 0, 0, 0, utc)},
		{"0 0 13 * FRI", time.Date(2026, 1, 1, 0, 0, 0, 0, utc), time.Date(2026, 1, 2, 0, 0, 0, 0, utc)},
		{"5,10 1-2 * JAN,FEB *", time.Date(2026, 12, 31, 23, 0, 0, 0, utc), time.Date(2027, 1, 1, 1, 5, 0, 0, utc)},
		{"@hourly", time.Date(2026, 1, 1, 10, 59, 59, 0, utc), time.Date(2026, 1, 1, 11, 0, 0, 0, utc)},
		{"@weekly", time.Date(2026, 1, 1, 0, 0, 0, 0, utc), time.Date(2026, 1, 4, 0, 0, 0, 0, utc)},
		// Spring forward: 02:30 does not exist on 2026-03-08 in New York.
		{"30 2 * * *", time.Date(2026, 3, 7, 12, 0, 0, 0, ny), time.Date(2026, 3, 8, 3, 30, 0, 0, ny)},
		{"0 9 * * *", time.Date(2026, 3, 7, 12, 0, 0, 0, ny), time.Date(2026, 3, 8, 9, 0, 0, 0, ny)},
		// Zones east of UTC: the skipped time runs right after the jump too.
		{"30 2 * * *", time.Date(2026, 3, 28, 12, 0, 0, 0, berlin), time.Date(2026, 3, 29, 3, 30, 0, 0, berlin)},
		{"0 2 * * *", time.Date(2026, 3, 29, 1, 30, 0, 0, berlin), time.Date(2026, 3, 29, 3, 0, 0, 0, berlin)},
		{"30 2 * * *", time.Date(2026, 10, 3, 12, 0, 0, 0, sydney), time.Date(2026, 10, 4, 3, 30, 0, 0, sydney)},
		{"30 2 * * *", time.Date(2026, 9, 26, 12, 0, 0, 0, auckland), time.Date(2026, 9, 27, 3, 30, 0, 0, auckland)},
		// Fall back: 01:30 happens twice on 2026-11-01 in New York; it runs once.
		{"30 1 * * *", time.Date(2026, 11, 1, 0, 0, 0, 0, ny), time.Date(2026, 11, 1, 1, 30, 0, 0, ny)},
		{"30 1 * * *", time.Date(2026, 11, 1, 1, 31, 0, 0, ny), time.Date(2026, 11, 2, 1, 30, 0, 0, ny)},
	}
	for _, tt := range tests {
		s, err := Parse(tt.expr)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tt.expr, err)
		}
		got, err := s.Next(tt.from)
		if err != nil {
			t.Fatalf("%q Next(%s): %v", tt.expr, tt.from, err)
		}
		if !got.Equal(tt.want) {
			t.Errorf("%q Next(%s) = %s, want %s", tt.expr, tt.from, got, tt.want)
		}
	}
}

func TestNextRunsTheRepeatedHourOnce(t *testing.T) {
	t.Parallel()
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("time zone database unavailable")
	}
	s, err := Parse("30 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	// From midnight to 04:00 on the day clocks go back, 01:xx happens twice
	// but runs once: 00:30, 01:30, 02:30, 03:30.
	at := time.Date(2026, 11, 1, 0, 0, 0, 0, ny)
	end := at.Add(5 * time.Hour)
	var runs int
	for {
		next, err := s.Next(at)
		if err != nil {
			t.Fatal(err)
		}
		if !next.Before(end) {
			break
		}
		runs++
		at = next
	}
	if runs != 4 {
		t.Fatalf("got %d runs, want 4", runs)
	}
}

func TestNextNeverMatches(t *testing.T) {
	t.Parallel()
	s, err := Parse("0 0 31 2 *")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Next(time.Now()); err == nil {
		t.Fatal("February 31 matched a time")
	}
}

func TestParseRejects(t *testing.T) {
	t.Parallel()
	for _, expr := range []string{
		"", "* * * *", "* * * * * *", "60 * * * *", "* 24 * * *", "* * 0 * *", "* * * 13 *",
		"* * * * 8", "*/0 * * * *", "5-1 * * * *", "a * * * *", "* * * FOO *", "1-2-3 * * * *",
	} {
		if _, err := Parse(expr); err == nil {
			t.Errorf("Parse(%q) succeeded", expr)
		}
	}
}

func FuzzParse(f *testing.F) {
	for _, s := range []string{"* * * * *", "*/5 1-3 1,15 JAN-MAR MON", "@daily", "0 0 29 2 *"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, expr string) {
		s, err := Parse(expr)
		if err != nil {
			return
		}
		from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		next, err := s.Next(from)
		if err != nil {
			return
		}
		if !next.After(from) || next.Second() != 0 {
			t.Fatalf("%q: Next(%s) = %s", expr, from, next)
		}
	})
}
