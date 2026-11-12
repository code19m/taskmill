package cron

import (
	"slices"
	"strconv"
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
		// A step beyond the field's range matches the range's first value
		// only, up to the largest step that fits an int.
		{"*/100 * * * *", time.Date(2026, 1, 1, 10, 0, 0, 0, utc), time.Date(2026, 1, 1, 11, 0, 0, 0, utc)},
		{"5/9223372036854775807 * * * *", time.Date(2026, 1, 1, 10, 0, 0, 0, utc), time.Date(2026, 1, 1, 10, 5, 0, 0, utc)},
		{"0 0 */9223372036854775807 * *", time.Date(2026, 1, 15, 0, 0, 0, 0, utc), time.Date(2026, 2, 1, 0, 0, 0, 0, utc)},
		{"0 0 1 3-12/9223372036854775807 *", time.Date(2026, 1, 15, 0, 0, 0, 0, utc), time.Date(2026, 3, 1, 0, 0, 0, 0, utc)},
		// Spring forward: 02:30 does not exist on 2026-03-08 in New York.
		{"30 2 * * *", time.Date(2026, 3, 7, 12, 0, 0, 0, ny), time.Date(2026, 3, 8, 3, 30, 0, 0, ny)},
		{"0 9 * * *", time.Date(2026, 3, 7, 12, 0, 0, 0, ny), time.Date(2026, 3, 8, 9, 0, 0, 0, ny)},
		// Zones east of UTC: the skipped time runs right after the jump too.
		{"30 2 * * *", time.Date(2026, 3, 28, 12, 0, 0, 0, berlin), time.Date(2026, 3, 29, 3, 30, 0, 0, berlin)},
		{"0 2 * * *", time.Date(2026, 3, 29, 1, 30, 0, 0, berlin), time.Date(2026, 3, 29, 3, 0, 0, 0, berlin)},
		{"30 2 * * *", time.Date(2026, 10, 3, 12, 0, 0, 0, sydney), time.Date(2026, 10, 4, 3, 30, 0, 0, sydney)},
		{"30 2 * * *", time.Date(2026, 9, 26, 12, 0, 0, 0, auckland), time.Date(2026, 9, 27, 3, 30, 0, 0, auckland)},
		// Every skipped time runs, also one that follows another: 02:00 at
		// 03:00, and then 02:30 at 03:30. A search that starts after the
		// jump finds it too.
		{"0,30 2 * * *", time.Date(2026, 3, 8, 1, 59, 0, 0, ny), time.Date(2026, 3, 8, 3, 0, 0, 0, ny)},
		{"0,30 2 * * *", time.Date(2026, 3, 8, 3, 0, 0, 0, ny), time.Date(2026, 3, 8, 3, 30, 0, 0, ny)},
		{"0,30 2 * * *", time.Date(2026, 3, 8, 3, 30, 0, 0, ny), time.Date(2026, 3, 9, 2, 0, 0, 0, ny)},
		{"30 2 * * *", time.Date(2026, 3, 8, 3, 10, 30, 0, ny), time.Date(2026, 3, 8, 3, 30, 0, 0, ny)},
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

// clockRuns lists when s runs between start and end, without Next: it walks
// the clock of start's location minute by minute. A wall-clock time that s
// matches runs the first time the clock shows it. One that the clock skips
// runs after the jump, as long after it as the time is into the gap.
func clockRuns(s *Schedule, start, end time.Time) []time.Time {
	matches := func(c time.Time) bool {
		match, err := s.nextCivil(c)
		return err == nil && match.Equal(c)
	}
	var runs []time.Time
	shown := map[time.Time]bool{}
	last := civil(start).Add(-time.Minute) // the latest time that the clock showed
	for at := start; at.Before(end); at = at.Add(time.Minute) {
		c := civil(at)
		for skipped := last.Add(time.Minute); skipped.Before(c); skipped = skipped.Add(time.Minute) {
			if moved := at.Add(skipped.Sub(last.Add(time.Minute))); matches(skipped) && moved.Before(end) {
				runs = append(runs, moved)
			}
		}
		if !shown[c] && matches(c) {
			runs = append(runs, at)
		}
		shown[c] = true
		if c.After(last) {
			last = c
		}
	}
	slices.SortFunc(runs, time.Time.Compare)
	return slices.CompactFunc(runs, time.Time.Equal)
}

// nextRuns lists when s runs between start and end, as Next finds it.
func nextRuns(s *Schedule, start, end time.Time) []time.Time {
	var runs []time.Time
	for at := start.Add(-time.Nanosecond); ; {
		next, err := s.Next(at)
		if err != nil || !next.Before(end) {
			return runs
		}
		runs, at = append(runs, next), next
	}
}

// clockChanges lists the times in year at which the clocks of loc change.
func clockChanges(loc *time.Location, year int) []time.Time {
	var changes []time.Time
	at := time.Date(year, 1, 1, 0, 0, 0, 0, loc)
	for _, change := at.ZoneBounds(); !change.IsZero() && change.Year() == year; _, change = at.ZoneBounds() {
		changes = append(changes, change)
		at = change
	}
	return changes
}

// Around every clock change of several zones, Next must find what walking
// the clock finds: each wall-clock time once, and none twice. The zones
// change by half an hour, an hour, two hours, and a day, some at midnight.
func TestNextFindsEveryRunAroundClockChanges(t *testing.T) {
	t.Parallel()
	exprs := []string{
		"* * * * *", "*/15 * * * *", "0 * * * *", "0,30 2 * * *", "30 2 * * *", "10,35 2 * * *",
		"5 0-4 * * *", "*/7 1-3 * * *", "0 0 * * *", "59 23 * * *", "30 0-3 * * SUN", "0 12 30,31 12 *",
		"0 12 30 12 *",
	}
	for zone, year := range map[string]int{
		"America/New_York": 2026, "Europe/Berlin": 2026, "Australia/Lord_Howe": 2026, "Antarctica/Troll": 2026,
		"America/St_Johns": 2026, "America/Santiago": 2026, "America/Havana": 2026, "Pacific/Apia": 2011,
	} {
		loc, err := time.LoadLocation(zone)
		if err != nil {
			t.Skip("time zone database unavailable")
		}
		changes := clockChanges(loc, year)
		if len(changes) == 0 {
			t.Errorf("%s has no clock change in %d", zone, year)
		}
		for _, change := range changes {
			start, end := change.Add(-26*time.Hour).Truncate(time.Minute), change.Add(50*time.Hour).Truncate(time.Minute)
			for _, expr := range exprs {
				s, err := Parse(expr)
				if err != nil {
					t.Fatalf("Parse(%q): %v", expr, err)
				}
				got, want := nextRuns(s, start, end), clockRuns(s, start, end)
				if !slices.EqualFunc(got, want, time.Time.Equal) {
					t.Errorf("%q around the clock change of %s in %s: Next found %d runs, want %d: %s",
						expr, change.Format(time.RFC3339), zone, len(got), len(want), firstDifference(got, want))
				}
			}
		}
	}
}

// firstDifference describes where two lists of times first differ.
func firstDifference(got, want []time.Time) string {
	for i := range max(len(got), len(want)) {
		g, w := "nothing", "nothing"
		if i < len(got) {
			g = got[i].Format(time.RFC3339)
		}
		if i < len(want) {
			w = want[i].Format(time.RFC3339)
		}
		if g != w {
			return "run " + strconv.Itoa(i+1) + " is " + g + ", want " + w
		}
	}
	return "no difference"
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
	for _, s := range []string{
		"* * * * *", "*/5 1-3 1,15 JAN-MAR MON", "@daily", "0 0 29 2 *",
		// Steps near the largest int once overflowed the loop that sets a
		// field's bits, which then panicked.
		"0 0 */9223372036854775807 * *", "5/9223372036854775807 * * * *", "0 0 1 1-12/9223372036854775807 *",
	} {
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
