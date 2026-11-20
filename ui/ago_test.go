package ui

import (
	"testing"
	"time"
)

// A relative time is worded from two times read to the second, as a clock
// shows them: so every such time of a page changes on the second. The
// script keeps the times of a page counting in the same words, from the
// same two instants: ago in static/app.js.
func TestAgoWordsATimeToTheSecond(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.November, 19, 12, 0, 0, 900_000_000, time.UTC)
	for _, c := range []struct {
		before time.Duration // how long before now; negative for a time ahead
		want   string
	}{
		{0, "now"},
		{900 * time.Millisecond, "now"},    // the same second
		{901 * time.Millisecond, "1s ago"}, // the second before
		{time.Second, "1s ago"},
		{59 * time.Second, "59s ago"},
		{time.Minute, "1m ago"},
		{59 * time.Minute, "59m ago"},
		{time.Hour, "1h ago"},
		{47 * time.Hour, "47h ago"},
		{48 * time.Hour, "2d ago"},
		{-99 * time.Millisecond, "now"},
		{-100 * time.Millisecond, "in 1s"}, // the next second
		{-time.Second, "in 1s"},
		{-59 * time.Second, "in 59s"},
		{-time.Minute, "in 1m"},
		{-59 * time.Minute, "in 59m"},
		{-time.Hour, "in 1h"},
		{-47 * time.Hour, "in 47h"},
		{-48 * time.Hour, "in 2d"},
	} {
		if got := ago(now, now.Add(-c.before)); got != c.want {
			t.Errorf("a time %v before the page was rendered shows as %q, want %q", c.before, got, c.want)
		}
	}

	// No time, and times farther away than a duration can hold.
	if got := ago(now, time.Time{}); got != "—" {
		t.Errorf("no time shows as %q, want a dash", got)
	}

	if got := ago(now, now.AddDate(400, 0, 0)); got != "in 146097d" {
		t.Errorf("a time 400 years ahead shows as %q, want in 146097d", got)
	}
}

// A length of time is worded to the tenth of a second below a second, and
// to the second above it, in two units at most, each cut down: "1.9s" is no
// whole second more than "1s". The script counts a gap in whole seconds, in
// the same words: span in static/app.js.
func TestSpanWordsALengthOfTime(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		d    time.Duration
		want string
	}{
		{-time.Second, "0s"}, // the timeline leaves such a gap out
		{0, "0s"},
		{time.Microsecond, "<1ms"},
		{999 * time.Microsecond, "<1ms"},
		{time.Millisecond, "1ms"},
		{35 * time.Millisecond, "35ms"},
		{999 * time.Millisecond, "999ms"},
		{time.Second, "1s"},
		{1300 * time.Millisecond, "1.3s"},
		{1999 * time.Millisecond, "1.9s"},
		{2 * time.Second, "2s"}, // a gap that counts: as the script words it
		{9999 * time.Millisecond, "9.9s"},
		{10 * time.Second, "10s"},
		{10900 * time.Millisecond, "10s"},
		{59*time.Second + 999*time.Millisecond, "59s"},
		{time.Minute, "1m"},
		{time.Minute + time.Second, "1m 1s"},
		{time.Hour - time.Second, "59m 59s"},
		{time.Hour, "1h"},
		{time.Hour + 59*time.Second, "1h"},
		{time.Hour + time.Minute, "1h 1m"},
		{24 * time.Hour, "24h"},
		{48*time.Hour - time.Second, "47h 59m"}, // as ago, which says "47h ago"
		{48 * time.Hour, "2d"},
		{49 * time.Hour, "2d 1h"},
		{400 * 24 * time.Hour, "400d"},
	} {
		if got := span(c.d); got != c.want {
			t.Errorf("span(%v) = %q, want %q", c.d, got, c.want)
		}
	}
}
