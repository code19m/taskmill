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
		{-100 * time.Millisecond, "1s from now"}, // the next second
		{-time.Second, "1s from now"},
		{-59 * time.Second, "59s from now"},
		{-time.Minute, "1m from now"},
		{-59 * time.Minute, "59m from now"},
		{-time.Hour, "1h from now"},
		{-47 * time.Hour, "47h from now"},
		{-48 * time.Hour, "2d from now"},
	} {
		if got := ago(now, now.Add(-c.before)); got != c.want {
			t.Errorf("a time %v before the page was rendered shows as %q, want %q", c.before, got, c.want)
		}
	}
	// No time, and times farther away than a duration can hold.
	if got := ago(now, time.Time{}); got != "—" {
		t.Errorf("no time shows as %q, want a dash", got)
	}
	if got := ago(now, now.AddDate(400, 0, 0)); got != "146097d from now" {
		t.Errorf("a time 400 years ahead shows as %q, want 146097d from now", got)
	}
}
