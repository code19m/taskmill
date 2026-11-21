package ui

import (
	"testing"
	"time"

	"github.com/code19m/taskmill/ui/internal/twins"
)

// The words of a relative time: the script keeps the times of a page
// counting in the same words, from the same two instants (ago in
// static/app.js), so the cases are the twins' to share.
func TestAgoWordsATimeToTheSecond(t *testing.T) {
	t.Parallel()
	for _, c := range twins.Ago {
		if got := ago(twins.Now, twins.Now.Add(-c.Before)); got != c.Want {
			t.Errorf("a time %v before the page was rendered shows as %q, want %q", c.Before, got, c.Want)
		}
	}

	// No time, and times farther away than a duration can hold.
	if got := ago(twins.Now, time.Time{}); got != "—" {
		t.Errorf("no time shows as %q, want a dash", got)
	}

	if got := ago(twins.Now, twins.Now.AddDate(400, 0, 0)); got != "in 146097d" {
		t.Errorf("a time 400 years ahead shows as %q, want in 146097d", got)
	}
}

// The words of a length of time: the script counts a gap in whole seconds,
// in the same words (span in static/app.js).
func TestSpanWordsALengthOfTime(t *testing.T) {
	t.Parallel()
	for _, c := range twins.Span {
		if got := span(c.D); got != c.Want {
			t.Errorf("span(%v) = %q, want %q", c.D, got, c.Want)
		}
	}
}

// A name shows the characters that no one could see, as the script shows a
// name that it adds to a page (visible in static/app.js).
func TestVisibleShowsTheCharactersThatNoOneCouldSee(t *testing.T) {
	t.Parallel()
	for _, c := range twins.Visible {
		if got := visible(c.Name); got != c.Want {
			t.Errorf("visible(%q) = %q, want %q", c.Name, got, c.Want)
		}
	}
}
