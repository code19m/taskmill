package ui

import (
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/code19m/taskmill"
)

func TestDrawBreaksLinesAtGaps(t *testing.T) {
	t.Parallel()
	nan := math.NaN()
	for _, c := range []struct {
		values     []float64
		line, area string
	}{
		{[]float64{nan, nan}, "", ""},
		// A lone value is a dot; a gap starts a new run.
		{[]float64{4, nan, 0, 2}, "M0.5 4.0h0M2.5 96.0L3.5 50.0", "M0.5 96L0.5 4.0L0.5 96ZM2.5 96L2.5 96.0L3.5 50.0L3.5 96Z"},
	} {
		line, area, drawn := draw(c.values, 4)
		if line != c.line || area != c.area || drawn != (c.line != "") {
			t.Errorf("draw(%v) = %q, %q, %v; want %q, %q", c.values, line, area, drawn, c.line, c.area)
		}
	}
}

func TestThroughputReadsTheNewestCompleteStep(t *testing.T) {
	t.Parallel()
	minute := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	ago := func(steps int, step time.Duration) time.Time { return minute.Add(-time.Duration(steps) * step) }
	hour := []taskmill.Activity{
		{At: ago(3, time.Minute), Completed: 900, Failed: 100},
		{At: ago(2, time.Minute), Completed: 500, Failed: 20},
		{At: ago(1, time.Minute), Completed: 600, Failed: 60},
		{At: minute, Completed: 5}, // in progress: it counts for nothing
	}
	for _, c := range []struct {
		name       string
		span       chartRange
		points     []taskmill.Activity
		now        time.Time
		rate, peak float64
	}{
		{"the minute before this one", chartRanges[0], hour, minute.Add(30 * time.Second), 660, 1000},
		// Workers may still add to a minute that has just ended.
		{"a minute that has just ended waits", chartRanges[0], hour, minute.Add(5 * time.Second), 520, 1000},
		{"nothing recorded in it", chartRanges[0], hour[:2], minute.Add(30 * time.Second), math.NaN(), 1000},
		{"nothing recorded at all", chartRanges[0], nil, minute.Add(30 * time.Second), math.NaN(), 0},
		// A longer step counts as its mean rate.
		{"a step of five minutes", chartRanges[1], []taskmill.Activity{
			{At: ago(2, 5*time.Minute), Completed: 4000},
			{At: ago(1, 5*time.Minute), Completed: 3000, Failed: 300},
			{At: minute, Completed: 9000},
		}, minute.Add(time.Minute), 660, 800},
	} {
		rate, peak := throughput(c.points, windowOf(c.span, c.now), c.now)
		if rate != c.rate && !(math.IsNaN(rate) && math.IsNaN(c.rate)) || peak != c.peak {
			t.Errorf("%s: got %v a minute and a peak of %v, want %v and %v", c.name, rate, peak, c.rate, c.peak)
		}
	}
}

// The chart of the attempts draws the steps that the dial reads: a step in
// progress is not on it, and neither is the step before it until its count
// is complete, 15 seconds after it ended. Drawn sooner, the line would end
// in a dip at the start of every step.
func TestAttemptChartDrawsTheStepsThatAreComplete(t *testing.T) {
	t.Parallel()
	minute := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	points := []taskmill.Activity{
		{At: minute.Add(-2 * time.Minute), Completed: 500, Failed: 20},
		{At: minute.Add(-time.Minute), Completed: 510, Failed: 3}, // workers still add to it for a moment
		{At: minute, Completed: 5},
	}
	const counting = "The first point shows when the count of its minute is complete."
	for _, c := range []struct {
		name              string
		points            []taskmill.Activity
		after             time.Duration // since the minute started
		completed, failed string        // the values of the last four steps
		note              string
	}{
		{"a minute that has just ended waits", points, 5 * time.Second, " 500  ", " 20  ", ""},
		{"until its count is complete", points, 14 * time.Second, " 500  ", " 20  ", ""},
		{"then it is drawn", points, 15 * time.Second, " 500 510 ", " 20 3 ", ""},
		{"only steps that still count", points[1:], 5 * time.Second, "   ", "   ", counting},
		{"only the step in progress", points[2:], 30 * time.Second, "   ", "   ", counting},
		{"nothing recorded", nil, 30 * time.Second, "   ", "   ", "Nothing recorded yet."},
	} {
		now := minute.Add(c.after)
		chart := attemptChart(c.points, windowOf(chartRanges[0], now), now)
		for k, want := range []string{c.completed, c.failed} {
			if got := chart.Series[k].Values; !strings.HasSuffix(got, want) || strings.TrimSpace(got) != strings.TrimSpace(want) {
				t.Errorf("%s: the chart holds the %s attempts %q, want them to end with %q", c.name, strings.ToLower(chart.Series[k].Name), got, want)
			}
		}
		if chart.Note != c.note {
			t.Errorf("%s: the chart's note is %q, want %q", c.name, chart.Note, c.note)
		}
	}
}

func TestDialTopIsTheSmallestRoundScale(t *testing.T) {
	t.Parallel()
	for _, c := range []struct{ peak, top float64 }{
		{0, 10},
		{0.4, 10},
		{10, 10},
		{10.5, 20},
		{20, 20},
		{21, 50},
		{50, 50},
		{51, 100},
		{660, 1000},
		{999.9, 1000},
		{1000, 1000},
		{1000.1, 2000},
		{4999, 5000},
		{5001, 10000},
		{1.2e6, 2e6},
		// What no count is: the scale still has a top.
		{-5, 10},
		{math.NaN(), 10},
	} {
		if top := dialTop(c.peak); top != c.top {
			t.Errorf("dialTop(%v) = %v, want %v", c.peak, top, c.top)
		}
	}
}

func TestDialTurnsTheNeedleByTheRate(t *testing.T) {
	t.Parallel()
	nan := math.NaN()
	for _, c := range []struct {
		rate, peak                float64
		angle, value, unit, label string
	}{
		{0, 700, "0", "0", "attempts a minute", "0 attempts a minute, on a dial from 0 to 1,000"},
		{660, 700, "118.8", "660", "attempts a minute", "660 attempts a minute, on a dial from 0 to 1,000"},
		{1000, 1000, "180", "1,000", "attempts a minute", "1,000 attempts a minute, on a dial from 0 to 1,000"},
		{0.5, 2, "9", "0.5", "attempts a minute", "0.5 attempts a minute, on a dial from 0 to 10"},
		{1, 2, "18", "1", "attempt a minute", "1 attempt a minute, on a dial from 0 to 10"},
		// Without a rate, the needle rests on zero, on the scale of the period.
		{nan, 700, "0", "—", "attempts a minute", "Attempts a minute: no data"},
		{nan, 0, "0", "—", "attempts a minute", "Attempts a minute: no data"},
		// The needle never leaves the scale, whatever the table holds.
		{-3, 5, "0", "-3", "attempts a minute", "-3 attempts a minute, on a dial from 0 to 10"},
		{30, 5, "180", "30", "attempts a minute", "30 attempts a minute, on a dial from 0 to 10"},
	} {
		d := newDial(c.rate, c.peak)
		if d.Angle != c.angle || d.Value != c.value || d.Unit != c.unit || d.Label != c.label {
			t.Errorf("newDial(%v, %v) = angle %s, %q %q, %q; want angle %s, %q %q, %q",
				c.rate, c.peak, d.Angle, d.Value, d.Unit, d.Label, c.angle, c.value, c.unit, c.label)
		}
	}
}

func TestDialDrawsItsScale(t *testing.T) {
	t.Parallel()
	// Zero is on the left of the hub, half of the scale above it, and the top
	// on the right.
	for _, c := range []struct{ share, x, y float64 }{{0, -50, 0}, {0.25, -35.36, -35.36}, {0.5, 0, -50}, {1, 50, 0}} {
		if x, y := dialPoint(c.share, 50); math.Abs(x-c.x) > 0.01 || math.Abs(y-c.y) > 0.01 {
			t.Errorf("dialPoint(%v, 50) = %.2f, %.2f; want %v, %v", c.share, x, y, c.x, c.y)
		}
	}
	// A label at every quarter of the scale, in few characters, each at a
	// major tick, with four minor ticks between two of them. The labels of
	// zero and of the top stand under the ends of the scale, where a needle
	// at rest does not cross them.
	d := newDial(660, 700)
	var texts []string
	for _, m := range d.Marks {
		texts = append(texts, m.Text)
	}
	if want := []string{"0", "250", "500", "750", "1k"}; !slices.Equal(texts, want) {
		t.Fatalf("labels: got %v, want %v", texts, want)
	}
	zero, half, top := d.Marks[0], d.Marks[2], d.Marks[4]
	if !strings.HasPrefix(zero.X, "-") || strings.HasPrefix(zero.Y, "-") || zero.Y == "0" ||
		half.X != "0" || !strings.HasPrefix(half.Y, "-") || zero.X != "-"+top.X || zero.Y != top.Y {
		t.Errorf("labels: got 0 at %s,%s, the half at %s,%s, and the top at %s,%s", zero.X, zero.Y, half.X, half.Y, top.X, top.Y)
	}
	if major, minor := strings.Count(d.Major, "M"), strings.Count(d.Minor, "M"); major != 5 || minor != 16 {
		t.Errorf("got %d major and %d minor ticks, want 5 and 16", major, minor)
	}
	for v, want := range map[float64]string{
		0: "0", 2.5: "2.5", 37.5: "37.5", 250: "250", 1000: "1k", 1250: "1.25k", 37500: "37.5k", 1e6: "1M", 1.25e6: "1.25M",
	} {
		if got := short(v); got != want {
			t.Errorf("short(%v) = %s, want %s", v, got, want)
		}
	}
}

func TestChartHoldsItsValuesForTheScript(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 19, 12, 0, 30, 0, time.UTC)
	w := window{start: now.Add(-3 * time.Minute).Truncate(time.Minute), step: time.Minute, steps: 4}
	c := newChart("Tasks", "", w, now, []line{{name: "Waiting", values: []float64{1234, math.NaN(), 0.5, 7}}})
	// One value per step, as the page words it, and nothing for a gap.
	if got := c.Series[0].Values; got != "1,234  0.5 7" {
		t.Errorf("values: got %q", got)
	}
	if c.Start != w.start.Unix() || c.Step != 60 {
		t.Errorf("got steps of %d seconds from %d, want 60 from %d", c.Step, c.Start, w.start.Unix())
	}
	// The columns' tooltips say the same.
	if got := c.Columns[0].Title; got != "3 min ago: 1,234 waiting" {
		t.Errorf("tooltip: got %q", got)
	}
	if got := c.Columns[1].Title; got != "2 min ago: no data" {
		t.Errorf("tooltip of a gap: got %q", got)
	}
}
