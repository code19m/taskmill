package ui

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/code19m/taskmill"
)

// chartRange is a span of time the overview's charts can show, with the step
// of their points.
type chartRange struct {
	Name       string
	span, step time.Duration
}

var chartRanges = []chartRange{
	{"1h", time.Hour, time.Minute},
	{"6h", 6 * time.Hour, 5 * time.Minute},
	{"24h", 24 * time.Hour, 15 * time.Minute},
}

// rangeOf returns the range named name, or the first one.
func rangeOf(name string) chartRange {
	for _, r := range chartRanges {
		if r.Name == name {
			return r
		}
	}
	return chartRanges[0]
}

// window is the steps a chart shows, ending with the step that holds now.
type window struct {
	start time.Time
	step  time.Duration
	steps int
}

func windowOf(r chartRange, now time.Time) window {
	end := now.Truncate(r.step).Add(r.step)
	return window{start: end.Add(-r.span), step: r.step, steps: int(r.span / r.step)}
}

// chart is a line chart that the server draws as SVG, so that it needs no
// script. Its view box is one unit wide per step and 100 units tall; the
// browser stretches it to the width of the page, and lines keep their width.
type chart struct {
	Title, Label string
	Steps        int
	Top, Half    string // labels of the y axis
	From, Mid    string // labels of the x axis, before "now"
	Series       []series
	Columns      []column
	Note         string // shown over a chart with nothing to draw
}

type series struct {
	Name, Class string // Class colors it by task state
	Line, Area  string // SVG paths; Area is empty for a plain line
}

// column is one step, with a tooltip that lists its values.
type column struct {
	X     int
	Title string
}

// line is one series' values, by step; NaN where nothing was recorded.
type line struct {
	name, class string
	area        bool
	values      []float64
}

// newChart scales lines to a shared y axis and draws them.
func newChart(title, unit string, w window, now time.Time, lines []line) chart {
	top := 0.0
	for _, l := range lines {
		for _, v := range l.values {
			if !math.IsNaN(v) {
				top = max(top, v)
			}
		}
	}
	top = niceTop(top)
	c := chart{
		Title: title, Steps: w.steps, Note: "Nothing recorded yet",
		Top: formatValue(top), Half: formatValue(top / 2),
		From: compact(w.step*time.Duration(w.steps)) + " ago", Mid: compact(w.step*time.Duration(w.steps)/2) + " ago",
	}
	c.Label = fmt.Sprintf("%s over the last %s", title, compact(w.step*time.Duration(w.steps)))
	for _, l := range lines {
		s := series{Name: l.name, Class: l.class}
		var drawn bool
		s.Line, s.Area, drawn = draw(l.values, top)
		if !l.area {
			s.Area = ""
		}
		if drawn {
			c.Note = ""
		}
		c.Series = append(c.Series, s)
	}
	for i := range w.steps {
		at := w.start.Add(time.Duration(i) * w.step)
		var parts []string
		for _, l := range lines {
			if v := l.values[i]; !math.IsNaN(v) {
				parts = append(parts, formatValue(v)+" "+strings.ToLower(l.name))
			}
		}
		text := "no data"
		if len(parts) > 0 {
			text = strings.Join(parts, ", ") + unit
		}
		c.Columns = append(c.Columns, column{X: i, Title: sinceLabel(now.Sub(at)) + ": " + text})
	}
	return c
}

// draw returns the paths of a line and of the area under it, scaled so that
// top is at the top of the chart. Each run of values between gaps is its own
// subpath, and a value alone shows as a dot. It reports whether it drew any.
func draw(values []float64, top float64) (line, area string, drawn bool) {
	var l, a strings.Builder
	y := func(v float64) string { return strconv.FormatFloat(96-v/top*92, 'f', 1, 64) }
	gap := func(i int) bool { return i < 0 || i >= len(values) || math.IsNaN(values[i]) }
	for i, v := range values {
		if gap(i) {
			continue
		}
		x := strconv.FormatFloat(float64(i)+0.5, 'f', 1, 64)
		if gap(i - 1) {
			fmt.Fprintf(&l, "M%s %s", x, y(v))
			fmt.Fprintf(&a, "M%s 96L%s %s", x, x, y(v))
		} else {
			fmt.Fprintf(&l, "L%s %s", x, y(v))
			fmt.Fprintf(&a, "L%s %s", x, y(v))
		}
		if gap(i + 1) {
			if gap(i - 1) {
				l.WriteString("h0")
			}
			fmt.Fprintf(&a, "L%s 96Z", x)
		}
	}
	return l.String(), a.String(), l.Len() > 0
}

// taskChart charts the tasks waiting, available, running, and dead.
func taskChart(points []taskmill.Activity, w window, now time.Time) chart {
	lines := []line{
		{name: "Waiting", class: "state-scheduled", values: blank(w.steps)},
		{name: "Available", class: "state-available", area: true, values: blank(w.steps)},
		{name: "Running", class: "state-running", area: true, values: blank(w.steps)},
		{name: "Dead", class: "state-dead", values: blank(w.steps)},
	}
	for _, p := range points {
		i, ok := w.index(p.At)
		if !ok || p.Counts == nil {
			continue
		}
		c := p.Counts
		lines[0].values[i] = float64(c[taskmill.StateScheduled] + c[taskmill.StateRetryable] + c[taskmill.StatePending])
		lines[1].values[i] = float64(c[taskmill.StateAvailable])
		lines[2].values[i] = float64(c[taskmill.StateRunning])
		lines[3].values[i] = float64(c[taskmill.StateDead])
	}
	return newChart("Tasks", "", w, now, lines)
}

// attemptChart charts the attempts that completed and failed, per minute.
// The step in progress is left out: its rate would read as a drop.
func attemptChart(points []taskmill.Activity, w window, now time.Time) chart {
	lines := []line{
		{name: "Completed", class: "state-completed", area: true, values: blank(w.steps)},
		{name: "Failed", class: "state-dead", values: blank(w.steps)},
	}
	minutes := w.step.Minutes()
	inProgress := false
	for _, p := range points {
		i, ok := w.index(p.At)
		switch {
		case !ok:
		case i == w.steps-1:
			inProgress = true
		default:
			lines[0].values[i] = float64(p.Completed) / minutes
			lines[1].values[i] = float64(p.Failed) / minutes
		}
	}
	c := newChart("Attempts", " a minute", w, now, lines)
	if c.Note != "" && inProgress {
		c.Note = "The first point shows when this " + stepName(w.step) + " ends"
	}
	return c
}

// stepName names a step in a sentence: "minute", or "5m step".
func stepName(d time.Duration) string {
	if d == time.Minute {
		return "minute"
	}
	return compact(d) + " step"
}

// index returns the step that starts at at.
func (w window) index(at time.Time) (int, bool) {
	d := at.Sub(w.start)
	if d < 0 || d%w.step != 0 {
		return 0, false
	}
	i := int(d / w.step)
	return i, i < w.steps
}

func blank(n int) []float64 {
	v := make([]float64, n)
	for i := range v {
		v[i] = math.NaN()
	}
	return v
}

// niceTop rounds the largest value up to a round top for the y axis.
func niceTop(v float64) float64 {
	if v <= 4 {
		return 4
	}
	step := math.Pow(10, math.Floor(math.Log10(v)))
	return math.Ceil(v/step) * step
}

// formatValue formats a count or a rate: whole numbers with separators, and
// small fractions with one decimal.
func formatValue(v float64) string {
	if v < 10 && v != math.Trunc(v) {
		return strconv.FormatFloat(v, 'f', 1, 64)
	}
	return number(int64(math.Round(v)))
}

// compact formats a span such as "30m" or "6h".
func compact(d time.Duration) string {
	if d < time.Hour {
		return strconv.Itoa(int(d.Minutes())) + "m"
	}
	return strconv.Itoa(int(d.Hours())) + "h"
}

// sinceLabel says how long ago a step started.
func sinceLabel(d time.Duration) string {
	if d < time.Minute {
		return "this minute"
	}
	if d < time.Hour {
		return strconv.Itoa(int(d.Minutes())) + " min ago"
	}
	return fmt.Sprintf("%dh %02dm ago", int(d.Hours()), int(d.Minutes())%60)
}
