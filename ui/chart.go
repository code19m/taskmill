package ui

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/code19m/taskmill"
)

// chartPeriod is a span of time the overview's charts can show, with the step
// of their points, and how long a read of its activity serves every overview:
// see activityCache.
type chartPeriod struct {
	Name             string
	span, step, keep time.Duration
}

var chartPeriods = []chartPeriod{
	{"1h", time.Hour, time.Minute, 0},
	{"6h", 6 * time.Hour, 5 * time.Minute, 30 * time.Second},
	{"24h", 24 * time.Hour, 15 * time.Minute, 30 * time.Second},
}

// periodOf returns the period named name, or the first one.
func periodOf(name string) chartPeriod {
	for _, p := range chartPeriods {
		if p.Name == name {
			return p
		}
	}
	return chartPeriods[0]
}

// window is the steps a chart shows, ending with the step that holds now.
type window struct {
	start time.Time
	step  time.Duration
	steps int
}

func windowOf(p chartPeriod, now time.Time) window {
	end := now.Truncate(p.step).Add(p.step)
	return window{start: end.Add(-p.span), step: p.step, steps: int(p.span / p.step)}
}

// activityCache keeps a read of the activity of each period that keeps its
// reads. A longer period reads the counts of every queue for hours, every
// open overview would read them again at each update, and its charts change
// little in half a minute. The hour's charts stay live.
type activityCache struct {
	mu   sync.Mutex
	kept map[string]keptActivity // by the name of the period
}

// keptActivity is a read of a period's activity: of the window that starts
// at start, at a time when complete of its steps had a complete count.
type keptActivity struct {
	start    time.Time
	complete int
	read     time.Time
	points   []taskmill.Activity
}

// activity returns the activity of window w of period p at now: from the
// cache, if it keeps a read of the same window, read less than p.keep ago, in
// which as many steps had a complete count as now; or else read anew. The
// charts only read the points, which every overview may share.
func (s *server) activity(ctx context.Context, p chartPeriod, w window, now time.Time) ([]taskmill.Activity, error) {
	c, complete := &s.charts, w.complete(now)
	c.mu.Lock()
	kept, ok := c.kept[p.Name]
	c.mu.Unlock()
	if ok && kept.start.Equal(w.start) && kept.complete == complete && now.Sub(kept.read) < p.keep {
		return kept.points, nil
	}

	points, err := s.client.Activity(ctx, &taskmill.ActivityParams{Since: w.start, Step: w.step})
	if err == nil && p.keep > 0 {
		c.mu.Lock()
		if c.kept == nil {
			c.kept = map[string]keptActivity{}
		}
		c.kept[p.Name] = keptActivity{start: w.start, complete: complete, read: now, points: points}
		c.mu.Unlock()
	}
	return points, err
}

// chart is a line chart that the server draws as SVG, so that it needs no
// script. Its view box is one unit wide per step and 100 units tall; the
// browser stretches it to the width of the page, and lines keep their width.
type chart struct {
	Title, Label string
	Steps        int
	Start, Step  int64  // when the first step starts and how long a step is, in Unix seconds
	Top, Half    string // labels of the y axis
	From, Mid    string // labels of the x axis, before "now"
	Series       []series
	Columns      []column
	Note         string // why a chart has nothing to draw, behind a hint
}

type series struct {
	Name, Class string // Class colors it by task state
	Line, Area  string // SVG paths; Area is empty for a plain line
	Values      string // each step's value as the page shows it, between spaces; nothing for a gap
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
		Title: title, Steps: w.steps, Note: "Nothing recorded yet.",
		Start: w.start.Unix(), Step: int64(w.step / time.Second),
		Top: formatValue(top), Half: formatValue(top / 2),
		From: compact(w.step*time.Duration(w.steps)) + " ago", Mid: compact(w.step*time.Duration(w.steps)/2) + " ago",
	}
	c.Label = fmt.Sprintf("%s over the last %s", title, compact(w.step*time.Duration(w.steps)))

	// Each value is worded once, for the tooltip of its column and for the
	// script, which shows the values of the step under the pointer.
	texts := make([][]string, len(lines))
	for k, l := range lines {
		texts[k] = wordValues(l.values)
		s := series{Name: l.name, Class: l.class, Values: strings.Join(texts[k], " ")}
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
		for k, l := range lines {
			if text := texts[k][i]; text != "" {
				parts = append(parts, text+" "+strings.ToLower(l.name))
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

// wordValues formats each value as the page shows it, and leaves a gap
// empty.
func wordValues(values []float64) []string {
	texts := make([]string, len(values))
	for i, v := range values {
		if !math.IsNaN(v) {
			texts[i] = formatValue(v)
		}
	}
	return texts
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

// attemptChart charts the attempts that completed and failed, per minute,
// in the steps whose counts are complete: the rate of a later step would
// read as a drop.
func attemptChart(points []taskmill.Activity, w window, now time.Time) chart {
	lines := []line{
		{name: "Completed", class: "state-completed", area: true, values: blank(w.steps)},
		{name: "Failed", class: "state-dead", values: blank(w.steps)},
	}
	minutes := w.step.Minutes()
	complete := w.complete(now)
	counting := false
	for _, p := range points {
		i, ok := w.index(p.At)
		switch {
		case !ok:
		case i >= complete:
			counting = true
		default:
			lines[0].values[i] = float64(p.Completed) / minutes
			lines[1].values[i] = float64(p.Failed) / minutes
		}
	}

	c := newChart("Attempts", " a minute", w, now, lines)
	if c.Note != "" && counting {
		c.Note = "The first point shows when the count of its " + stepName(w.step) + " is complete."
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

// settle is how long after a step ends its count of attempts may still grow:
// a worker writes the attempts it counted every 10 seconds, and the write
// takes a moment.
const settle = 15 * time.Second

// complete returns how many of the window's steps have a complete count of
// attempts at now. The last step is in progress, and the one before it is
// complete only settle after it ended: read sooner, it would pull the dial's
// needle and the end of the chart's line down for a moment at the start of
// every step. The dial and the chart of the attempts both go by this rule.
func (w window) complete(now time.Time) int {
	n := w.steps - 1
	if now.Sub(w.start.Add(time.Duration(n)*w.step)) < settle {
		n--
	}
	return n
}

// throughput returns the attempts a minute in the newest step whose count is
// complete, NaN if nothing was recorded in it, and the highest rate of the
// steps with a complete count.
func throughput(points []taskmill.Activity, w window, now time.Time) (rate, peak float64) {
	complete := w.complete(now)
	rate = math.NaN()
	for _, p := range points {
		i, ok := w.index(p.At)
		if !ok || i >= complete {
			continue
		}
		r := (float64(p.Completed) + float64(p.Failed)) / w.step.Minutes()
		peak = max(peak, r)
		if i == complete-1 {
			rate = r
		}
	}

	return rate, peak
}

// dial is the gauge of the attempts a minute. The server draws it as SVG,
// like the charts, in a view box that has the hub at its origin. So one
// rotation about the origin turns the needle and the arc of the value: 0
// degrees at the scale's zero, on the left, and 180 at its top, on the right.
// A browser animates a change of the rotation, and no shape changes.
type dial struct {
	Label        string // what a screen reader says
	Value, Unit  string // the rate, or a dash without one, and what it counts
	Angle        string // the rotation of the needle and the arc, in degrees
	Minor, Major string // SVG paths of the ticks
	Marks        []dialMark
}

// dialMark is a label of the dial's scale, and where its middle is.
type dialMark struct{ X, Y, Text string }

// The dial's measures, in the units of its view box. The template draws the
// track at 92 from the hub, and the needle to 73.
const (
	dialTicks   = 20   // steps of the scale; every fifth tick is major, with a label
	dialTickEnd = 86.0 // where a tick ends, short of the track
	dialMinor   = 81.0 // where a minor tick starts
	dialMajor   = 76.0 // where a major tick starts
	dialMarkAt  = 60.0 // the middle of a label
	dialEndDrop = 10.0 // the labels of zero and of the top stand this far below their ticks
)

// newDial draws the dial for rate, on the smallest round scale that holds
// peak, so the needle reads against the highest rate of the period. A rate
// that is not a number shows as no data, with the needle at rest on zero.
func newDial(rate, peak float64) dial {
	top := dialTop(peak)
	d := dial{Label: "Attempts a minute: no data", Value: "—", Unit: "attempts a minute", Angle: "0"}
	if !math.IsNaN(rate) {
		d.Value = formatValue(rate)
		if d.Value == "1" {
			d.Unit = "attempt a minute"
		}
		d.Label = d.Value + " " + d.Unit + ", on a dial from 0 to " + formatValue(top)
		d.Angle = coord(180 * min(max(rate/top, 0), 1))
	}

	var minor, major strings.Builder
	for i := range dialTicks + 1 {
		share := float64(i) / dialTicks
		if i%(dialTicks/4) != 0 {
			tick(&minor, share, dialMinor)
			continue
		}
		tick(&major, share, dialMajor)
		x, y := dialPoint(share, dialMarkAt)
		if i%dialTicks == 0 { // under its tick, clear of a needle that rests on zero or on the top
			x, _ = dialPoint(share, (dialMajor+dialTickEnd)/2)
			y = dialEndDrop
		}
		d.Marks = append(d.Marks, dialMark{coord(x), coord(y), short(top * share)})
	}

	d.Minor, d.Major = minor.String(), major.String()
	return d
}

// dialTop returns the top of the smallest round scale that holds peak: 1, 2,
// or 5 times a power of ten, and at least 10.
func dialTop(peak float64) float64 {
	if math.IsNaN(peak) || peak <= 10 {
		return 10
	}

	power := math.Pow(10, math.Floor(math.Log10(peak)))
	for _, m := range []float64{1, 2, 5} {
		if m*power >= peak {
			return m * power
		}
	}
	return 10 * power
}

// dialPoint returns the point at distance r from the hub where the dial
// shows share of its scale: 0 is on the left, a half at the top, and 1 on
// the right.
func dialPoint(share, r float64) (x, y float64) {
	sin, cos := math.Sincos(math.Pi * share)
	return -r * cos, -r * sin
}

// tick adds a tick at share of the scale to path, from the distance from
// to where every tick ends.
func tick(path *strings.Builder, share, from float64) {
	x1, y1 := dialPoint(share, from)
	x2, y2 := dialPoint(share, dialTickEnd)
	path.WriteString("M" + coord(x1) + " " + coord(y1) + "L" + coord(x2) + " " + coord(y2))
}

// coord formats a coordinate or an angle of the dial to a tenth, as short as
// it gets: "43.8", "-86", "0".
func coord(v float64) string {
	v = math.Round(v*10) / 10
	if v == 0 {
		return "0" // never "-0"
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// short formats a mark of the dial's scale in few characters: "250", "2.5",
// "1k", "1.25M".
func short(v float64) string {
	unit := ""
	switch {
	case v >= 1e6:
		v, unit = v/1e6, "M"
	case v >= 1e3:
		v, unit = v/1e3, "k"
	}
	return strconv.FormatFloat(v, 'f', -1, 64) + unit
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
