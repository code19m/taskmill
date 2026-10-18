// Package cron parses standard five-field cron expressions and computes
// their next run times.
//
// An expression has five fields separated by spaces: minute (0-59), hour
// (0-23), day of month (1-31), month (1-12 or JAN-DEC), and day of week (0-7
// or SUN-SAT, where both 0 and 7 mean Sunday). Each field is "*", a value, a
// range "a-b", or a list of those separated by commas, and any of them may
// carry a step "/n". When both day fields are restricted, a day matches if
// either matches, as in classic cron. The macros @yearly, @annually,
// @monthly, @weekly, @daily, @midnight, and @hourly are also accepted.
package cron

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule is a parsed cron expression.
type Schedule struct {
	minute, hour, dom, month, dow uint64

	// domAny and dowAny record a "*" day field, which decides how the two
	// day fields combine.
	domAny, dowAny bool
}

type field struct {
	name     string
	min, max int
	names    map[string]int
}

var (
	minuteField = field{name: "minute", min: 0, max: 59}
	hourField   = field{name: "hour", min: 0, max: 23}
	domField    = field{name: "day of month", min: 1, max: 31}
	monthField  = field{name: "month", min: 1, max: 12, names: map[string]int{
		"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
		"JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
	}}
	dowField = field{name: "day of week", min: 0, max: 7, names: map[string]int{
		"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6,
	}}
)

var macros = map[string]string{
	"@yearly":   "0 0 1 1 *",
	"@annually": "0 0 1 1 *",
	"@monthly":  "0 0 1 * *",
	"@weekly":   "0 0 * * 0",
	"@daily":    "0 0 * * *",
	"@midnight": "0 0 * * *",
	"@hourly":   "0 * * * *",
}

// Parse parses a cron expression.
func Parse(expr string) (*Schedule, error) {
	expr = strings.TrimSpace(expr)
	if m, ok := macros[strings.ToLower(expr)]; ok {
		expr = m
	}
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron expression %q must have 5 fields, has %d", expr, len(fields))
	}

	var s Schedule
	var err error
	if s.minute, _, err = minuteField.parse(fields[0]); err != nil {
		return nil, err
	}
	if s.hour, _, err = hourField.parse(fields[1]); err != nil {
		return nil, err
	}
	if s.dom, s.domAny, err = domField.parse(fields[2]); err != nil {
		return nil, err
	}
	if s.month, _, err = monthField.parse(fields[3]); err != nil {
		return nil, err
	}
	if s.dow, s.dowAny, err = dowField.parse(fields[4]); err != nil {
		return nil, err
	}
	if s.dow&(1<<7) != 0 { // 7 is Sunday too
		s.dow = s.dow&^(1<<7) | 1
	}
	return &s, nil
}

// parse returns the bit set of values a field matches, and whether it is "*".
func (f field) parse(spec string) (bits uint64, star bool, err error) {
	for part := range strings.SplitSeq(spec, ",") {
		b, partStar, err := f.parsePart(part)
		if err != nil {
			return 0, false, err
		}
		bits |= b
		star = star || partStar
	}
	return bits, star && !strings.Contains(spec, ","), nil
}

func (f field) parsePart(part string) (bits uint64, star bool, err error) {
	rng, stepText, hasStep := strings.Cut(part, "/")
	step := 1
	if hasStep {
		step, err = strconv.Atoi(stepText)
		if err != nil || step < 1 {
			return 0, false, fmt.Errorf("invalid step %q in %s field", stepText, f.name)
		}
	}

	lo, hi := f.min, f.max
	switch {
	case rng == "*":
		star = !hasStep
	case strings.Contains(rng, "-"):
		a, b, _ := strings.Cut(rng, "-")
		if lo, err = f.value(a); err != nil {
			return 0, false, err
		}
		if hi, err = f.value(b); err != nil {
			return 0, false, err
		}
		if lo > hi {
			return 0, false, fmt.Errorf("range %q in %s field runs backward", rng, f.name)
		}
	default:
		if lo, err = f.value(rng); err != nil {
			return 0, false, err
		}
		if !hasStep {
			hi = lo
		}
	}
	for v := lo; v <= hi; v += step {
		bits |= 1 << v
	}
	return bits, star, nil
}

func (f field) value(text string) (int, error) {
	if v, ok := f.names[strings.ToUpper(text)]; ok {
		return v, nil
	}
	v, err := strconv.Atoi(text)
	if err != nil || v < f.min || v > f.max {
		return 0, fmt.Errorf("value %q is out of range for %s field (%d-%d)", text, f.name, f.min, f.max)
	}
	return v, nil
}

// errNoTime means the schedule matches no time within the search horizon,
// such as February 31.
var errNoTime = errors.New("cron schedule matches no time in the next five years")

// Next returns the first time after t that the schedule matches, in t's
// location. It fails if the schedule never matches, such as on February 31.
//
// The schedule is matched against the wall clock, so daylight saving time
// behaves as in classic cron: a time skipped when clocks go forward runs right
// after the jump, and a time repeated when clocks go back runs once.
func (s *Schedule) Next(t time.Time) (time.Time, error) {
	loc := t.Location()
	// wall is the wall clock in loc, held as a UTC time so that arithmetic
	// on it never meets a daylight saving transition.
	wall := civil(t).Add(time.Minute)
	for {
		match, err := s.nextCivil(wall)
		if err != nil {
			return time.Time{}, err
		}
		at := instant(match, loc)
		if at.After(t) {
			return at, nil
		}
		// The wall time occurred earlier: it is the repeat of a time that
		// clocks going back already passed.
		wall = match.Add(time.Minute)
	}
}

// instant returns when the wall-clock time c happens in loc. A time skipped
// when clocks go forward moves forward by the length of the jump, and a time
// that happens twice when clocks go back resolves to its first occurrence.
func instant(c time.Time, loc *time.Location) time.Time {
	at := time.Date(c.Year(), c.Month(), c.Day(), c.Hour(), c.Minute(), 0, 0, loc)
	if got := civil(at); !got.Equal(c) {
		return at.Add(c.Sub(got))
	}
	for _, back := range []time.Duration{time.Hour, 30 * time.Minute} {
		if earlier := at.Add(-back); civil(earlier).Equal(c) {
			return earlier
		}
	}
	return at
}

// civil returns t's wall clock as a UTC time.
func civil(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, time.UTC)
}

// nextCivil returns the first wall-clock time at or after c that matches,
// skipping whole months, days, and hours that cannot.
func (s *Schedule) nextCivil(c time.Time) (time.Time, error) {
	limit := c.Year() + 5
wrap:
	if c.Year() > limit {
		return time.Time{}, errNoTime
	}
	for s.month&(1<<uint(c.Month())) == 0 {
		c = time.Date(c.Year(), c.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		if c.Month() == time.January {
			goto wrap
		}
	}
	for !s.dayMatches(c) {
		c = time.Date(c.Year(), c.Month(), c.Day()+1, 0, 0, 0, 0, time.UTC)
		if c.Day() == 1 {
			goto wrap
		}
	}
	for s.hour&(1<<uint(c.Hour())) == 0 {
		c = c.Truncate(time.Hour).Add(time.Hour)
		if c.Hour() == 0 {
			goto wrap
		}
	}
	for s.minute&(1<<uint(c.Minute())) == 0 {
		c = c.Add(time.Minute)
		if c.Minute() == 0 {
			goto wrap
		}
	}
	return c, nil
}

func (s *Schedule) dayMatches(t time.Time) bool {
	dom := s.dom&(1<<uint(t.Day())) != 0
	dow := s.dow&(1<<uint(t.Weekday())) != 0
	if s.domAny || s.dowAny {
		return dom && dow
	}
	return dom || dow
}
