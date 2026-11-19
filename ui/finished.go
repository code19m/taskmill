package ui

import (
	"errors"
	"fmt"
	"maps"
	"net/url"
	"strings"
	"time"
)

// timeForms are the forms of a time that the address of a task list takes,
// in the "since" and "before" of a range of finish times: RFC 3339 with "Z"
// or an offset, to the second or to the minute, and the same without a zone,
// as a datetime-local field sends it.
var timeForms = []string{time.RFC3339, "2006-01-02T15:04Z07:00", "2006-01-02T15:04:05", "2006-01-02T15:04"}

// queryTime reads the time that the parameter name of q holds, in UTC; the
// zero time if it holds none. A time without a zone is in UTC: the time zone
// of the process never decides what an address selects. The time has whole
// seconds, so that the page can show all of it. In UTC, it is after the zero
// time, which tells the client "no bound", and its year has four digits, so
// that the links of the page can hold it.
func queryTime(q url.Values, name string) (time.Time, error) {
	v := q.Get(name)
	if v == "" {
		return time.Time{}, nil
	}
	// A query spells a space as "+". So the plus of an offset arrives as a
	// space, unless whoever wrote the address escaped it. No time has a
	// space of its own.
	v = strings.ReplaceAll(v, " ", "+")
	for _, form := range timeForms {
		t, err := time.Parse(form, v)
		if t = t.UTC(); err == nil && t.Nanosecond() == 0 && t.After(time.Time{}) && t.Year() <= 9999 {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("%s must be a time such as 2026-01-31T14:05Z or 2026-01-31T19:05:30+05:00", name)
}

// finishRange reads the range of finish times in the query of a task list:
// its two bounds, each the zero time if the query does not set it.
func finishRange(q url.Values) (since, before time.Time, err error) {
	if since, err = queryTime(q, "since"); err != nil {
		return since, before, err
	}
	if before, err = queryTime(q, "before"); err != nil {
		return since, before, err
	}
	if !since.IsZero() && !before.IsZero() && !since.Before(before) {
		return since, before, errors.New("the range of finish times must start before it ends")
	}
	return since, before, nil
}

// withRange returns the query q with the range of finish times from since to
// before in place of its own. A zero time is no bound. The query holds each
// bound as an instant in UTC, so its address shows the same tasks to every
// reader.
func withRange(q url.Values, since, before time.Time) url.Values {
	v := maps.Clone(q)
	v.Del("since")
	v.Del("before")
	if !since.IsZero() {
		v.Set("since", instant(since))
	}
	if !before.IsZero() {
		v.Set("before", instant(before))
	}
	return v
}

// instant formats t as an address takes it and as the script reads it: RFC
// 3339 in UTC, with milliseconds if t has any. It is empty for the zero time.
// The server words every time in UTC, and gives the time's instant beside
// the words, in a data-time attribute: the script words it again there, in
// the reader's time zone.
func instant(t time.Time) string {
	switch t = t.UTC(); {
	case t.IsZero():
		return ""
	case t.Nanosecond() < int(time.Millisecond):
		return t.Format(time.RFC3339)
	}
	return t.Format("2006-01-02T15:04:05.000Z")
}

// fieldTime formats t as a datetime-local field takes it, in UTC: to the
// minute, or to the second if t is not at a whole minute. It is empty for
// the zero time.
func fieldTime(t time.Time) string {
	switch t = t.UTC(); {
	case t.IsZero():
		return ""
	case t.Second() == 0:
		return t.Format("2006-01-02T15:04")
	}
	return t.Format("2006-01-02T15:04:05")
}

// presets are the ranges that the filter by finish time offers without
// typing: each is the time from so long ago until now.
var presets = []struct {
	label string
	last  time.Duration
}{
	{"Last hour", time.Hour},
	{"Last 6 hours", 6 * time.Hour},
	{"Last 24 hours", 24 * time.Hour},
	{"Last 7 days", 7 * 24 * time.Hour},
	{"Last 30 days", 30 * 24 * time.Hour},
}

// finishFilter is the filter of a task list by when its tasks finished: the
// range that is set, how its button names it, and the presets.
type finishFilter struct {
	Since, Before time.Time // the bounds of the range; zero for none
	Set           bool      // the range has a bound
	Label         string    // the range in short, on the button
	Full          string    // the range with its years and its zone, for the tooltip and for a screen reader
	Presets       []preset
}

// preset is a link to the list with a range that needs no typing.
type preset struct {
	Label, Href string
	Current     bool // it is the range of the list
}

// finishFilter returns the filter by finish time of the list with the query
// first, whose range is from since to before. A preset leads to the same
// list from a time ago, with no end, so the list stays open to the tasks that
// finish next. Its address holds that time as an instant: the server needs
// no time zone for it, and a link that is shared shows the same tasks. The
// time is rounded down to the minute, so the page changes once a minute, not
// with every update.
func (s *server) finishFilter(first url.Values, since, before, now time.Time) *finishFilter {
	f := &finishFilter{Since: since, Before: before, Set: !since.IsZero() || !before.IsZero()}
	f.Label, f.Full = rangeWords(since, before, now)
	for _, p := range presets {
		from := now.Add(-p.last).Truncate(time.Minute)
		f.Presets = append(f.Presets, preset{Label: p.label, Href: s.path(listPath(withRange(first, from, time.Time{})))})
	}
	whole := s.path(listPath(withRange(first, time.Time{}, time.Time{})))
	f.Presets = append(f.Presets, preset{Label: "Any time", Href: whole, Current: !f.Set})
	return f
}

// rangeWords says a range of finish times in UTC, twice: in short, as the
// button of the filter does, and in full, with the years and the zone. In
// short, a day of this year has no year, and a range within one day names
// the day once. A time has seconds only if it is not at a whole minute. The
// script says the same in the reader's time zone: its words follow these.
func rangeWords(since, before, now time.Time) (short, full string) {
	since, before = since.UTC(), before.UTC()
	clock := func(t time.Time) string {
		if t.Second() != 0 {
			return t.Format("15:04:05")
		}
		return t.Format("15:04")
	}
	brief := func(t time.Time) string {
		if t.Year() != now.UTC().Year() {
			return t.Format("2 Jan 2006 ") + clock(t)
		}
		return t.Format("2 Jan ") + clock(t)
	}
	long := func(t time.Time) string { return t.Format("2 Jan 2006 ") + clock(t) + " UTC" }
	switch {
	case since.IsZero() && before.IsZero():
		return "Any time", "Any time"
	case before.IsZero():
		return "Since " + brief(since), "Since " + long(since)
	case since.IsZero():
		return "Before " + brief(before), "Before " + long(before)
	}
	end := brief(before)
	if since.Format(time.DateOnly) == before.Format(time.DateOnly) {
		end = clock(before)
	}
	return brief(since) + " to " + end, long(since) + " to " + long(before)
}
