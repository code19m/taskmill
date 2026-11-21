package ui

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/code19m/taskmill"
)

// funcs returns the functions that the templates call, each by the name of
// its Go function.
func (s *server) funcs() template.FuncMap {
	return template.FuncMap{
		"path":        s.path,
		"asset":       s.asset,
		"ago":         ago,
		"agoMinutes":  agoMinutes,
		"timestamp":   timestamp,
		"instant":     instant,
		"fieldTime":   fieldTime,
		"prettyJSON":  prettyJSON,
		"lastError":   lastError,
		"newestFirst": newestFirst,
		"timeline":    timeline,
		"visible":     visible,
		"outbox":      outbox,
		"count":       count,
		"atLeast":     atLeast,
		"plural":      plural,
		"percent":     percent,
		"period":      period,
		"cronWords":   cronWords,
		"afterHost":   afterHost,
		"fit":         fit,
		"join":        strings.Join,
		"dict":        dict,
		"query":       query,
		"unreachable": unreachable,
		"lower":       strings.ToLower,
	}
}

// query returns the query of an address, from pairs of names and values: a
// value that is empty is left out, and so is the whole query if every
// value is.
func query(pairs ...string) string {
	v := url.Values{}
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] != "" {
			v.Set(pairs[i], pairs[i+1])
		}
	}

	if len(v) == 0 {
		return ""
	}
	return "?" + v.Encode()
}

// ago words how long before now t was, or how long after it: "8s ago", or
// "in 3m". It reads both times to the second, as a clock shows them, so
// every relative time of a page changes on the second, with the others. The
// script counts from there in the same words: see ago in app.js.
func ago(now, t time.Time) string {
	if t.IsZero() {
		return "—"
	}

	d, ahead := now.Unix()-t.Unix(), false
	if d < 0 {
		d, ahead = -d, true
	}

	n, unit := d/86400, "d"
	switch {
	case d == 0:
		return "now"
	case d < 60:
		n, unit = d, "s"
	case d < 3600:
		n, unit = d/60, "m"
	case d < 48*3600:
		n, unit = d/3600, "h"
	}

	if ahead {
		return "in " + strconv.FormatInt(n, 10) + unit
	}
	return strconv.FormatInt(n, 10) + unit + " ago"
}

// agoMinutes words t as ago does, but to the minute: "within a minute" for a
// time less than a minute from now, before or after it. It is for words that
// the script does not count, such as those of a tooltip: they change once a
// minute at most, so a page that holds them stays the page that a client
// has for the minute (see etag).
func agoMinutes(now, t time.Time) string {
	if d := now.Unix() - t.Unix(); d > -60 && d < 60 {
		return "within a minute"
	}
	return ago(now, t)
}

// span words a length of time at the scale of a handler's run:
// milliseconds below a second, as "35ms", tenths below ten seconds, as
// "1.3s", then "13s", "3m 12s", "2h 5m", and from two days, as ago, "3d 4h".
// Each part is cut down, never rounded up, as ago does, and a whole number
// of seconds has no tenths. Below a millisecond it is "<1ms", where "0ms"
// would read as no time; no time is "0s". A gap that counts is whole
// seconds, and the script counts it in the same words: see span in app.js.
func span(d time.Duration) string {
	switch {
	case d <= 0:
		return "0s"
	case d < time.Millisecond:
		return "<1ms"
	case d < time.Second:
		return strconv.Itoa(int(d/time.Millisecond)) + "ms"
	case d < 10*time.Second:
		tenths := int(d / (100 * time.Millisecond))
		if tenths%10 == 0 {
			return strconv.Itoa(tenths/10) + "s"
		}
		return strconv.Itoa(tenths/10) + "." + strconv.Itoa(tenths%10) + "s"
	}

	s := int64(d / time.Second)
	pair := func(n int64, unit string, rest int64, sub string) string {
		if rest == 0 {
			return strconv.FormatInt(n, 10) + unit
		}
		return strconv.FormatInt(n, 10) + unit + " " + strconv.FormatInt(rest, 10) + sub
	}
	switch {
	case s < 60:
		return pair(s, "s", 0, "")
	case s < 3600:
		return pair(s/60, "m", s%60, "s")
	case s < 48*3600:
		return pair(s/3600, "h", s/60%60, "m")
	}
	return pair(s/86400, "d", s/3600%24, "h")
}

func timestamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02 15:04:05.000 UTC")
}

// number formats n with thousands separators. It works on the digits, not
// on -n, which the smallest number does not have.
func number(n int64) string {
	sign, s := "", strconv.FormatInt(n, 10)
	if n < 0 {
		sign, s = "-", s[1:]
	}
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return sign + s
}

// count formats a count of tasks, marking one that stopped at its cap.
func count(n taskmill.TaskCount) string {
	if n.Capped {
		return number(n.N) + "+"
	}
	return number(n.N)
}

// atLeast is the tooltip of a count that stopped at its cap, and empty for
// any other count: it says what the plus after the number means.
func atLeast(n taskmill.TaskCount) string {
	if !n.Capped {
		return ""
	}
	return "At least " + number(n.N) + " tasks: the count stopped at its cap"
}

// period formats a retention period in whole days, or hours below two days.
func period(d time.Duration) string {
	if d >= 48*time.Hour {
		return strconv.Itoa(int(d/(24*time.Hour))) + " days"
	}
	if h := int(d / time.Hour); h != 1 {
		return strconv.Itoa(h) + " hours"
	}
	return "1 hour"
}

// plural returns one for a count of 1, and many for any other count.
func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// percent returns part of whole as a percentage, from 0 to 100.
func percent(part, whole int) int {
	if whole <= 0 {
		return 0
	}
	return min(100, max(0, part*100/whole))
}

// dict builds a map from key and value pairs, for passing several values to
// a template.
func dict(pairs ...any) map[string]any {
	m := make(map[string]any, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		if k, ok := pairs[i].(string); ok {
			m[k] = pairs[i+1]
		}
	}
	return m
}

// visible shows the characters of a name that no one could see: control
// characters, such as the separator that the outbox puts between a topic and
// a key, as their Unicode control pictures where they have one, and the rest
// of them, with the format characters, such as those that reverse the
// direction of text or have no width, as their code points, "<U+202E>". So a
// name cannot hide or reorder a part of itself, and two names that differ
// never look alike. The script's "visible" shows a name in the same way.
func visible(s string) string {
	if !strings.ContainsFunc(s, hidden) {
		return s
	}

	var b strings.Builder
	for _, r := range s {
		switch {
		case r < 0x20:
			b.WriteRune(0x2400 + r)
		case r == 0x7f:
			b.WriteRune('\u2421')
		case hidden(r):
			fmt.Fprintf(&b, "<U+%04X>", r)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// hidden reports whether visible shows r in place of itself.
func hidden(r rune) bool {
	return unicode.In(r, unicode.Cc, unicode.Cf)
}

// outboxKey is a FIFO group key that the outbox made, in its two parts.
type outboxKey struct{ Topic, Key string }

// String says the key in words, for a tooltip and a screen reader.
func (k outboxKey) String() string { return "Outbox topic " + k.Topic + ", key " + k.Key }

// outbox returns the parts of a FIFO group key that the outbox made, and nil
// for any other key. The outbox joins a message's topic and key with the
// unit separator, which no font draws (groupKey in package outbox): its key
// has one separator between two parts, and no other character that visible
// shows.
func outbox(group string) *outboxKey {
	topic, key, ok := strings.Cut(group, "\x1f")
	if !ok || topic == "" || key == "" || strings.ContainsFunc(topic, hidden) || strings.ContainsFunc(key, hidden) {
		return nil
	}
	return &outboxKey{topic, key}
}

// afterHost returns the part of a worker's ID after its host name and the
// dash that joins them: the process ID and the random suffix, which tell the
// workers of one host apart. It returns nothing for an ID that does not start
// with the host, or that has nothing after it.
func afterHost(id, host string) string {
	rest, ok := strings.CutPrefix(id, host+"-")
	if !ok || host == "" {
		return ""
	}
	return rest
}

// queuesRoom is how many characters of queue names a row of the Workers page
// has room for, at the width of its Queues column. The count of the names
// that it leaves out takes moreRoom of them.
const (
	queuesRoom = 34
	moreRoom   = 4
)

// fitted is the start of a list of names, for one line of a table.
type fitted struct {
	Line string // the first names, with commas between them
	More int    // how many names follow them
}

// fit returns the queues that a row of the Workers page shows, each as
// visible shows it: all of them if the row has room, or else the first ones
// that leave room for the count of the rest, and the first one at least. The
// row is one line tall, however many queues a worker reads, and the page
// lists them all on demand.
func fit(names []string) fitted {
	if len(names) == 0 {
		return fitted{}
	}

	const comma = ", "
	shown := make([]string, len(names))
	total := (len(names) - 1) * len(comma)
	for i, name := range names {
		shown[i] = visible(name)
		total += utf8.RuneCountInString(shown[i])
	}

	if total <= queuesRoom {
		return fitted{Line: strings.Join(shown, comma)}
	}

	n, used := 1, utf8.RuneCountInString(shown[0])
	for ; n < len(shown); n++ {
		used += len(comma) + utf8.RuneCountInString(shown[n])
		if used > queuesRoom-moreRoom {
			break
		}
	}
	return fitted{Line: strings.Join(shown[:n], comma), More: len(shown) - n}
}

// lastError returns the most recent attempt error, or nil.
func lastError(errs []taskmill.AttemptError) *taskmill.AttemptError {
	if len(errs) == 0 {
		return nil
	}
	return &errs[len(errs)-1]
}

// errorLine is an error of a task's history, as its page lists it.
type errorLine struct {
	taskmill.AttemptError
	Retried bool // the task was retried between the attempt before this one and this one
}

// newestFirst returns errs, newest first, and marks where the task was
// retried: a retry starts the count of attempts again, so an attempt's
// number that is not above the one before it follows a retry. A note, such
// as the wait for a handler, is no attempt, and counts for nothing here.
func newestFirst(errs []taskmill.AttemptError) []errorLine {
	out := make([]errorLine, len(errs))
	before := 0 // the number of the attempt before, in time
	for i, e := range errs {
		attempt := e.Attempt > 0 && e.Code != taskmill.CodeNoHandler
		out[len(errs)-1-i] = errorLine{AttemptError: e, Retried: attempt && before > 0 && e.Attempt <= before}
		if attempt {
			before = e.Attempt
		}
	}
	return out
}

// prettyJSON indents a payload for reading. It keeps the text as stored, so
// large integers stay exact, and characters such as "<" stay as they are: the
// template escapes them for HTML.
func prettyJSON(raw json.RawMessage) string {
	var b bytes.Buffer
	if err := json.Indent(&b, raw, "", "  "); err != nil {
		return string(raw)
	}
	return b.String()
}

// tasks says how many tasks n is: "1 task", "1,234 tasks", or "10,000+
// tasks" for a count that stopped at its cap.
func tasks(n taskmill.TaskCount) string {
	if n == (taskmill.TaskCount{N: 1}) {
		return "1 task"
	}
	return count(n) + " tasks"
}
