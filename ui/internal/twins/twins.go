// Package twins holds the cases that the dashboard's server and its script
// must word alike. The script, in ui/static, keeps the words that the
// server put on a page up to date, so each of its word functions has a twin
// in the server's Go: ago, span, and visible. The tests of package ui run
// the Go functions through these cases, and the browser tests, in
// ui/browsertest, run the script's.
package twins

import "time"

// Now is the time of rendering of the cases of [Ago].
var Now = time.Date(2026, time.November, 19, 12, 0, 0, 900_000_000, time.UTC)

// Ago are the words of a relative time: of a time Before [Now], or after it
// for a negative Before. Both times are read to the second, as a clock shows
// them, so every relative time of a page changes on the second.
var Ago = []struct {
	Before time.Duration
	Want   string
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
}

// Span are the words of a length of time: to the tenth of a second below a
// second, and to the second above it, in two units at most, each cut down,
// so "1.9s" is no whole second more than "1s". The script counts only the
// gaps that last until now, in whole seconds: it words the cases of whole
// seconds.
var Span = []struct {
	D    time.Duration
	Want string
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
	{2 * time.Second, "2s"},
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
}

// Visible are names with characters that no one could see, as a page shows
// them: a control character as its Unicode control picture where it has
// one, and any other, or a format character, as its code point.
var Visible = []struct {
	Name, Want string
}{
	{"plain", "plain"},
	{"orders\x1fkey", "orders␟key"}, // the separator of an outbox key
	{"\x00\t\n\x1b", "␀␉␊␛"},
	{"del\x7f", "del␡"},
	{"next\u0085line", "next<U+0085>line"}, // a control character with no picture
	{"mail\u202eliam", "mail<U+202E>liam"}, // reverses the text after it
	{"pay\u200bment", "pay<U+200B>ment"},   // has no width
	{"\ufeffbom", "<U+FEFF>bom"},
	{"tag\U000E0041", "tag<U+E0041>"},                                // a format character outside the first plane
	{"cafe\u0301 \U0001F600 \u2603", "cafe\u0301 \U0001F600 \u2603"}, // a combining mark, an emoji, and a symbol show as they are
}
