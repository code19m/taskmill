package ui

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/code19m/taskmill/internal/cron"
)

// cronCases are cron expressions with the words that the Schedules page
// shows for them: none for an expression that it shows alone.
var cronCases = []struct{ expr, want string }{
	// All day, at even steps.
	{"* * * * *", "Every minute"},
	{"*/1 * * * *", "Every minute"},
	{"*/2 * * * *", "Every 2 minutes"},
	{"*/5 * * * *", "Every 5 minutes"},
	{"*/30 * * * *", "Every 30 minutes"},
	{"0,30 * * * *", "Every 30 minutes"},
	{"0-59/30 * * * *", "Every 30 minutes"},
	{"0 * * * *", "Every hour"},
	{"@hourly", "Every hour"},
	{"15 * * * *", "Every hour at :15"},
	{"15,45 * * * *", "Every hour at :15 and :45"},
	{"5,25,45 * * * *", "Every hour at :05, :25, and :45"},
	{"0 */6 * * *", "Every 6 hours"},
	{"0 0,12 * * *", "Every 12 hours"},
	{"30 */8 * * *", "Every 8 hours at :30"},
	{"0,20 */2 * * *", "Every 2 hours at :00 and :20"},
	// A step that does not divide the hour or the day starts again at each
	// full hour, or at midnight: its times are listed while they are few.
	{"*/25 * * * *", "Every hour at :00, :25, and :50"},
	{"*/45 * * * *", "Every hour at :00 and :45"},
	{"0 */9 * * *", "Every day at 00:00, 09:00, and 18:00"},
	{"0 */16 * * *", "Every day at 00:00 and 16:00"},

	// At times of the day.
	{"30 9 * * *", "Every day at 09:30"},
	{"0 0 * * *", "Every day at 00:00"},
	{"@daily", "Every day at 00:00"},
	{"@midnight", "Every day at 00:00"},
	{"@DAILY", "Every day at 00:00"},
	{" 59\t23 * * * ", "Every day at 23:59"},
	{"0 9,17 * * *", "Every day at 09:00 and 17:00"},
	{"0,30 9 * * *", "Every day at 09:00 and 09:30"},
	{"0 6,14,22 * * *", "Every day at 06:00, 14:00, and 22:00"},

	// On days of the week.
	{"0 9 * * 1-5", "Weekdays at 09:00"},
	{"0 9 * * mon-fri", "Weekdays at 09:00"},
	{"0 10 * * 0,6", "Weekends at 10:00"},
	{"0 10 * * SAT,SUN", "Weekends at 10:00"},
	{"0 10 * * 6-7", "Weekends at 10:00"},
	{"0 0 * * 0", "Sundays at 00:00"},
	{"0 0 * * 7", "Sundays at 00:00"},
	{"@weekly", "Sundays at 00:00"},
	{"0 9 * * 2,4", "Tuesdays and Thursdays at 09:00"},
	{"0 10 * * 5,6", "Fridays and Saturdays at 10:00"},
	{"0 18 * * MON,WED,FRI", "Mondays, Wednesdays, and Fridays at 18:00"},
	{"0 9,17 * * 1-5", "Weekdays at 09:00 and 17:00"},

	// On days of the month.
	{"0 3 1 * *", "On the 1st of every month at 03:00"},
	{"@monthly", "On the 1st of every month at 00:00"},
	{"0 6 1,15 * *", "On the 1st and 15th of every month at 06:00"},
	{"0 0 2,3,4 * *", "On the 2nd, 3rd, and 4th of every month at 00:00"},
	{"0 0 11,12,13 * *", "On the 11th, 12th, and 13th of every month at 00:00"},
	{"0 0 21,22,23 * *", "On the 21st, 22nd, and 23rd of every month at 00:00"},
	{"0 0 28 * *", "On the 28th of every month at 00:00"},
	{"@yearly", "On the 1st of January at 00:00"},
	{"@annually", "On the 1st of January at 00:00"},
	{"0 0 1 JAN *", "On the 1st of January at 00:00"},
	{"0 0 29 2 *", "On the 29th of February at 00:00"},
	{"0 0 31 12 *", "On the 31st of December at 00:00"},
	{"0 12 30 4,6 *", "On the 30th of April and June at 12:00"},
	{"0 0 1 3,6,9 *", "On the 1st of March, June, and September at 00:00"},

	// A day field that matches every day, though it is not "*": then the
	// other day field does not narrow the days, as the parser reads it.
	{"0 0 1-31 * *", "Every day at 00:00"},
	{"0 0 * * 0-6", "Every day at 00:00"},
	{"0 0 */1 * 1", "Every day at 00:00"},
	{"0 0 1 * 0-7", "Every day at 00:00"},

	// Not an expression that the parser reads.
	{"", ""},
	{"* * * *", ""},
	{"* * * * * *", ""},
	{"60 * * * *", ""},
	{"@every 5m", ""},
	{"@reboot", ""},
	{"0 0 L * *", ""},
	{"0 0 15W * *", ""},
	{"0 0 ? * MON", ""},
	{"0 0 * * MON#1", ""},
	{"TZ=UTC 0 0 * * *", ""},
	{"every day at 09:00", ""},

	// More times, days, or months than a phrase lists.
	{"*/7 * * * *", ""},
	{"5/15 * * * *", ""},
	{"0 */5 * * *", ""},
	{"0 3/6 * * *", ""},
	{"0,15,30,45 9 * * *", ""},
	{"0 8,12,16,20 * * *", ""},
	{"0 9 * * 1-4", ""},
	{"0 9 * * 1-6", ""},
	{"0 9 * * 2-6", ""},
	{"0 0 */2 * *", ""},
	{"0 0 1,8,15,22 * *", ""},
	{"0 0 1 */3 *", ""},
	{"0 9,17 * * 1,4", ""},
	{"0 9,17 1,15 * *", ""},
	{"0 0 1,15 1,7 *", ""},

	// Even steps, but not all day, or not on every day.
	{"* 9 * * *", ""},
	{"*/15 9-17 * * *", ""},
	{"*/5 */6 * * *", ""},
	{"0 9-17 * * 1-5", ""},
	{"*/5 * * * 1-5", ""},
	{"0 * * * 0", ""},
	{"0 * 1 * *", ""},
	{"* * * 1 *", ""},

	// Every value of a field but one: that is not all of them.
	{"1-59 * * * *", ""},
	{"0 0-22 * * *", ""},
	{"0 0 2-31 * *", ""},
	{"0 0 * 1-11 *", ""},
	{"0 0 * * 1-6", ""},

	// A day of the month or a day of the week: two rules for the day.
	{"0 0 13 * 5", ""},
	{"0 0 1,15 * MON", ""},

	// A day that not every month of the phrase has.
	{"0 0 29 * *", ""},
	{"0 0 31 * *", ""},
	{"0 0 30 2 *", ""},
	{"0 0 31 4 *", ""},
	{"0 0 31 1,4 *", ""},

	// Every day, or days of the week, in some months only.
	{"0 0 * 1 *", ""},
	{"0 9 * 6-8 *", ""},
	{"0 9 * 1 1", ""},
}

func TestCronWords(t *testing.T) {
	t.Parallel()
	for _, c := range cronCases {
		if got := cronWords(c.expr); got != c.want {
			t.Errorf("cronWords(%q) = %q, want %q", c.expr, got, c.want)
		}
	}
}

// TestCronWordsNameTheRuns guards the rule that the words never say more or
// less than the expression: for every combination of these spellings of the
// five fields, the words, where there are any, name exactly the times that
// the schedule runs at. The spellings stand on both sides of each rule of
// cronWords.
func TestCronWordsNameTheRuns(t *testing.T) {
	t.Parallel()
	exprs := []string{""}
	for _, spellings := range [][]string{
		{"*", "*/15", "*/7", "30", "0,45"},      // minute
		{"*", "*/6", "*/5", "9", "0,9"},         // hour
		{"*", "1", "1,15", "29", "31", "1-31"},  // day of the month
		{"*", "2", "1,7", "*/3"},                // month
		{"*", "0", "1-5", "MON,WED,FRI", "*/1"}, // day of the week
	} {
		var longer []string
		for _, expr := range exprs {
			for _, spelling := range spellings {
				longer = append(longer, strings.TrimSpace(expr+" "+spelling))
			}
		}
		exprs = longer
	}
	for _, expr := range exprs {
		wordsNameTheRuns(t, expr)
	}
}

// FuzzCronWords checks, for any text in place of a cron expression, that
// cronWords does not panic, and that its words are short, plain, and true.
// Its seeds are the expressions of TestCronWords, so every test run also
// compares each of their phrases with the runs.
func FuzzCronWords(f *testing.F) {
	for _, c := range cronCases {
		f.Add(c.expr)
	}
	f.Fuzz(func(t *testing.T, expr string) { wordsNameTheRuns(t, expr) })
}

// A schedule's runs are compared with its words over a leap year: each of
// the first runs, more than a day of them for a schedule that runs every
// minute, and from then on the first run of every day. So a schedule that
// runs three times a day is compared at every run, and one that runs every
// minute on every day, in little time.
var (
	comparedFrom = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	comparedTo   = comparedFrom.AddDate(1, 0, 0)
)

const comparedEach = 1500

// plainWords matches the characters of a phrase.
var plainWords = regexp.MustCompile(`^[A-Za-z0-9:, ]*$`)

// wordsNameTheRuns fails the test if the words for expr are not short and
// plain, or if they do not name exactly the times that the parser's schedule
// runs at: a time that they name without a run, or a run that they do not
// name. It reads the times from the words alone, with said.
func wordsNameTheRuns(tb testing.TB, expr string) {
	tb.Helper()
	words := cronWords(expr)
	if len(words) > 60 || !plainWords.MatchString(words) {
		tb.Fatalf("cronWords(%q) = %q, want at most 60 letters, digits, spaces, commas, and colons", expr, words)
	}
	if words == "" {
		return
	}
	s, err := cron.Parse(expr)
	if err != nil {
		tb.Fatalf("cronWords(%q) = %q, but the parser refuses the expression: %v", expr, words, err)
	}
	onDay, times := said(tb, words)
	last, runs := comparedFrom.Add(-time.Minute), 0
	for day := comparedFrom; day.Before(comparedTo); day = day.AddDate(0, 0, 1) {
		if !onDay(day) {
			continue
		}
		for _, minutes := range times {
			named := day.Add(time.Duration(minutes) * time.Minute)
			run, err := s.Next(last)
			if err != nil || !run.Equal(named) {
				tb.Fatalf("cronWords(%q) = %q, which names %s after %s, but the schedule's next run is %s (error: %v)",
					expr, words, named, last, run, err)
			}
			last = run
			if runs++; runs >= comparedEach {
				last = day.Add(24*time.Hour - time.Minute) // on to the next day's first run
				break
			}
		}
	}
	if run, err := s.Next(last); err == nil && run.Before(comparedTo) {
		tb.Fatalf("cronWords(%q) = %q, which names no time from %s to %s, but the schedule runs at %s",
			expr, words, last, comparedTo, run)
	}
}

// The forms of a phrase, as said takes them apart.
var (
	everyMinutes = regexp.MustCompile(`^Every (?:(\d+) )?minutes?$`)
	everyHours   = regexp.MustCompile(`^Every (?:(\d+) )?hours?(?: at (.+))?$`)
	onMonthDays  = regexp.MustCompile(`^On the (.+) of (.+)$`)
	monthDay     = regexp.MustCompile(`^(\d+)(?:st|nd|rd|th)$`)
	listed       = regexp.MustCompile(`, and | and |, `)
)

// said returns what a phrase says, read from its words alone, as a reader of
// the page takes them: which days it names, and the times of such a day, in
// minutes from midnight and in the order of the words. It knows nothing of
// cron: "every 6 hours" is every sixth hour from midnight, whatever
// expression the words stand for. It fails the test for words that it
// cannot read, so every form of phrase needs its reading here.
func said(tb testing.TB, words string) (onDay func(time.Time) bool, times []int) {
	tb.Helper()
	everyDay := func(time.Time) bool { return true }
	if m := everyMinutes.FindStringSubmatch(words); m != nil {
		for t := 0; t < 24*60; t += saidStep(tb, m[1]) {
			times = append(times, t)
		}
		return everyDay, times
	}
	if m := everyHours.FindStringSubmatch(words); m != nil {
		marks := []string{":00"}
		if m[2] != "" {
			marks = listed.Split(m[2], -1)
		}
		for h := 0; h < 24; h += saidStep(tb, m[1]) {
			for _, mark := range marks {
				times = append(times, 60*h+saidTime(tb, ":04", mark))
			}
		}
		return everyDay, times
	}
	days, clocks, ok := strings.Cut(words, " at ")
	if !ok {
		tb.Fatalf("the words %q have no form that the test reads", words)
	}
	for _, clock := range listed.Split(clocks, -1) {
		times = append(times, saidTime(tb, "15:04", clock))
	}
	return saidDays(tb, days), times
}

// saidStep reads the number of "Every 6 hours": 1 where there is none.
func saidStep(tb testing.TB, text string) int {
	tb.Helper()
	if text == "" {
		return 1
	}
	n, err := strconv.Atoi(text)
	if err != nil || n < 2 {
		tb.Fatalf("the words say every %q of a unit", text)
	}
	return n
}

// saidTime reads a time of the day, or a minute of the hour, in minutes.
func saidTime(tb testing.TB, layout, text string) int {
	tb.Helper()
	at, err := time.Parse(layout, text)
	if err != nil {
		tb.Fatalf("the words name the time %q: %v", text, err)
	}
	return 60*at.Hour() + at.Minute()
}

// saidDays reads the days that a phrase names before its times.
func saidDays(tb testing.TB, days string) func(time.Time) bool {
	tb.Helper()
	weekend := func(t time.Time) bool { return t.Weekday() == time.Saturday || t.Weekday() == time.Sunday }
	switch days {
	case "Every day":
		return func(time.Time) bool { return true }
	case "Weekdays":
		return func(t time.Time) bool { return !weekend(t) }
	case "Weekends":
		return weekend
	}
	if m := onMonthDays.FindStringSubmatch(days); m != nil {
		return saidMonthDays(tb, m[1], m[2])
	}
	named := map[time.Weekday]bool{}
	for _, name := range listed.Split(days, -1) {
		day := time.Sunday
		for day <= time.Saturday && day.String()+"s" != name {
			day++
		}
		if day > time.Saturday {
			tb.Fatalf("the words name the days %q", name)
		}
		named[day] = true
	}
	return func(t time.Time) bool { return named[t.Weekday()] }
}

// saidMonthDays reads "the 1st and 15th" of "every month", or of months
// that the words name. A day of every month is one that each month has in
// every year, and a day of a named month is one that the month has in some
// year: it fails the test for words that name any other day.
func saidMonthDays(tb testing.TB, ordinals, of string) func(time.Time) bool {
	tb.Helper()
	year, months := 2025, map[time.Month]bool{}
	for month := time.January; month <= time.December; month++ {
		months[month] = of == "every month"
	}
	if of != "every month" {
		year = 2024 // a leap year
		for _, name := range listed.Split(of, -1) {
			months[saidMonth(tb, name)] = true
		}
	}
	days := map[int]bool{}
	for _, ordinal := range listed.Split(ordinals, -1) {
		m := monthDay.FindStringSubmatch(ordinal)
		if m == nil {
			tb.Fatalf("the words name the day %q of a month", ordinal)
		}
		day, _ := strconv.Atoi(m[1]) // digits, by the pattern
		days[day] = true
		for month := time.January; month <= time.December; month++ {
			if months[month] && time.Date(year, month, day, 0, 0, 0, 0, time.UTC).Month() != month {
				tb.Fatalf("the words name the %s of %s, a day that %s %d does not have", ordinal, of, month, year)
			}
		}
	}
	return func(t time.Time) bool { return days[t.Day()] && months[t.Month()] }
}

// saidMonth reads the name of a month.
func saidMonth(tb testing.TB, name string) time.Month {
	tb.Helper()
	for month := time.January; month <= time.December; month++ {
		if month.String() == name {
			return month
		}
	}
	tb.Fatalf("the words name the month %q", name)
	return 0
}
