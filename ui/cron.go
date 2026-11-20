package ui

import (
	"fmt"
	"math/bits"
	"strconv"
	"strings"
	"time"

	"github.com/code19m/taskmill/internal/cron"
)

// Sets of a field's values, as the parser's sets of bits: all that a day or
// month field can match, and the two sets of week days that have a name.
const (
	everyMonthDay = 1<<32 - 1<<1 // 1 to 31
	everyMonth    = 1<<13 - 1<<1 // 1 to 12
	everyWeekDay  = 1<<7 - 1<<0  // Sunday, 0, to Saturday, 6
	workDays      = 1<<6 - 1<<1  // Monday to Friday
	weekend       = 1<<6 | 1<<0  // Saturday and Sunday
)

// maxListed is how many things a phrase may list in all, such as times of
// the day or days of the week. More of them are easier to read in the
// expression.
const maxListed = 3

// daily starts the phrase of a schedule that runs on every day.
const daily = "Every day"

// monthLength is the length of each month, with February as in a leap year.
var monthLength = [...]int{31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

// cronWords returns what a schedule's cron expression means, in a few
// English words: "Weekdays at 09:00". The times are on the clock of the
// schedule's time zone. It returns "" for an expression that has no exact
// and short words, and the page then shows the expression alone.
//
// The words never say more or less than the expression does. They come from
// the parser that runs the schedules, and from the values that each field
// matches, not from the expression's text: so "*/30", "0,30", and "0-59/30"
// read the same, and an expression that the parser refuses has no words.
// "Every 15 minutes" stands only for a step that divides the hour: "*/7"
// starts again at every full hour, so it is not every 7 minutes. A day of
// the month is named only if every month of the phrase has it.
func cronWords(expr string) string {
	s, err := cron.Parse(expr)
	if err != nil {
		return ""
	}

	days, listed := dayWords(s)
	if days == daily {
		if repeat := repeatWords(s); repeat != "" {
			return repeat
		}
	}

	minutes, hours := values(s.Minute), values(s.Hour)
	if days == "" || listed*len(hours)*len(minutes) > maxListed {
		return ""
	}

	times := make([]int, 0, maxListed)
	for _, h := range hours {
		for _, m := range minutes {
			times = append(times, 60*h+m)
		}
	}

	return days + " at " + list(times, func(t int) string { return fmt.Sprintf("%02d:%02d", t/60, t%60) })
}

// repeatWords returns the words of a schedule that runs all day at even
// steps, on every day: every minute or every few minutes, or every hour or
// every few hours at up to three minutes of the hour. It returns "" for
// any other schedule.
func repeatWords(s *cron.Schedule) string {
	hourStep, minuteStep := step(s.Hour, 24), step(s.Minute, 60)
	if hourStep == 0 || hourStep == 24 {
		return "" // it runs at set hours of the day
	}
	if hourStep == 1 && minuteStep > 0 && minuteStep < 60 {
		return every(minuteStep, "minute")
	}

	marks := values(s.Minute)
	if len(marks) > maxListed {
		return ""
	}

	words := every(hourStep, "hour")
	if minuteStep == 60 {
		return words // on the hour
	}
	return words + " at " + list(marks, func(m int) string { return fmt.Sprintf(":%02d", m) })
}

// dayWords returns the days that s runs on, as the start of a phrase, and
// how many things the words list. It returns "" for days that a list does
// not name: those of two rules, or of a rule for some months only.
func dayWords(s *cron.Schedule) (words string, listed int) {
	byMonthDay, byWeekDay := s.Dom != everyMonthDay, s.Dow != everyWeekDay
	if s.EitherDay && byMonthDay != byWeekDay {
		byMonthDay, byWeekDay = false, false // the other field matches every day
	}

	var named []int // the months, if the schedule does not run in all of them
	if s.Month != everyMonth {
		named = values(s.Month)
	}

	switch {
	case byMonthDay && byWeekDay:
		return "", 0 // two rules for the day
	case byMonthDay:
		return monthDayWords(s.Dom, named)
	case len(named) > 0:
		return "", 0 // every day, or some days of the week, in some months
	case !byWeekDay:
		return daily, 1
	case s.Dow == workDays:
		return "Weekdays", 1
	case s.Dow == weekend:
		return "Weekends", 1
	}
	days := values(s.Dow)
	return list(days, func(d int) string { return time.Weekday(d).String() + "s" }), len(days)
}

// monthDayWords returns the words for the days dom of every month, or of
// the named months if there are any: "On the 1st and 15th of every month".
// It returns "" if one of those months does not have one of the days: the
// 31st of every month would be a run in 7 months of the year, not in all of
// them. Every month has 28 days in every year. A named month counts with
// its length in a leap year: the 29th of February is a day that everyone
// knows to be rare.
func monthDayWords(dom uint64, named []int) (words string, listed int) {
	last, of := 28, "every month"
	if len(named) > 0 {
		last, of = 31, list(named, func(m int) string { return time.Month(m).String() })
	}
	for _, m := range named {
		last = min(last, monthLength[m-1])
	}

	if bits.Len64(dom)-1 > last {
		return "", 0
	}
	return "On the " + list(values(dom), ordinal) + " of " + of, bits.OnesCount64(dom) * max(1, len(named))
}

// step returns n if set holds every n-th of the values from 0 to size-1, and
// n divides size: the values are then n apart around the clock as well, from
// the last one to the first. It returns 0 for any other set.
func step(set uint64, size int) int {
	held := bits.OnesCount64(set)
	if held == 0 || size%held != 0 {
		return 0
	}

	n := size / held
	for v := 0; v < size; v += n {
		if set&(1<<v) == 0 {
			return 0
		}
	}
	return n
}

// values returns the members of a set of bits, in order.
func values(set uint64) []int {
	out := make([]int, 0, bits.OnesCount64(set))
	for ; set != 0; set &= set - 1 {
		out = append(out, bits.TrailingZeros64(set))
	}
	return out
}

// every returns "Every hour" for one unit, and "Every 6 hours" for more.
func every(n int, unit string) string {
	if n == 1 {
		return "Every " + unit
	}
	return "Every " + strconv.Itoa(n) + " " + unit + "s"
}

// list writes values as a sentence lists them: "a", "a and b", or
// "a, b, and c".
func list(items []int, word func(int) string) string {
	words := make([]string, len(items))
	for i, item := range items {
		words[i] = word(item)
	}
	if n := len(words); n > 2 {
		return strings.Join(words[:n-1], ", ") + ", and " + words[n-1]
	}
	return strings.Join(words, " and ")
}

// ordinal returns a day of the month as it is read: "1st", "22nd", "13th".
func ordinal(day int) string {
	suffix := "th"
	if day/10 != 1 { // not 11th, 12th, or 13th
		switch day % 10 {
		case 1:
			suffix = "st"
		case 2:
			suffix = "nd"
		case 3:
			suffix = "rd"
		}
	}
	return strconv.Itoa(day) + suffix
}
