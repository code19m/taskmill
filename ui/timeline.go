package ui

import (
	"strconv"
	"time"

	"github.com/code19m/taskmill"
)

// dueSlack is how long after its creation a task may be due and still be
// due at once. A task enqueued without a due time is due at its creation to
// the microsecond, as one statement sets both. A due time that the
// application took from its own clock is a few milliseconds off, by that
// clock and the trip to the database, and one before the creation was due
// when the task was created. Within that slack, the two times are one time
// to the reader, and the wait is counted from the creation.
const dueSlack = 50 * time.Millisecond

// entry is a line of a task's timeline: a step, or the gap between two
// steps. A step is a fact, or a plan that is yet to come.
type entry struct {
	Step   string             // a step's name, such as "Started"; empty for a gap
	At     time.Time          // when the step was or is due; zero for one with no time yet
	Note   string             // a step's second line, such as "attempt 2"
	Plan   bool               // the step is yet to come
	Gap    string             // a gap's words, such as "Waited", or "Waiting, behind" the task in Behind
	Took   string             // a gap's length of time, such as "0.2s"; empty if it has none
	Work   bool               // the gap is an attempt that ran, not a wait: its line is solid, not dashed
	Since  time.Time          // for a gap that lasts until now, when it began: the script counts its length
	Behind *taskmill.TaskInfo // for a gap, the task that this one waits behind in its FIFO group
	Tone   string             // the color of a step's dot, or of a gap's line and length: "run", "ok", "err", or "warn"
}

// timeline returns the steps of task t at now, with the time between them:
// when it was created, due, started, and finished, from the times that its
// row keeps, and what is yet to come. head is the task that t waits behind
// in its FIFO group, if any.
//
// The row keeps the start of the last attempt only, and one due time, which
// a retry and a snooze both move: so the timeline shows the last attempt,
// and leaves out what the row cannot tell, such as whether a due time came
// from a retry, or how long a wait was when the due time of the wait is
// gone. A gap between two times out of order, as the clocks of two machines
// can make them, is left out too.
func timeline(now time.Time, t, head *taskmill.TaskInfo) []entry {
	s := steps{now: now, t: t, started: t.AttemptedAt}
	if t.Attempt == 0 {
		s.started = time.Time{} // a snooze or a stop gave the attempt back
	}

	switch t.State {
	case taskmill.StateScheduled:
		s.created()
	case taskmill.StatePending, taskmill.StateAvailable:
		s.waiting(head)
	case taskmill.StateRunning:
		due := s.created()
		s.gap("Waited", due, s.started, "")
		if s.start("run") {
			s.tl = append(s.tl, entry{Gap: "Running for", Took: s.lasting(s.started), Work: true, Since: s.started, Tone: "run"})
		}
	case taskmill.StateRetryable:
		s.retrying()
	case taskmill.StateCompleted:
		due := s.created()
		s.gap("Waited", due, s.started, "")
		s.start("")
		s.gap("Ran", s.started, t.FinalizedAt, "ok")
		s.step("Completed", t.FinalizedAt, "", "ok")
	case taskmill.StateDead:
		due := s.created()
		s.gap("Waited", due, s.started, "")
		s.start("")
		s.gap("Failed after", s.started, t.FinalizedAt, "err")
		s.step("Died", t.FinalizedAt, "", "err")
	case taskmill.StateCanceled:
		s.canceled()
	default:
	}

	return s.tl
}

// steps builds the timeline of a task.
type steps struct {
	now     time.Time
	t       *taskmill.TaskInfo
	started time.Time // the start of the last attempt that counts; zero if none
	tl      []entry
}

func (s *steps) step(name string, at time.Time, note, tone string) {
	s.tl = append(s.tl, entry{Step: name, At: at, Note: note, Tone: tone})
}

// plan adds a step that is yet to come until its time, or has no time yet.
func (s *steps) plan(name string, at time.Time, note string) {
	s.tl = append(s.tl, entry{Step: name, At: at, Note: note, Plan: at.IsZero() || at.After(s.now)})
}

// gap adds the time from one step to the next, unless a time is missing or
// the two are out of order. A gap that ends a start, Waited, is a wait; any
// other is the attempt that ran.
func (s *steps) gap(words string, from, to time.Time, tone string) {
	if !from.IsZero() && !to.IsZero() && !to.Before(from) {
		s.tl = append(s.tl, entry{Gap: words, Took: span(to.Sub(from)), Work: words != "Waited", Tone: tone})
	}
}

// lasting words the time from since until now, in whole seconds of the
// clock, as ago reads them.
func (s *steps) lasting(since time.Time) string {
	return span(max(0, s.now.Truncate(time.Second).Sub(since.Truncate(time.Second))))
}

// created adds the step Created and, unless the task was due at once, the
// step Due. It returns the time from which the task waited.
func (s *steps) created() time.Time {
	if s.t.RunAt.Sub(s.t.CreatedAt) <= dueSlack {
		s.step("Created", s.t.CreatedAt, "due at once", "")
		return s.t.CreatedAt
	}
	s.step("Created", s.t.CreatedAt, "", "")
	s.plan("Due", s.t.RunAt, "")
	return s.t.RunAt
}

// start adds the start of the last attempt, if one counts, and reports
// whether it did.
func (s *steps) start(tone string) bool {
	if s.started.IsZero() {
		return false
	}

	note := ""
	if s.t.Attempt > 1 {
		note = "attempt " + strconv.Itoa(s.t.Attempt)
	}
	s.step("Started", s.started, note, tone)
	return true
}

// failed returns when the last attempt failed, if its error ends the
// history: an error from its start on that is no note of a wait for a
// handler. It returns zero otherwise.
func (s *steps) failed() time.Time {
	e := lastError(s.t.Errors)
	if e == nil || s.started.IsZero() || e.At.Before(s.started) || e.Code == taskmill.CodeNoHandler {
		return time.Time{}
	}
	return e.At
}

// waiting adds the steps of a task that waits to start: once it is due, it
// waits, and in a FIFO group behind the task before it.
func (s *steps) waiting(head *taskmill.TaskInfo) {
	wait := entry{Behind: head}
	if due := s.created(); !due.After(s.now) {
		wait.Gap, wait.Took, wait.Since = "Waiting", s.lasting(due), due
	}
	if head != nil {
		wait.Tone = "warn"
		if wait.Gap == "" {
			wait.Gap = "Behind"
		} else {
			wait.Gap += ", behind"
		}
	}

	if wait.Took != "" || head != nil {
		s.tl = append(s.tl, wait)
	}
	s.plan("Start", time.Time{}, "")
}

// retrying adds the steps of a task that failed and runs again. Its due
// time is the next attempt's: the last one's is gone. A history of one
// error is that of a first attempt, which waited from the task's creation.
func (s *steps) retrying() {
	s.step("Created", s.t.CreatedAt, "", "")
	if len(s.t.Errors) == 1 {
		s.gap("Waited", s.t.CreatedAt, s.started, "")
	}
	s.start("")
	s.gap("Failed after", s.started, s.failed(), "err")
	s.plan("Due again", s.t.RunAt, "attempt "+strconv.Itoa(s.t.Attempt+1))
}

// canceled adds the steps that a canceled task reached: its due time if
// that came before the cancel, as the due time of its attempt if it came
// before the start, or else of the next attempt, after the start.
func (s *steps) canceled() {
	end := s.t.FinalizedAt
	reached := !s.t.RunAt.After(end)
	first := s.started.IsZero() || !s.t.RunAt.After(s.started)
	if reached && first {
		due := s.created()
		s.gap("Waited", due, s.started, "")
	} else {
		s.step("Created", s.t.CreatedAt, "", "")
	}

	if s.start("") {
		s.gap("Failed after", s.started, s.failed(), "err")
		if reached && !first {
			s.step("Due", s.t.RunAt, "", "")
		}
	}

	note := ""
	if !reached {
		note = "was due in " + span(s.t.RunAt.Sub(end))
	}
	s.step("Canceled", end, note, "")
}
