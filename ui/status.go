package ui

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/code19m/taskmill"
)

// statusPage is what a status page shows under its title, in place of a page
// that the dashboard cannot show: a picture of what happened, what it means
// and what to do, in plain words, and where to go. The details of an error
// stay in the server log.
type statusPage struct {
	Glyph   string // the picture: a name of the "glyph" template
	Text    string
	Actions []link // the most useful first
	Retry   bool   // the cause may pass: with the script, the page tries its address again
}

// link is an action of a status page or of an empty list: where it leads,
// what it says, and the icon of the sprite before it.
type link struct {
	Href, Label, Icon string
}

// emptyNote is what an empty list says, in its card: a picture, what is not
// there, and why, or what may help, with a link that does, if any.
type emptyNote struct {
	Glyph, Title, Text string
	link
}

// status answers with a status page.
func (s *server) status(w http.ResponseWriter, r *http.Request, code int, title string, st statusPage) {
	s.render(w, r, code, "status.html", page{Title: title, Retry: st.Retry, Data: st})
}

// refuse answers a request that the dashboard refuses, and says why: a value
// that is not valid, or an action that does not fit.
func (s *server) refuse(w http.ResponseWriter, r *http.Request, code int, title, why string, back link) {
	s.status(w, r, code, title, statusPage{Glyph: "refused", Text: why, Actions: []link{back}})
}

func (s *server) notFound(w http.ResponseWriter, r *http.Request) {
	s.status(w, r, http.StatusNotFound, "Page not found", statusPage{
		Glyph:   "search",
		Text:    "Nothing exists at this address.",
		Actions: []link{{"/", "Back to the overview", "back"}},
	})
}

func (s *server) taskNotFound(w http.ResponseWriter, r *http.Request, id int64) {
	s.status(w, r, http.StatusNotFound, "Task not found", statusPage{
		Glyph:   "search",
		Text:    fmt.Sprintf("Task %d does not exist. It may have been deleted, or removed after its retention period.", id),
		Actions: []link{{"/tasks", "Back to the tasks", "back"}},
	})
}

// unreachablePage is the status page that the script shows in place of a
// page that got no answer from the dashboard: the network is down, the
// dashboard is, or a proxy in front of it answered in its place. Its action
// leads to the address that the page has then.
var unreachablePage = page{Title: "Can’t reach the dashboard", Data: statusPage{
	Glyph:   "offline",
	Text:    "No answer came from the dashboard. The network may be down, or the dashboard may be restarting.",
	Actions: []link{{"", "Try again", "retry"}}, Retry: true,
}}

// unreachable returns unreachablePage, which every page holds as a template.
func unreachable() page { return unreachablePage }

// backTo is the way back to where an action leads.
func backTo(target string) link {
	label := "Back to the overview"
	switch p, _, _ := strings.Cut(target, "?"); {
	case strings.HasPrefix(p, "/tasks/"):
		label = "Back to the task"
	case p == "/tasks":
		label = "Back to the tasks"
	case p == "/schedules":
		label = "Back to the schedules"
	}
	return link{target, label, "back"}
}

// reason words an error that the reader can act on as a sentence, without
// the name of its class: the title says that in plain words.
func reason(err error) string {
	text := err.Error()
	for _, class := range []error{taskmill.ErrInvalidArgument, taskmill.ErrInvalidState, taskmill.ErrConflict} {
		if _, after, ok := strings.Cut(text, class.Error()+": "); ok {
			text = after
		}
	}

	first, size := utf8.DecodeRuneInString(text)
	text = string(unicode.ToUpper(first)) + text[size:]
	if !strings.HasSuffix(text, ".") {
		text += "."
	}
	return text
}

// cursorHint tells the reader of a task list that the client refused, at an
// address with a cursor, where such a cursor comes from: each release makes
// its own, and so does each of a list's two orders.
const cursorHint = " A link or a bookmark from before an upgrade can hold a cursor that this list no longer takes."

// filtersHint tells the reader of a task list that ran out of time what may
// help. A list by state, kind, or FIFO group can read many tasks that it does
// not show: only some lists have an index that holds their tasks in the
// list's order.
const filtersHint = " A list with these filters can read many tasks that it does not show. " +
	"A queue and a state in the filters, or fewer filters, may help."

// unfinished tells the reader of an action that failed that it may have
// changed something before it did.
const unfinished = " The action did not finish: check the page before you try again."

// timeLimit reports whether the database ended a read at its time limit: the
// client's reads of tasks, groups, and activity set one of 5 seconds.
func timeLimit(err error) bool {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	return ok && pgErr.Code == "57014" // query_canceled
}

// cannotConnect reports whether err is a failure to connect to the database:
// it is down, out of reach, or refuses the dashboard's login. Then no
// statement ran.
func cannotConnect(err error) bool {
	_, ok := errors.AsType[*pgconn.ConnectError](err)
	return ok
}

// fail answers a request whose reads or action failed. A request whose
// client went away gets no answer, and is not an error of the dashboard.
func (s *server) fail(w http.ResponseWriter, r *http.Request, err error) {
	s.failWith(w, r, err, "", "")
}

// failWith is fail with a hint for the reader, shown if the request, or a
// read of it, ran out of time, and for an action, the page it leads back to.
// A request runs out of time at the deadline that the dashboard sets, or at
// an earlier one that the application set in the request's context: the
// answer is the same. Only a request that was canceled is one whose client
// went away.
//
// Each cause may pass, so the page tries again by itself, with the script:
// a page that the server cannot read now, it may read in a moment. The
// script waits longer after each failure, so that a page that fails for good
// costs a request a minute. An action is never sent again: its page tries
// the page that the action was sent from.
func (s *server) failWith(w http.ResponseWriter, r *http.Request, err error, hint, back string) {
	ctx := r.Context()
	actions := []link{{r.URL.RequestURI(), "Try again", "retry"}}
	if r.URL.Path != "/" {
		actions = append(actions, backTo("/"))
	}

	failed := "The dashboard could not read what this page shows. "
	if r.Method != http.MethodGet {
		if back == "" {
			back = "/"
		}
		actions, failed, hint = []link{backTo(back)}, "", unfinished
	}

	answer := func(code int, title, glyph, text string) {
		if code == http.StatusServiceUnavailable {
			w.Header().Set("Retry-After", "5")
		}
		s.status(w, r, code, title, statusPage{Glyph: glyph, Text: text + hint, Actions: actions, Retry: true})
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || timeLimit(err) {
		s.logger.LogAttrs(ctx, slog.LevelWarn, "dashboard request timed out",
			slog.String(logKeyPath, r.URL.Path), slog.Any(logKeyError, err))
		answer(http.StatusServiceUnavailable, "Taking too long", "slow", "The database did not answer in time.")
		return
	}

	if ctx.Err() != nil {
		return
	}

	s.logger.LogAttrs(ctx, slog.LevelWarn, "dashboard request failed",
		slog.String(logKeyPath, r.URL.Path), slog.Any(logKeyError, err))
	if cannotConnect(err) {
		answer(http.StatusServiceUnavailable, "Can’t reach the database", "database",
			"The dashboard is running, but it cannot connect to its database.")
		return
	}
	answer(http.StatusInternalServerError, "Something went wrong", "alert", failed+"The server log has the details.")
}
