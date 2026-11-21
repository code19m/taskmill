package ui

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/code19m/taskmill"
)

// maxChoices is the most values of one filter that a request may choose.
const maxChoices = 100

// listedStates are the states the task list can filter by, in lifecycle
// order.
var listedStates = []taskmill.State{
	taskmill.StateScheduled, taskmill.StatePending, taskmill.StateAvailable, taskmill.StateRunning,
	taskmill.StateRetryable, taskmill.StateCompleted, taskmill.StateDead, taskmill.StateCanceled,
}

// listedStateNames are the names of listedStates: the options of the filter
// by state.
var listedStateNames = func() []string {
	names := make([]string, len(listedStates))
	for i, st := range listedStates {
		names[i] = string(st)
	}
	return names
}()

// taskFilter reads the task list's filters from a query.
func taskFilter(q url.Values) taskmill.TaskFilter {
	f := taskmill.TaskFilter{Queues: selected(q["queue"]), Kinds: selected(q["kind"]), GroupKeys: selected(q["group"])}
	for _, st := range selected(q["state"]) {
		f.States = append(f.States, taskmill.State(st))
	}
	return f
}

// filterQuery is the query of a task filter, for links and redirects.
func filterQuery(f taskmill.TaskFilter) url.Values {
	v := url.Values{"queue": f.Queues, "kind": f.Kinds, "group": f.GroupKeys}
	for _, st := range f.States {
		v.Add("state", string(st))
	}
	return v
}

// filter is a multiple choice filter of the task list.
type filter struct {
	Name    string // the query parameter
	Title   string // what it filters by
	Plural  string // what it chooses from
	Label   string // what the trigger shows: all, the one choice, or how many
	Empty   bool   // nothing is chosen
	Search  bool   // its menu has a search box, which can also add any value
	Source  string // where the search finds choices the menu does not list
	Options []option
}

func (f filter) searchable() filter {
	f.Search = true
	return f
}

type option struct {
	Value    string
	Selected bool
}

// newFilter lists the chosen values that are not among all options first,
// such as a queue that no longer has tasks or a value typed in the search,
// then all options.
func newFilter(name, title, plural string, all, chosen []string) filter {
	f := filter{Name: name, Title: title, Plural: plural, Empty: len(chosen) == 0}
	for _, v := range chosen {
		if !slices.Contains(all, v) {
			f.Options = append(f.Options, option{v, true})
		}
	}
	for _, v := range all {
		f.Options = append(f.Options, option{v, slices.Contains(chosen, v)})
	}

	switch len(chosen) {
	case 0:
		f.Label = "All " + plural
	case 1:
		f.Label = chosen[0]
	default:
		f.Label = strconv.Itoa(len(chosen)) + " " + plural
	}
	return f
}

// selected returns the non-empty values of a repeated query parameter,
// without duplicates, up to maxChoices of them.
func selected(values []string) []string {
	var out []string
	for _, v := range values[:min(len(values), maxChoices)] {
		if v != "" && !slices.Contains(out, v) {
			out = append(out, v)
		}
	}
	return out
}

// groupChoices is how many FIFO groups a filter lists; it searches the rest.
const groupChoices = 100

// choices are the kinds and FIFO groups that the task filters offer: the
// kinds that workers handle or schedules enqueue, and the first groups with
// unfinished tasks.
type choices struct {
	kinds, groups []string
	moreGroups    bool // there are more groups than listed
}

// choiceCache keeps the filter choices until a time. They change rarely, and
// every open task list would read them again with each live update.
type choiceCache struct {
	mu    sync.Mutex
	until time.Time
	last  choices
}

// Choices read in full are kept for choicesTTL, and those of a read that
// failed for choicesRetry: while the database fails, each task list would
// otherwise add three reads that fail to its own.
const (
	choicesTTL   = 30 * time.Second
	choicesRetry = 5 * time.Second
)

// filterChoices returns the filter choices, read at most choicesTTL ago, or
// choicesRetry ago if a read failed. A read that the end of its request cut
// short is not kept.
func (s *server) filterChoices(r *http.Request) choices {
	s.choices.mu.Lock()
	if time.Now().Before(s.choices.until) {
		c := s.choices.last
		s.choices.mu.Unlock()
		return c
	}
	s.choices.mu.Unlock()

	c, complete := s.readChoices(r)
	if r.Context().Err() != nil {
		return c
	}

	keep := choicesTTL
	if !complete {
		keep = choicesRetry
	}
	s.choices.mu.Lock()
	s.choices.until, s.choices.last = time.Now().Add(keep), c
	s.choices.mu.Unlock()
	return c
}

// readChoices reads the filter choices, and reports whether every read
// succeeded. The filters are not worth failing the page for: without their
// choices, they offer what is chosen.
func (s *server) readChoices(r *http.Request) (c choices, complete bool) {
	ctx := r.Context()
	complete = true
	warn := func(what string, err error) {
		complete = false
		if ctx.Err() == nil {
			s.logger.LogAttrs(ctx, slog.LevelWarn, "read filter choices failed",
				slog.String(logKeyTarget, what), slog.Any(logKeyError, err))
		}
	}
	if workers, err := s.client.Workers(ctx); err != nil {
		warn("workers", err)
	} else {
		for _, w := range workers {
			c.kinds = append(c.kinds, w.Kinds...)
		}
	}
	if schedules, err := s.client.Schedules(ctx); err != nil {
		warn("schedules", err)
	} else {
		for _, sc := range schedules {
			c.kinds = append(c.kinds, sc.Kind)
		}
	}
	slices.Sort(c.kinds)
	c.kinds = slices.Compact(c.kinds)

	found, err := s.client.Groups(ctx, &taskmill.GroupParams{Limit: groupChoices})
	if err != nil {
		warn("groups", err)
		found = &taskmill.GroupsResult{}
	}
	for _, g := range found.Groups {
		c.groups = append(c.groups, g.Key)
	}
	slices.Sort(c.groups)
	c.groups, c.moreGroups = slices.Compact(c.groups), found.Next != ""

	return c, complete
}

// groupChoicesJSON lists the keys of the FIFO groups that match a search, for
// the group filter when it has more groups than it lists.
func (s *server) groupChoicesJSON(w http.ResponseWriter, r *http.Request) {
	found, err := s.client.Groups(r.Context(), &taskmill.GroupParams{Search: r.URL.Query().Get("search"), Limit: groupChoices})
	if errors.Is(err, taskmill.ErrInvalidArgument) {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err != nil {
		if r.Context().Err() == nil {
			s.logger.LogAttrs(r.Context(), slog.LevelWarn, "search groups failed", slog.Any(logKeyError, err))
		}
		http.Error(w, "taskmill: search failed", http.StatusServiceUnavailable)
		return
	}

	keys := make([]string, 0, len(found.Groups))
	for _, g := range found.Groups {
		keys = append(keys, g.Key)
	}
	slices.Sort(keys)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(slices.Compact(keys)) // the client may be gone; there is no one to tell
}
