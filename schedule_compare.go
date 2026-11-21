package taskmill

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"maps"
	"math/big"
	"slices"
	"strconv"
	"sync"
	"time"
)

// This file holds how the maintainer compares the workers' schedules: the
// hash of each definition, the JSON values that it covers, and the watch
// that logs a difference that lasts.

// definitionHash returns a short hash of what decides when a run of schedule
// s happens, and which task it enqueues, which run holds resolved: the
// expression, the time zone, the queue, the kind, the priority, the most
// attempts, the FIFO group, and the payload, unless it varies. The payload
// counts as a JSON value, as jsonEqual compares it. A zone named Local is
// each process's own, so its offsets count too. A run's dedupe key and
// window follow from the schedule's name and timetable. The metadata is left
// out: it may hold values of each process, such as a trace's. A release that
// changes what the hash covers makes the schedules differ during its rolling
// deploy, which ends before the maintainer logs that.
func definitionHash(s *compiledSchedule, run *resolvedTask, varies bool) string {
	var b bytes.Buffer
	for _, field := range []string{
		s.Cron, zoneName(s.Location), run.queue, run.kind, strconv.Itoa(int(run.priority)), strconv.Itoa(run.maxAttempts), run.group,
	} {
		b.WriteString(strconv.Quote(field))
	}

	if !varies {
		var v any
		_ = decodeJSON(run.payload, &v) // never fails: resolve encoded the payload
		writeJSON(&b, v)
	}

	sum := sha256.Sum256(b.Bytes())
	return hex.EncodeToString(sum[:8])
}

// zoneName returns the name of loc, and for a zone named Local, which is the
// zone of each process, its offsets from UTC in January and in July, which
// tell most zones apart.
func zoneName(loc *time.Location) string {
	name := loc.String()
	if name != "Local" {
		return name
	}

	_, january := time.Date(2025, time.January, 15, 12, 0, 0, 0, time.UTC).In(loc).Zone()
	_, july := time.Date(2025, time.July, 15, 12, 0, 0, 0, time.UTC).In(loc).Zone()
	return name + " " + strconv.Itoa(january) + " " + strconv.Itoa(july)
}

// scheduleSums returns the names of schedules ss, sorted, and the hash of
// each one's definition, in the same order: what a worker stores of its
// schedules, so that the maintainer can compare them. Both are empty, not
// nil, for no schedules: the columns hold arrays.
func scheduleSums(ss []compiledSchedule) (names, hashes []string) {
	sorted := slices.SortedFunc(slices.Values(ss), func(a, b compiledSchedule) int { return cmp.Compare(a.Name, b.Name) })
	names, hashes = make([]string, len(sorted)), make([]string, len(sorted))
	for i, s := range sorted {
		names[i], hashes[i] = s.Name, s.hash
	}
	return names, hashes
}

// compareSchedules compares the schedules of the other live workers that
// take part in the maintainer election with this maintainer's, by name and
// by definition, and logs a difference that lasts: see scheduleWatch. It
// compares them also when this maintainer defines none, which is the worst
// case: then no schedule runs.
func (m *maintainer) compareSchedules(ctx context.Context) {
	term, ok := m.watch.current()
	if !ok {
		return
	}

	diff, err := m.otherSchedules(ctx)
	m.compareFailures.observe(ctx, err)
	if err == nil {
		m.watch.observe(ctx, m.w, diff, term)
	}
}

// otherSchedules returns how the schedules of up to 10 other live workers
// differ from this maintainer's.
func (m *maintainer) otherSchedules(ctx context.Context) (*scheduleDiff, error) {
	ctx, cancel := m.bound(ctx)
	defer cancel()
	r, err := m.c.pool.Query(ctx, m.c.sql.otherSchedules, m.w.scheduleNames, m.w.scheduleHashes)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	d := &scheduleDiff{missingHere: map[string]bool{}, missingThere: map[string]bool{}, definedDifferently: map[string]bool{}}
	for r.Next() {
		var id string
		var names, hashes []string
		if err := r.Scan(&id, &names, &hashes); err != nil {
			return nil, err
		}
		d.add(id, m.ownHashes, byName(names, hashes))
	}
	return d, r.Err()
}

// byName returns the hash of each schedule by its name. The table holds a
// hash for each name.
func byName(names, hashes []string) map[string]string {
	out := make(map[string]string, len(names))
	for i, name := range names {
		out[name] = hashes[i]
	}
	return out
}

// scheduleDiff says how the schedules of other workers differ from the
// maintainer's.
type scheduleDiff struct {
	workers []string // the workers whose schedules differ; none if they all match

	// The schedules that the workers define and the maintainer does not,
	// those that it defines and some of them do not, and those that both
	// define differently.
	missingHere, missingThere, definedDifferently map[string]bool
}

// add adds the worker id, whose schedules are theirs, to d if they differ
// from ours, the maintainer's. Both map each schedule's name to its hash.
func (d *scheduleDiff) add(id string, ours, theirs map[string]string) {
	differs := false
	for name, hash := range theirs {
		switch h, ok := ours[name]; {
		case !ok:
			d.missingHere[name], differs = true, true
		case h != hash:
			d.definedDifferently[name], differs = true, true
		}
	}
	for name := range ours {
		if _, ok := theirs[name]; !ok {
			d.missingThere[name], differs = true, true
		}
	}

	if differs {
		d.workers = append(d.workers, id)
	}
}

// schedulesLogged is how many names of each list of schedules a line names
// at most. It says how many more there are after them.
const schedulesLogged = 20

// attrs returns the attributes of a line about d, which has lasted for
// lasted.
func (d *scheduleDiff) attrs(lasted time.Duration) []slog.Attr {
	sorted := func(set map[string]bool) []string {
		names := slices.AppendSeq(make([]string, 0, len(set)), maps.Keys(set)) // not nil: none logs as []
		slices.Sort(names)
		if more := len(names) - schedulesLogged; more > 0 {
			names = append(names[:schedulesLogged], "and "+strconv.Itoa(more)+" more")
		}
		return names
	}
	return []slog.Attr{
		slog.Any(logKeyWorkers, d.workers),
		slog.Any(logKeyMissingHere, sorted(d.missingHere)),
		slog.Any(logKeyMissingThere, sorted(d.missingThere)),
		slog.Any(logKeyDefinedDifferently, sorted(d.definedDifferently)),
		slog.Duration(logKeyDuration, lasted),
	}
}

// scheduleWatch follows a difference between the schedules of other workers
// and the maintainer's. A rolling deploy that changes a schedule makes one
// for a few minutes, so it logs nothing until the difference has lasted for
// timing.scheduleGrace: then an error, again at most once per
// timing.scheduleReport while it lasts, and a line when it ends. It starts
// over in each term of the maintainer, and counts only the comparisons of
// the term: one that a term began and that ended after it may report what
// changed between the terms, such as a difference that ended, which is the
// next term's to find.
type scheduleWatch struct {
	mu     sync.Mutex
	term   int       // counts the terms of the maintainer
	open   bool      // the term lasts
	since  time.Time // when the comparison first found the difference; zero while there is none
	logged time.Time // when it was last logged; zero if not yet
}

// current returns the term that a comparison which starts now belongs to,
// and false between terms.
func (s *scheduleWatch) current() (term int, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.term, s.open
}

// observe takes the outcome of one comparison, which began in term. It
// drops the outcome if that term has ended.
func (s *scheduleWatch) observe(ctx context.Context, w *Worker, d *scheduleDiff, term int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.open || s.term != term {
		return
	}

	now := time.Now()
	switch {
	case len(d.workers) == 0:
		if !s.logged.IsZero() {
			w.logger.LogAttrs(ctx, slog.LevelInfo, "the schedules of other workers match the maintainer's again",
				slog.Duration(logKeyDuration, now.Sub(s.since)))
		}
		s.since, s.logged = time.Time{}, time.Time{}
	case s.since.IsZero():
		s.since = now
	case now.Sub(s.since) >= w.timing.scheduleGrace && now.Sub(s.logged) >= w.timing.scheduleReport:
		w.logger.LogAttrs(ctx, slog.LevelError, "the schedules of other workers differ from the maintainer's",
			d.attrs(now.Sub(s.since))...)
		s.logged = now
	}
}

// start begins a term without the difference of the last one, and without
// a line.
func (s *scheduleWatch) start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.term++
	s.open, s.since, s.logged = true, time.Time{}, time.Time{}
}

// end ends the term: the comparisons under way count for nothing.
func (s *scheduleWatch) end() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.open = false
}

// jsonEqual reports whether a and b hold the same JSON value, as PostgreSQL's
// jsonb compares them: numbers by their exact value, so that 0 equals -0 and
// 100 equals 1e2. jsonb stores numbers in its own form, so comparing their
// text, or their values as float64, would find a stored payload changed in
// every pass.
func jsonEqual(a, b []byte) bool {
	var x, y any
	return decodeJSON(a, &x) == nil && decodeJSON(b, &y) == nil && sameJSON(x, y)
}

// decodeJSON decodes JSON text into v, and keeps each number as its text.
func decodeJSON(text []byte, v *any) error {
	dec := json.NewDecoder(bytes.NewReader(text))
	dec.UseNumber()
	return dec.Decode(v)
}

// sameJSON reports whether two values from decodeJSON are equal.
func sameJSON(x, y any) bool {
	switch x := x.(type) {
	case json.Number:
		y, ok := y.(json.Number)
		return ok && (x == y || sameNumber(x, y))
	case []any:
		y, ok := y.([]any)
		return ok && slices.EqualFunc(x, y, sameJSON)
	case map[string]any:
		y, ok := y.(map[string]any)
		return ok && maps.EqualFunc(x, y, sameJSON)
	default: // a string, a bool, or null
		return x == y
	}
}

// writeJSON writes v, a value from decodeJSON, to b in one form for all the
// values that sameJSON finds equal: each number as its exact value, and the
// keys of each object in order.
func writeJSON(b *bytes.Buffer, v any) {
	switch v := v.(type) {
	case json.Number:
		if r, ok := new(big.Rat).SetString(v.String()); ok {
			b.WriteString("n" + r.Num().Text(16) + "/" + r.Denom().Text(16) + " ")
		} else {
			b.WriteString("t" + v.String() + " ") // a number that only its own text equals: see sameNumber
		}
	case []any:
		b.WriteString("[")
		for _, x := range v {
			writeJSON(b, x)
		}
		b.WriteString("]")
	case map[string]any:
		b.WriteString("{")
		for _, key := range slices.Sorted(maps.Keys(v)) {
			b.WriteString(strconv.Quote(key))
			writeJSON(b, v[key])
		}
		b.WriteString("}")
	case string:
		b.WriteString(strconv.Quote(v))
	case bool:
		b.WriteString(strconv.FormatBool(v) + " ")
	default: // null
		b.WriteString("null ")
	}
}

// sameNumber reports whether two JSON numbers have the same value. It is
// false for a number whose exponent is beyond what math/big reads, a million,
// which is far beyond what PostgreSQL stores.
func sameNumber(x, y json.Number) bool {
	p, okP := new(big.Rat).SetString(x.String())
	q, okQ := new(big.Rat).SetString(y.String())
	return okP && okQ && p.Cmp(q) == 0
}
