package taskmill

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/code19m/taskmill/internal/recovered"
)

// runOne runs a single attempt and records its outcome, on record: a
// context that outlives parent, which ends the attempt.
func (w *Worker) runOne(parent, record context.Context, t *liveAttempt) {
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	// A handler or middleware that ends its goroutine with runtime.Goexit, as
	// t.Fatal does in a test, skips the end of the attempt: its task goes
	// back to its queue. After any other end, the attempt is finished, and
	// this does nothing.
	defer w.finish(record, t, outcome{kind: outcomeRelease}, time.Time{})

	t.mu.Lock()
	t.cancel = cancel
	t.mu.Unlock()

	if t.att.cancelRequested.Load() {
		cancel(ErrTaskCanceled)
	}
	if w.leaseOver(parent, t) {
		// The task may run on another worker already, as after a slow fetch:
		// no code of the application starts on it here. A loss after this
		// check cancels ctx, as t.cancel is set.
		w.finish(record, t, outcome{kind: outcomeDiscard}, time.Time{})
		return
	}

	if t.invalid != nil {
		// No code of the application sees a task whose row the worker cannot
		// trust: not its handler, and not the middleware, whose result would
		// decide what becomes of the task.
		w.finish(record, t, outcome{kind: outcomeDead, err: t.invalid, msg: t.invalid.Error(), code: CodeInvalidRow}, time.Time{})
		return
	}
	if t.reg == nil {
		w.finish(record, t, w.unknownKind(ctx, t), time.Time{})
		return
	}

	w.counts.started.Add(1)
	ctx, cancelTimeout := context.WithTimeoutCause(ctx, t.reg.opts.Timeout, ErrAttemptTimeout)
	defer cancelTimeout()
	start := time.Now()
	stopWatch := w.watch(ctx, record, []*liveAttempt{t}, start)
	defer stopWatch()
	err := w.invoke(ctx, t, func(ctx context.Context) error { return t.reg.run(ctx, t.info, t.att) })
	stopWatch()
	w.finish(record, t, outcomeOf(t, err, context.Cause(ctx)), start)
}

// runBatch runs a batch handler for tasks, and records their outcomes on
// record, as runOne does. Middleware still wraps each task: each one's chain
// runs in its own goroutine, and the batch starts once every chain has
// either reached the handler or returned early.
func (w *Worker) runBatch(parent, record context.Context, tasks []*liveAttempt) {
	// As in runOne: a task whose lease ended before the batch starts does
	// not reach the handler, so that a relay does not publish its message
	// again.
	tasks = slices.DeleteFunc(tasks, func(t *liveAttempt) bool {
		if !w.leaseOver(parent, t) {
			return false
		}
		w.finish(record, t, outcome{kind: outcomeDiscard}, time.Time{})
		return true
	})
	if len(tasks) == 0 {
		return
	}

	w.counts.started.Add(int64(len(tasks)))
	reg := tasks[0].reg
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	ctx, cancelTimeout := context.WithTimeoutCause(ctx, reg.opts.Timeout, ErrAttemptTimeout)
	defer cancelTimeout()
	// As in runOne: tasks whose chain or batch handler ended its goroutine go
	// back to their queue.
	defer w.finishAll(record, tasks, func(int) outcome { return outcome{kind: outcomeRelease} }, time.Time{})

	c := &batchChains{
		arrivals: make(chan arrival, len(tasks)),
		early:    make(chan struct{}, len(tasks)),
		ended:    make(chan struct{}),
		errs:     make([]error, len(tasks)),
	}
	defer close(c.ended)
	start := time.Now()
	stopWatch := w.watch(ctx, record, tasks, start)
	defer stopWatch()

	for i, t := range tasks {
		c.running.Go(func() { w.runChain(ctx, record, c, i, t) })
	}

	var arrived []arrival
	for pending := len(tasks); pending > 0; pending-- {
		select {
		case a := <-c.arrivals:
			arrived = append(arrived, a)
		case <-c.early:
		}
	}

	if len(arrived) > 0 {
		slices.SortFunc(arrived, func(a, b arrival) int { return cmp.Compare(a.index, b.index) })
		infos := make([]*TaskInfo, len(arrived))
		atts := make([]*attempt, len(arrived))
		for j, a := range arrived {
			infos[j], atts[j] = tasks[a.index].info, tasks[a.index].att
		}

		results := safeBatch(ctx, reg, infos, atts)
		for j, a := range arrived {
			a.result <- results[j]
		}
	}
	c.running.Wait()
	stopWatch()

	cause := context.Cause(ctx)
	w.finishAll(record, tasks, func(i int) outcome { return outcomeOf(tasks[i], c.errs[i], cause) }, start)
}

// batchChains are the middleware chains of the tasks of a batch.
type batchChains struct {
	arrivals chan arrival   // a chain reached the handler
	early    chan struct{}  // a chain returned without reaching it
	ended    chan struct{}  // closed when runBatch returns
	errs     []error        // what each chain returned
	running  sync.WaitGroup // the chains
}

// arrival is a chain of a batch that reached the handler, at index of the
// batch's tasks, and waits for its result.
type arrival struct {
	index  int
	result chan error
}

// runChain runs the middleware chain of task t, at index i of a batch. At
// the handler, it waits for the batch's result for t. A batch handler that
// ends its goroutine leaves it without one: its next returns errBatchEnded.
func (w *Worker) runChain(ctx, record context.Context, c *batchChains, i int, t *liveAttempt) {
	var reached, returned atomic.Bool
	defer func() {
		if !returned.Load() { // the chain ended its goroutine
			w.finish(record, t, outcome{kind: outcomeRelease}, time.Time{})
		}
		if !reached.Load() {
			c.early <- struct{}{}
		}
	}()

	c.errs[i] = w.invoke(ctx, t, func(context.Context) error {
		reached.Store(true)
		a := arrival{index: i, result: make(chan error, 1)}
		c.arrivals <- a
		select {
		case err := <-a.result:
			return err
		case <-c.ended:
			return errBatchEnded
		}
	})
	returned.Store(true)
}

// watch abandons the attempts ts if their handler has not returned
// shutdownGrace after ctx, the handler's context, ended for another reason
// than a shutdown, which the drain handles: a handler that ignores its
// context would otherwise keep its tasks for good. In a queue with a
// MaxRunning limit it only warns, unless the lease was lost. The rules are
// those of [HandlerOptions.Timeout]. The returned stop must be called once
// the handler returned.
func (w *Worker) watch(ctx, record context.Context, ts []*liveAttempt, start time.Time) (stop func()) {
	h := &handlerWatch{w: w, record: record, ts: ts, start: start, q: w.queues[ts[0].info.Queue]}
	if ts[0].direct {
		h.q = nil // RunTask's attempts take no slot
	}

	stopAfter := context.AfterFunc(ctx, func() {
		cause := context.Cause(ctx)
		if errors.Is(cause, ErrShutdown) {
			return
		}

		// stop may have run since the context ended, and found no timer: it
		// marks the watch finished before it reads the timer, so one of the
		// two sees the other.
		grace := time.AfterFunc(w.timing.shutdownGrace, func() { h.expire(cause) })
		h.grace.Store(grace)
		if h.state.Load() == watchFinished {
			grace.Stop()
		}
	})
	return func() { h.stop(stopAfter) }
}

// handlerWatch is what watch keeps of one handler call. Its state goes from
// watching to stuck to finished, or from watching to finished: the slot of
// Run's attempts counts as stuck from the end of the grace until the
// handler returns, for Stats and Health.
type handlerWatch struct {
	w      *Worker
	record context.Context
	ts     []*liveAttempt
	start  time.Time
	q      *queueState // nil for RunTask's attempts
	state  atomic.Int32
	grace  atomic.Pointer[time.Timer] // set once the handler's context ended
}

// The states of a handlerWatch.
const (
	watchWatching int32 = iota
	watchStuck
	watchFinished
)

// expire abandons the attempts, or warns, once the grace after cause ended.
func (h *handlerWatch) expire(cause error) {
	if h.state.Load() == watchFinished {
		return
	}

	if h.q != nil {
		h.q.stuck.Add(1)
		if !h.state.CompareAndSwap(watchWatching, watchStuck) {
			h.q.stuck.Add(-1) // the handler returned meanwhile
		}
	}
	for _, t := range h.ts {
		if q := h.w.queues[t.info.Queue]; q != nil && q.maxRunning.Load() > 0 && !errors.Is(cause, ErrLeaseLost) {
			h.w.logger.LogAttrs(h.record, slog.LevelWarn, "handler ignored its context; its task keeps running because its queue has a MaxRunning limit",
				slog.Int64(logKeyTaskID, t.att.id), slog.String(logKeyKind, t.info.Kind), slog.Any(logKeyError, cause))
			continue
		}
		h.w.abandon(h.record, t, cause, h.start)
	}
}

// stop ends the watch once the handler returned. It may be called again.
func (h *handlerWatch) stop(stopAfter func() bool) {
	switch h.state.Swap(watchFinished) {
	case watchFinished:
		return
	case watchStuck:
		h.q.stuck.Add(-1)
	}
	stopAfter()
	if t := h.grace.Load(); t != nil {
		t.Stop()
	}
}

// abandon ends an attempt whose handler ignored the cancel of its context,
// for watch.
func (w *Worker) abandon(record context.Context, t *liveAttempt, cause error, start time.Time) {
	t.mu.Lock()
	running := t.state == attemptRunning
	t.mu.Unlock()
	if !running {
		return
	}

	w.counts.abandoned.Add(1)
	w.logger.LogAttrs(record, slog.LevelWarn, "handler ignored its context; abandoning its attempt",
		slog.Int64(logKeyTaskID, t.att.id), slog.String(logKeyKind, t.info.Kind), slog.Any(logKeyError, cause))
	err := fmt.Errorf("the handler did not return %s after its context ended: %w", w.timing.shutdownGrace, cause)
	w.finish(record, t, outcomeOf(t, err, cause), start)
}

// safeBatch calls a batch handler and turns a panic into an error for every
// task.
func safeBatch(ctx context.Context, reg *registration, infos []*TaskInfo, atts []*attempt) (results []error) {
	defer func() {
		if r := recover(); r != nil {
			err := panicFailure(r)
			results = make([]error, len(infos))
			for i := range results {
				results[i] = err
			}
		}
	}()

	return reg.runBatch(ctx, infos, atts)
}

// invoke runs call through the worker's middleware, by the rules of through,
// turning panics into errors: a panic of call reaches the middleware as an
// error from its next.
func (w *Worker) invoke(ctx context.Context, t *liveAttempt, call func(context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicFailure(r)
		}
	}()

	return through(ctx, w.cfg.Middleware, t.info, func(ctx context.Context) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = panicFailure(r)
			}
		}()

		return call(ctx)
	}, errNextTwice, errSkippedHandler)
}

// errBatchEnded is what the next of a batch's task returns when the batch
// handler ended its goroutine without results.
var errBatchEnded = errors.New("taskmill: the batch handler ended its goroutine")

// errNextTwice fails the second call of next in an attempt's middleware.
var errNextTwice = errors.New("taskmill: middleware called next twice")

// errSkippedHandler fails an attempt whose middleware returned nil without
// running the handler.
var errSkippedHandler = classError("taskmill: middleware returned nil without running the handler", "middleware")

func panicFailure(r any) *failure {
	stack := debug.Stack()
	return &failure{code: CodePanic, err: fmt.Errorf("panic: %s\n%s", recovered.Text(r), stack)}
}

// outcome is how an attempt ended, and what to record for it.
type outcome struct {
	kind  outcomeKind
	delay time.Duration // before the next run, for retries and snoozes
	err   error         // the failure, for the log
	msg   string        // the failure's text, or a snooze's note, for the task's errors
	code  ErrorCode
}

type outcomeKind int

const (
	outcomeComplete outcomeKind = iota
	outcomeRetry
	outcomeDead
	outcomeSnooze
	outcomeCancel
	outcomeRelease
	outcomeDiscard // nothing to record: the lease is gone, or the handler completed the task itself
)

// outcomeOf turns a handler's result into the outcome to record, and reads
// the text of its error. Both run code of the application after the handler
// returned: the methods of the error, and the retry policy. A panic there
// must not end the worker's process, and with it every other attempt. It
// fails the attempt instead, as a handler's panic does: the task is retried
// after the default backoff, or is dead after its last attempt.
func outcomeOf(t *liveAttempt, err, cause error) (o outcome) {
	defer func() {
		if r := recover(); r != nil {
			f := panicFailure(r)
			o = outcome{kind: outcomeDead, err: f, msg: f.Error(), code: f.code}
			if t.info.Attempt < t.info.MaxAttempts {
				o.kind, o.delay = outcomeRetry, ExponentialBackoff{}.Backoff(t.info.Attempt)
			}
		}
	}()

	o = classify(t, err, cause)
	if o.err != nil {
		o.msg = o.err.Error()
	}
	return o
}

// classify turns a handler's result into the outcome to record. It may
// panic, as the application's code that it runs may: call it through
// outcomeOf.
func classify(t *liveAttempt, err, cause error) outcome {
	switch {
	case t.lost.Load():
		return outcome{kind: outcomeDiscard}
	case err == nil && t.att.completedInTx.Load():
		return outcome{kind: outcomeDiscard}
	case err == nil:
		return outcome{kind: outcomeComplete}
	case t.att.cancelRequested.Load():
		return outcome{kind: outcomeCancel}
	case errors.Is(cause, ErrShutdown):
		return outcome{kind: outcomeRelease}
	}

	if s, ok := errors.AsType[*SnoozeError](err); ok {
		return outcome{kind: outcomeSnooze, delay: max(s.Delay, 0)}
	}

	o := outcome{kind: outcomeRetry, err: err}
	if f, ok := errors.AsType[*failure](err); ok {
		o.code = f.code
		if f.permanent {
			o.kind = outcomeDead
		}
	}
	if _, ok := errors.AsType[*PermanentError](err); ok {
		o.kind = outcomeDead
	}
	if errors.Is(cause, ErrAttemptTimeout) && o.code == "" {
		o.code = CodeTimeout
	}

	if o.kind == outcomeRetry {
		if t.info.Attempt >= t.info.MaxAttempts {
			o.kind = outcomeDead
		} else {
			o.delay = t.reg.opts.Retry.Backoff(t.info.Attempt)
		}
	}
	return o
}

// unknownKind decides what to do with a task that no handler of this worker
// knows: wait for a worker that has one, for a while, then give up.
//
// The wait counts from the first time a worker met the task without a
// handler, which that worker records with the task's errors as a no_handler
// note. The note counts only while it is the last of those errors, which it
// stays through this wait's snoozes. An error after it shows that the wait
// is over: a worker with a handler ran the task, or the wait ended in death,
// recorded as unknown_kind, and an operator retried the task. A worker with
// a handler that records no error, because it snoozes the task or gives it
// back, removes the note instead, and so does a retry of a task that was
// canceled while it waited. After any of these, the next worker without a
// handler starts a new wait.
func (w *Worker) unknownKind(ctx context.Context, t *liveAttempt) outcome {
	limit := w.cfg.NoHandlerWait
	var since time.Time
	for _, e := range slices.Backward(t.info.Errors) {
		if e.Code != CodeNoHandler {
			break
		}
		since = e.At
	}

	o := outcome{kind: outcomeSnooze, code: CodeNoHandler}
	var waited time.Duration
	if since.IsZero() {
		o.msg = fmt.Sprintf("no handler for kind %q; waiting up to %s for a worker that has one", t.info.Kind, limit)
	} else {
		waited = t.info.AttemptedAt.Sub(since) // both times come from the database's clock
	}

	if waited >= limit {
		err := fmt.Errorf("no handler for kind %q appeared within %s", t.info.Kind, limit)
		return outcome{kind: outcomeDead, code: CodeUnknownKind, err: err, msg: err.Error()}
	}

	if since.IsZero() {
		w.noHandler.warn(ctx, w, t.info.Kind)
	}
	w.logger.LogAttrs(ctx, slog.LevelDebug, "no handler for task kind; snoozing",
		slog.Int64(logKeyTaskID, t.att.id), slog.String(logKeyKind, t.info.Kind))

	o.delay = min(w.timing.noHandlerSnooze, limit-waited)
	return o
}

// kindWarnings warns of the tasks of kinds that the worker has no handler
// for: per kind, at most once per timing.failureReport, with the number of
// tasks that began to wait since the last line. A rolling deploy can leave
// thousands of tasks of a new kind to workers of the old release, and a line
// for each would flood the log. It keeps at most maxKindWarnings kinds, and
// forgets them all when it would keep more: the kinds come from rows that
// any producer writes.
type kindWarnings struct {
	mu    sync.Mutex
	kinds map[string]*kindWarning
}

// kindWarning is what kindWarnings knows of one kind.
type kindWarning struct {
	logged time.Time // when it was last logged
	tasks  int       // the tasks that began to wait since then
}

// maxKindWarnings is the most kinds that kindWarnings keeps.
const maxKindWarnings = 1000

// warn counts a task of kind that began to wait for a handler, and logs the
// count if the kind was not logged for timing.failureReport.
func (k *kindWarnings) warn(ctx context.Context, w *Worker, kind string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.kinds == nil || len(k.kinds) >= maxKindWarnings && k.kinds[kind] == nil {
		k.kinds = make(map[string]*kindWarning)
	}

	kw := k.kinds[kind]
	if kw == nil {
		kw = &kindWarning{}
		k.kinds[kind] = kw
	}
	kw.tasks++
	if now := time.Now(); now.Sub(kw.logged) >= w.timing.failureReport {
		w.logger.LogAttrs(ctx, slog.LevelWarn, "no handler for task kind; its tasks wait for a worker that has one",
			slog.String(logKeyKind, kind), slog.Int(logKeyTasks, kw.tasks))
		kw.logged, kw.tasks = now, 0
	}
}

// finish records an attempt's outcome on ctx, unless the attempt has ended
// already: its outcome was recorded, or a shutdown released its task. start
// is when the handler started, or zero if none ran.
func (w *Worker) finish(ctx context.Context, t *liveAttempt, o outcome, start time.Time) {
	t.mu.Lock()
	if t.state != attemptRunning {
		t.mu.Unlock()
		return
	}
	t.state = attemptRecording
	if !t.direct {
		// Under t.mu, so that shutdown either abandons the attempt or waits
		// for its result.
		w.recording.Add(1)
		defer w.recording.Done()
	}
	t.mu.Unlock()

	if o.kind == outcomeRetry || o.kind == outcomeDead {
		attrs := []slog.Attr{
			slog.Int64(logKeyTaskID, t.att.id), slog.String(logKeyKind, t.info.Kind),
			slog.Int(logKeyAttempt, t.info.Attempt), slog.Any(logKeyError, o.err),
		}
		if !start.IsZero() { // a handler ran
			attrs = append(attrs, slog.Duration(logKeyDuration, time.Since(start)))
		}

		if o.kind == outcomeDead {
			w.logger.LogAttrs(ctx, slog.LevelError, "attempt failed; the task is dead", attrs...)
		} else {
			attrs = append(attrs, slog.Duration(logKeyRetryIn, o.delay))
			w.logger.LogAttrs(ctx, slog.LevelDebug, "attempt failed; retrying", attrs...)
		}
	}

	switch {
	case o.kind == outcomeComplete && t.direct:
		done := w.completeWaiting(ctx, []*liveAttempt{t})
		t.completedDirectly = len(done) == 1
	case o.kind == outcomeComplete:
		w.completions <- t // Run stops the completer only after every result is in
	default:
		w.record(ctx, t, o)
	}
}

// record writes an outcome other than completion to the database, and
// counts the attempt once that commits.
func (w *Worker) record(ctx context.Context, t *liveAttempt, o outcome) {
	defer w.untrack(t)

	if o.kind == outcomeDiscard && t.att.completedInTx.Load() {
		w.counts.succeeded.Add(1)
		w.tally.add(t.info.Queue, 1, 0) // the handler's transaction completed it
	}
	if o.kind == outcomeDiscard || o.kind == outcomeComplete {
		return
	}

	if o.kind == outcomeRelease {
		w.released.Add(1)
	}

	// An ungrouped attempt of Run goes to the recorder. A grouped one, one
	// of RunTask, and one whose row another transaction held record alone.
	var ended attemptEnd
	var err error
	alone := t.info.GroupKey != "" || t.direct
	if !alone {
		ended, alone, err = w.recordGathered(ctx, t, o)
	}
	if alone {
		try := func(ctx context.Context) error {
			var err error
			ended, err = w.recordOnce(ctx, t, o)
			return err
		}
		err = w.whileLeased(ctx, []*liveAttempt{t}, try)
		for delay := blockedRetryDelay; pgCode(err) == codeLockNotAvailable; delay = min(2*delay, blockedRetryMax) {
			select {
			case <-ctx.Done(): // the next try fails at once
			case <-time.After(delay):
			}
			err = w.whileLeased(ctx, []*liveAttempt{t}, try)
		}
	}
	switch {
	case err == nil && ended == endedFailed:
		w.counts.failed.Add(1)
		w.tally.add(t.info.Queue, 0, 1)
	case err == nil && ended == endedCompleted:
		w.counts.succeeded.Add(1)
		w.tally.add(t.info.Queue, 1, 0)
	}
	if err != nil {
		w.logger.LogAttrs(ctx, slog.LevelWarn, "record attempt failed; the task is rescued when its lease runs out",
			slog.Int64(logKeyTaskID, t.att.id), slog.String(logKeyKind, t.info.Kind), slog.Any(logKeyError, err))
	}
}

// recordOnce makes one try of record. Only a grouped task's outcome takes
// several statements that must commit together. Their transaction waits for
// a row that another transaction holds, such as the group's while a
// transaction enqueues into it, for lockWait at most. Meanwhile it holds the
// task's row, which the renewal of the lease skips: a longer wait could
// outlast the lease, and the rescue would record a lost lease in place of
// this outcome. So the transaction gives up, and record tries again, for as
// long as the row is held and the lease renewed in between.
func (w *Worker) recordOnce(ctx context.Context, t *liveAttempt, o outcome) (attemptEnd, error) {
	conn, err := w.acquire(ctx)
	if err != nil {
		return endedOther, err
	}
	defer conn.Release()

	if t.info.GroupKey == "" {
		return w.recordTx(ctx, w.client.own(conn), t, o)
	}
	ended := endedOther
	err = w.client.inTx(ctx, conn, func(q querier) error {
		// The transaction notifies nobody, so the timeout may last until
		// its commit: see liftLockTimeout.
		if _, err := q.exec(ctx, lockTimeoutSQL, w.timing.lockTimeout()); err != nil {
			return err
		}
		ended, err = w.recordTx(ctx, q, t, o)
		return err
	})
	return ended, err
}

// attemptEnd is how recording an outcome ended an attempt, for the activity.
type attemptEnd int

const (
	endedOther     attemptEnd = iota
	endedFailed               // its failure was recorded
	endedCompleted            // the handler's transaction had completed its task
)

// recordTx records outcome o of t on q, which the worker owns: see
// ownQuerier.
func (w *Worker) recordTx(ctx context.Context, q querier, t *liveAttempt, o outcome) (attemptEnd, error) {
	var rows outcomeRows
	rows.add(t, o)
	found, err := recordOutcomes(ctx, q, w.client.sql.record, &rows)
	if err != nil {
		return endedOther, err
	}
	row, ok := found[keyOf(t)]
	return w.recorded(ctx, q, t, o, ok, row)
}

// recorded finishes the recording of outcome o of t, in q's transaction, if
// it has one, once the statement that records it ran: found says whether
// the attempt still held its task, and row is what the statement returned.
// It says how the outcome ended the attempt, for the activity.
func (w *Worker) recorded(ctx context.Context, q querier, t *liveAttempt, o outcome, found bool, row recordedOutcome) (attemptEnd, error) {
	id := t.att.id
	switch {
	case !found && t.att.completedInTx.Load():
		w.logger.LogAttrs(ctx, slog.LevelWarn, "handler failed after completing its task; it stays completed",
			slog.Int64(logKeyTaskID, id), slog.String(logKeyKind, t.info.Kind), slog.Any(logKeyError, o.err))
		return endedCompleted, nil
	case !found:
		w.logger.LogAttrs(ctx, slog.LevelDebug, "result discarded: the attempt lost its lease",
			slog.Int64(logKeyTaskID, id), slog.String(logKeyKind, t.info.Kind))
		return endedOther, nil
	}

	ended := endedOther
	if o.kind == outcomeRetry || o.kind == outcomeDead {
		ended = endedFailed
	}

	switch s := State(row.state); {
	case s == StateDead && row.group != "":
		return ended, w.client.haltGroup(ctx, w.logger, q, row.queue, row.group, id)
	case s.Finished() && row.group != "":
		return ended, w.client.leaveGroups(ctx, w.logger, q, []groupLeave{{id: id, queue: row.queue, group: row.group, head: true}})
	case s == StateAvailable:
		madeAvailable(q, row.queue)
	}
	return ended, nil
}

// errorJSON encodes an attempt's error for the errors column.
func errorJSON(o outcome) string {
	msg := strings.ReplaceAll(cmp.Or(o.msg, "unknown error"), "\x00", "") // jsonb cannot store NUL
	if len(msg) > errorMessageLimit {
		msg = strings.ToValidUTF8(msg[:errorMessageLimit], "") + "…"
	}

	// The statement adds the attempt and the time.
	b, _ := json.Marshal(struct { //nolint:errchkjson // two strings always encode
		Message string    `json:"message"`
		Code    ErrorCode `json:"code,omitempty"`
	}{msg, o.code})
	return string(b)
}
