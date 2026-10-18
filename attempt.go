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
)

// runOne runs a single attempt and records its outcome.
func (w *Worker) runOne(runCtx context.Context, t *tracked) {
	ctx, cancel := context.WithCancelCause(runCtx)
	defer cancel(nil)
	t.mu.Lock()
	t.cancel = cancel
	t.mu.Unlock()
	if t.cancelRequested.Load() {
		cancel(errCancelRequested)
	}
	if t.lost.Load() {
		cancel(ErrLeaseLost)
	}
	if t.reg == nil {
		w.finish(ctx, t, w.unknownKind(ctx, t), nil)
		return
	}

	ctx, cancelTimeout := context.WithTimeoutCause(ctx, t.reg.opts.Timeout, errAttemptTimeout)
	start := time.Now()
	err := w.invoke(ctx, t, func(ctx context.Context) error { return t.reg.run(ctx, t.info, t.att) })
	cause := context.Cause(ctx)
	cancelTimeout()
	w.finish(ctx, t, classify(t, err, cause), &start)
}

// runBatch runs a batch handler for tasks. Middleware still wraps each task:
// each one's chain runs in its own goroutine, and the batch starts once every
// chain has either reached the handler or returned early.
func (w *Worker) runBatch(runCtx context.Context, tasks []*tracked) {
	reg := tasks[0].reg
	ctx, cancel := context.WithCancelCause(runCtx)
	defer cancel(nil)
	ctx, cancelTimeout := context.WithTimeoutCause(ctx, reg.opts.Timeout, errAttemptTimeout)
	defer cancelTimeout()

	type arrival struct {
		index  int
		result chan error
	}
	arrivals := make(chan arrival, len(tasks))
	early := make(chan struct{}, len(tasks))
	errs := make([]error, len(tasks))
	var chains sync.WaitGroup
	start := time.Now()
	for i, t := range tasks {
		chains.Go(func() {
			var reached atomic.Bool
			errs[i] = w.invoke(ctx, t, func(context.Context) error {
				if reached.Swap(true) {
					return errors.New("taskmill: middleware called next twice")
				}
				a := arrival{index: i, result: make(chan error, 1)}
				arrivals <- a
				return <-a.result
			})
			if !reached.Load() {
				early <- struct{}{}
			}
		})
	}

	var arrived []arrival
	for pending := len(tasks); pending > 0; pending-- {
		select {
		case a := <-arrivals:
			arrived = append(arrived, a)
		case <-early:
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
	chains.Wait()

	cause := context.Cause(ctx)
	for i, t := range tasks {
		w.finish(ctx, t, classify(t, errs[i], cause), &start)
	}
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

// invoke runs call through the middleware chain, turning panics into errors.
func (w *Worker) invoke(ctx context.Context, t *tracked, call func(context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicFailure(r)
		}
	}()
	next := func(ctx context.Context) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = panicFailure(r)
			}
		}()
		return call(ctx)
	}
	for _, m := range slices.Backward(w.cfg.Middleware) {
		inner := next
		next = func(ctx context.Context) error { return m(ctx, t.info, inner) }
	}
	return next(ctx)
}

func panicFailure(r any) *failure {
	stack := debug.Stack()
	return &failure{code: codePanic, err: fmt.Errorf("panic: %v\n%s", r, stack)}
}

// outcome is how an attempt ended, and what to record for it.
type outcome struct {
	kind  outcomeKind
	delay time.Duration // before the next run, for retries and snoozes
	err   error
	code  string
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

// classify turns a handler's result into the outcome to record.
func classify(t *tracked, err, cause error) outcome {
	switch {
	case t.lost.Load():
		return outcome{kind: outcomeDiscard}
	case err == nil && t.att.completedInTx.Load():
		return outcome{kind: outcomeDiscard}
	case err == nil:
		return outcome{kind: outcomeComplete}
	case t.cancelRequested.Load():
		return outcome{kind: outcomeCancel}
	case errors.Is(cause, errShutdown):
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
	if errors.Is(cause, errAttemptTimeout) && o.code == "" {
		o.code = codeTimeout
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
// note. A wait that ended in death, recorded as unknown_kind, does not count:
// an operator retried the task since.
func (w *Worker) unknownKind(ctx context.Context, t *tracked) outcome {
	o := outcome{kind: outcomeSnooze, code: codeNoHandler}
	var since time.Time
	for _, e := range slices.Backward(t.info.Errors) {
		if e.Code == codeUnknownKind {
			break
		}
		if e.Code == codeNoHandler {
			since = e.At
		}
	}
	var waited time.Duration
	if since.IsZero() {
		o.err = fmt.Errorf("no handler for kind %q; waiting up to %s for a worker that has one",
			t.info.Kind, w.cfg.UnknownKindTimeout)
	} else {
		waited = t.info.AttemptedAt.Sub(since) // both times come from the database's clock
	}
	if waited >= w.cfg.UnknownKindTimeout {
		return outcome{
			kind: outcomeDead,
			code: codeUnknownKind,
			err:  fmt.Errorf("no handler for kind %q appeared within %s", t.info.Kind, w.cfg.UnknownKindTimeout),
		}
	}
	w.logger.LogAttrs(ctx, slog.LevelWarn, "no handler for task kind; snoozing",
		slog.Int64(logKeyTaskID, t.att.id), slog.String(logKeyKind, t.info.Kind))
	o.delay = min(w.timing.unknownKind, w.cfg.UnknownKindTimeout-waited)
	return o
}

// finish records an attempt's outcome, unless shutdown already released its
// task.
func (w *Worker) finish(ctx context.Context, t *tracked, o outcome, start *time.Time) {
	t.mu.Lock()
	if t.state != stateRunning {
		t.mu.Unlock()
		return
	}
	t.state = stateRecording
	t.mu.Unlock()

	if start != nil && (o.kind == outcomeRetry || o.kind == outcomeDead) {
		attrs := []slog.Attr{
			slog.Int64(logKeyTaskID, t.att.id), slog.String(logKeyKind, t.info.Kind),
			slog.Int(logKeyAttempt, t.info.Attempt), slog.Duration(logKeyDuration, time.Since(*start)),
			slog.Any(logKeyError, o.err),
		}
		if o.kind == outcomeDead {
			w.logger.LogAttrs(ctx, slog.LevelWarn, "attempt failed; the task is dead", attrs...)
		} else {
			w.logger.LogAttrs(ctx, slog.LevelInfo, "attempt failed; retrying", attrs...)
		}
	}

	if o.kind == outcomeComplete && t.direct {
		err := retry(context.WithoutCancel(ctx), func(ctx context.Context) error {
			return w.client.inTx(ctx, func(q querier) error {
				done, err := w.client.complete(ctx, q, completions([]*tracked{t}))
				t.completedDirectly = err == nil && len(done) == 1
				return err
			})
		})
		w.completed(ctx, []*tracked{t}, err)
		return
	}
	if o.kind == outcomeComplete {
		select {
		case w.completions <- t:
			return
		case <-w.completerUp:
			// The completer stopped: the lease expires and the task is
			// rescued, so it runs again rather than being lost.
			w.untrack(t)
			return
		}
	}
	w.record(context.WithoutCancel(ctx), t, o)
}

// record writes an outcome other than completion to the database.
func (w *Worker) record(ctx context.Context, t *tracked, o outcome) {
	defer w.untrack(t)
	if o.kind == outcomeDiscard {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, recordTimeout)
	defer cancel()

	// Only a grouped task's outcome takes several statements that must
	// commit together.
	var err error
	if t.info.GroupKey == "" {
		err = w.recordTx(ctx, pgxQuerier{q: w.client.pool}, t, o)
	} else {
		err = w.client.inTx(ctx, func(q querier) error { return w.recordTx(ctx, q, t, o) })
	}
	if err != nil {
		w.logger.LogAttrs(ctx, slog.LevelError, "record attempt failed; the task is rescued when its lease expires",
			slog.Int64(logKeyTaskID, t.att.id), slog.Any(logKeyError, err))
	}
}

func (w *Worker) recordTx(ctx context.Context, q querier, t *tracked, o outcome) error {
	sql := w.client.sql
	id, token := t.att.id, t.att.token
	var queue, group string
	var found bool
	var err error

	// fail, snooze, and release cancel the task instead if its cancellation
	// was requested, so they return the state they left it in.
	var state string
	switch o.kind {
	case outcomeRetry, outcomeDead:
		want := StateRetryable
		if o.kind == outcomeDead {
			want = StateDead
		}
		found, err = queryRow(ctx, q, sql.fail, []any{id, token, string(want), o.delay.Microseconds(), errorJSON(o)},
			&queue, &group, &state)
	case outcomeCancel:
		state = string(StateCanceled)
		found, err = queryRow(ctx, q, sql.cancelRunning, []any{id, token}, &queue, &group)
	case outcomeSnooze:
		var note *string // an error to record, if the snooze has one
		if o.err != nil {
			s := errorJSON(o)
			note = &s
		}
		found, err = queryRow(ctx, q, sql.snooze, []any{id, token, o.delay.Microseconds(), note}, &queue, &group, &state)
	case outcomeRelease:
		found, err = queryRow(ctx, q, sql.release, []any{id, token}, &queue, &group, &state)
	case outcomeComplete, outcomeDiscard:
		return nil
	}
	switch {
	case err != nil:
		return err
	case !found && t.att.completedInTx.Load():
		w.logger.LogAttrs(ctx, slog.LevelWarn, "handler failed after completing its task; it stays completed",
			slog.Int64(logKeyTaskID, id), slog.Any(logKeyError, o.err))
		return nil
	case !found:
		w.logger.LogAttrs(ctx, slog.LevelDebug, "result discarded: the attempt lost its lease",
			slog.Int64(logKeyTaskID, id))
		return nil
	}

	switch s := State(state); {
	case s.Finished() && group != "":
		return w.client.leaveGroup(ctx, q, queue, group, id, true, s == StateDead)
	case s == StateAvailable:
		w.client.notifyAvailable(ctx, q, queue)
	}
	return nil
}

// errorJSON encodes an attempt's error for the errors column.
func errorJSON(o outcome) string {
	msg := "unknown error"
	if o.err != nil {
		msg = strings.ReplaceAll(o.err.Error(), "\x00", "") // jsonb cannot store NUL
	}
	if len(msg) > errorMessageLimit {
		msg = strings.ToValidUTF8(msg[:errorMessageLimit], "") + "…"
	}
	e := struct {
		Message string `json:"message"`
		Code    string `json:"code,omitempty"`
	}{msg, o.code}
	b, err := json.Marshal(e)
	if err != nil {
		return `{"message": "error could not be encoded"}`
	}
	return string(b)
}
