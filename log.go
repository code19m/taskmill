package taskmill

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// Log attribute keys, shared so that logs are consistent and searchable.
const (
	logKeyAttempt     = "attempt"
	logKeyConcurrency = "concurrency"
	logKeyDuration    = "duration"
	logKeyError       = "error"
	logKeyFailures    = "failures"
	logKeyGroup       = "group"
	logKeyKind        = "kind"
	logKeyKinds       = "kinds"
	logKeyMaxConns    = "max_conns"
	logKeyName        = "name"
	logKeyOperation   = "operation"
	logKeyPartition   = "partition"
	logKeyQueue       = "queue"
	logKeyQueues      = "queues"
	logKeyReleased    = "released"
	logKeyRetryIn     = "retry_in"
	logKeyTaskID      = "task_id"
	logKeyTasks       = "tasks"
	logKeyTimeout     = "timeout"
	logKeyVersion     = "version"
	logKeyWorker      = "worker_id"
)

// failures logs the failures of an operation that the worker repeats, such
// as a fetch. While the database is down, such an operation fails at every
// try, so it logs only now and then: a warning when it starts failing, an
// error at most once per timing.failureReport while it keeps failing, and
// the recovery. A failure soon after a line waits for the next one, so an
// operation that fails at every other try logs no more.
type failures struct {
	w         *Worker
	operation string
	attrs     []slog.Attr // more about the operation, such as its queue

	mu        sync.Mutex
	count     int       // failures since the incident began; 0 if none
	since     time.Time // when the incident began
	until     time.Time // when it ended, if a success not yet logged ended it
	logged    time.Time // when the last line was logged
	announced bool      // a line reported the incident
}

func newFailures(w *Worker, operation string, attrs ...slog.Attr) *failures {
	return &failures{w: w, operation: operation, attrs: attrs}
}

// observe takes the outcome of one try: err is nil if it succeeded. A
// failure after ctx was canceled does not count: the worker is stopping.
func (f *failures) observe(ctx context.Context, err error) {
	if err != nil && errors.Is(ctx.Err(), context.Canceled) {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	quiet := now.Sub(f.logged) < f.w.timing.failureReport
	if err == nil {
		if f.count == 0 {
			return
		}
		if f.until.IsZero() {
			f.until = now
		}
		if quiet && !f.announced {
			return // the next line, once the quiet time passes, reports it
		}
		f.w.logger.LogAttrs(ctx, slog.LevelInfo, "operation recovered",
			f.with(slog.Int(logKeyFailures, f.count), slog.Duration(logKeyDuration, f.until.Sub(f.since)))...)
		f.count, f.announced, f.until, f.logged = 0, false, time.Time{}, now
		return
	}

	if f.count == 0 {
		f.since = now
	}
	f.count++
	f.until = time.Time{}
	if quiet {
		return
	}
	if age := now.Sub(f.since); age < f.w.timing.failureReport {
		f.w.logger.LogAttrs(ctx, slog.LevelWarn, "operation failed", f.with(slog.Any(logKeyError, err))...)
	} else {
		f.w.logger.LogAttrs(ctx, slog.LevelError, "operation still failing",
			f.with(slog.Int(logKeyFailures, f.count), slog.Duration(logKeyDuration, age), slog.Any(logKeyError, err))...)
	}
	f.announced, f.logged = true, now
}

// with returns the attributes of a line: the operation's, then attrs.
func (f *failures) with(attrs ...slog.Attr) []slog.Attr {
	all := make([]slog.Attr, 0, 1+len(f.attrs)+len(attrs))
	all = append(append(all, slog.String(logKeyOperation, f.operation)), f.attrs...)
	return append(all, attrs...)
}
