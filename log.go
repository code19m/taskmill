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
// try, so it logs only now and then: a warning when an incident starts, an
// error at most once per timing.failureReport while it lasts, and the
// recovery. An incident lasts while failures come less than a report
// interval apart, even with successes between them, so an operation that
// fails at every other try escalates too, and logs no more.
type failures struct {
	w         *Worker
	operation string
	attrs     []slog.Attr // more about the operation, such as its queue

	mu      sync.Mutex
	count   int       // failures of the current incident; 0 if none
	since   time.Time // the incident's first failure
	last    time.Time // its last failure
	failing bool      // the last try failed
	pending bool      // the operation recovered, and no line said so yet
	paired  bool      // the last line was a failure line: the recovery follows at once
	logged  time.Time // when the last line was logged
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
	every := f.w.timing.failureReport
	quiet := now.Sub(f.logged) < every
	if err == nil {
		if f.failing {
			f.failing, f.pending = false, true
		}
		if f.pending && (f.paired || !quiet) {
			f.recovered(ctx, now)
		}
		return
	}

	// An incident ends once its operation has not failed for a report
	// interval: this failure starts another.
	if f.count > 0 && !f.failing && now.Sub(f.last) >= every {
		if f.pending {
			f.recovered(ctx, now)
		}
		f.count = 0
	}
	if f.count == 0 {
		f.since = now
	}
	f.count++
	f.last, f.failing = now, true
	if quiet {
		return
	}
	if age := now.Sub(f.since); age < every {
		f.w.logger.LogAttrs(ctx, slog.LevelWarn, "operation failed", f.with(slog.Any(logKeyError, err))...)
	} else {
		f.w.logger.LogAttrs(ctx, slog.LevelError, "operation still failing",
			f.with(slog.Int(logKeyFailures, f.count), slog.Duration(logKeyDuration, age), slog.Any(logKeyError, err))...)
	}
	f.paired, f.logged = true, now
}

// recovered logs that the incident's operation works again.
func (f *failures) recovered(ctx context.Context, now time.Time) {
	f.w.logger.LogAttrs(ctx, slog.LevelInfo, "operation recovered",
		f.with(slog.Int(logKeyFailures, f.count), slog.Duration(logKeyDuration, f.last.Sub(f.since)))...)
	f.pending, f.paired, f.logged = false, false, now
}

// reset forgets the current incident without a line, for an operation that
// stops running here, such as the maintainer's work when another worker
// takes over.
func (f *failures) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.count, f.failing, f.pending, f.paired, f.logged = 0, false, false, false, time.Time{}
}

// with returns the attributes of a line: the operation's, then attrs.
func (f *failures) with(attrs ...slog.Attr) []slog.Attr {
	all := make([]slog.Attr, 0, 1+len(f.attrs)+len(attrs))
	all = append(append(all, slog.String(logKeyOperation, f.operation)), f.attrs...)
	return append(all, attrs...)
}
