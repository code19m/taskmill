package taskmill

import (
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// Errors returned by taskmill. Test for them with [errors.Is], never with ==
// and never by their message: taskmill wraps them with details, such as the
// task's ID, and a message may change in any release. Test for the cause of a
// handler's canceled context with [context.Cause]. The packages outbox, ui,
// and otel return the same errors.
//
// A database or network error is none of these, and has no class. taskmill
// wraps it with what it was doing, so [errors.As] still finds pgx's error
// types in it.
//
// Each error names its class through an ErrorType() string method, which
// OpenTelemetry's semconv.ErrorType reports as error.type. The classes are
// part of the API and do not change: "not_found", "lease_lost",
// "invalid_state", "invalid_argument", "schema_outdated",
// "blocking_migration", "conflict", "stored_without_ids", "canceled",
// "timeout", and "shutdown". Failures that taskmill classifies
// itself have a class, but no error value: "panic" for a handler or an
// outbox publisher that panicked, "invalid_payload" for a payload that did
// not decode into the kind's Args, and "middleware" for middleware that
// returned nil without calling its next. A minor release may add a class:
// treat one that is not named here like any other failure.
var (
	// ErrNotFound means the task or schedule does not exist, or no longer
	// exists: a task was deleted, or retention removed it. Treat it as gone.
	ErrNotFound = classError("taskmill: not found", "not_found")

	// ErrLeaseLost means the attempt no longer owns its task: its lease
	// expired and the task was rescued, canceled, or fetched again.
	// [Task.CompleteTx] returns it: return it from the handler, so that the
	// handler's transaction rolls back.
	//
	// It is also the cause of the context of a handler whose attempt lost its
	// lease. Either the worker could not renew the lease in time, so the task
	// may already run on another worker. Or this worker fetched the task
	// again after a rescue: then the older of its two attempts gets this
	// cause. The result of the attempt is discarded: the maintainer rescues
	// a task whose lease ran out as a failed attempt, with the code
	// [CodeLeaseLost], so the task is retried.
	ErrLeaseLost = classError("taskmill: lease lost", "lease_lost")

	// ErrInvalidState means the operation does not fit the state of its
	// target: a retry of a task that is not dead or canceled, a cancel of a
	// finished task, a delete of a running one, a run of a paused schedule,
	// or [Worker.Run] on a worker that already runs. To check a task first,
	// use [State.CanRetry], [State.CanCancel], or [State.CanDelete].
	ErrInvalidState = classError("taskmill: invalid state", "invalid_state")

	// ErrInvalidArgument means an argument, an option, or a configuration is
	// invalid: out of range, too long, or text that PostgreSQL cannot store,
	// such as a NUL character. Every call that takes arguments can return it.
	// The message says what is wrong, and the same call fails again until
	// the caller fixes it.
	ErrInvalidArgument = classError("taskmill: invalid argument", "invalid_argument")

	// ErrSchemaOutdated means the database schema is missing or older than
	// this version of taskmill. [NewClient] returns it. Run [Migrate], or
	// apply [Migrations] with the application's own tool, before the new
	// version of the application starts.
	ErrSchemaOutdated = classError("taskmill: database schema is missing or outdated", "schema_outdated")

	// ErrBlockingMigration means that [Migrate] refused a pending migration
	// that would block the writes to a large table while it builds an
	// index: see [MigrateConfig.MaxBlockingIndexRows]. Migrate changed
	// nothing, and trying again does not help: a person runs it at a quiet
	// time, with the limit raised.
	ErrBlockingMigration = classError("taskmill: a migration would block writes to a large table", "blocking_migration")

	// ErrConflict means that other work stands in the way of the call.
	// [Client.UpdateQueue] and [Client.UpdateSettings] return it when they
	// lost to concurrent updates of the same settings each of the 10 times
	// they tried: try again a little later. [Client.Retry] returns it when
	// another task holds the dedupe key that the canceled task had: that
	// task stands for the same work, so trying again helps only once it
	// gave the key away.
	ErrConflict = classError("taskmill: conflict", "conflict")

	// ErrStoredWithoutIDs means that an enqueue outside a transaction stored
	// its tasks, and then failed while it looked for the tasks that hold the
	// dedupe keys of its duplicates: see [EnqueueResult.ID]. The tasks
	// without a dedupe key are stored, so do not enqueue them again. Those
	// with a dedupe key may be enqueued again, to learn their IDs. On an
	// executor that runs in a transaction that taskmill cannot see as one,
	// as [Executor] lists, stored means stored in that transaction, which
	// can rarely commit after this error: roll it back, and the call stored
	// nothing.
	ErrStoredWithoutIDs = classError("taskmill: the tasks are stored, but the IDs of their duplicates are unknown", "stored_without_ids")

	// ErrTaskCanceled is the cause of a handler's context when the task was
	// canceled while it ran. The task becomes canceled when the attempt ends,
	// unless the handler returns nil, which completes it. A handler that has
	// not returned 5 seconds later is abandoned, and the task becomes
	// canceled: see [HandlerOptions.Timeout].
	ErrTaskCanceled = classError("taskmill: task canceled", "canceled")

	// ErrAttemptTimeout is the cause of a handler's context when the attempt
	// ran longer than [HandlerOptions.Timeout]. A failure that the handler
	// then returns is recorded with the code [CodeTimeout]; returning nil
	// completes the task. A handler that has not returned 5 seconds later is
	// abandoned, and the attempt fails with that code all the same.
	ErrAttemptTimeout = classError("taskmill: attempt timed out", "timeout")

	// ErrShutdown is the cause of a handler's context when the worker is
	// shutting down and the attempt outlived [WorkerConfig.DrainTimeout], or
	// the context of [Worker.RunTask] ended. The task goes back to its queue
	// without using an attempt, unless the handler returns nil, which
	// completes it.
	ErrShutdown = classError("taskmill: worker shutting down", "shutdown")
)

// classified is an error that names its class. OpenTelemetry's
// semconv.ErrorType reports the class of the first such error in a chain as
// error.type, without taskmill depending on OpenTelemetry.
type classified struct {
	msg, class string
}

func classError(msg, class string) error { return &classified{msg: msg, class: class} }

// Error implements the error interface.
func (e *classified) Error() string { return e.msg }

// ErrorType returns the error's class, such as "invalid_argument".
func (e *classified) ErrorType() string { return e.class }

// invalidArgument returns an error that wraps [ErrInvalidArgument] with
// details.
func invalidArgument(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidArgument, fmt.Sprintf(format, args...))
}

// PermanentError marks a handler error that retrying cannot fix. The task
// becomes dead right away, keeping its remaining attempts unused. Create one
// with [Permanent].
//
// It names no error class of its own. OpenTelemetry's semconv.ErrorType
// reports the class of the first error in Err's chain that has an
// ErrorType() string method, and "*taskmill.PermanentError" if none has.
type PermanentError struct {
	// Err is the handler's error. [errors.Is] and [errors.As] still find it.
	Err error
}

// Permanent wraps err so that the task becomes dead without retrying, for
// example when its payload fails validation. Permanent(nil) returns nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

// Error implements the error interface.
func (e *PermanentError) Error() string {
	if e.Err == nil {
		return "permanent failure"
	}
	return "permanent: " + e.Err.Error()
}

// Unwrap returns the wrapped error.
func (e *PermanentError) Unwrap() error {
	return e.Err
}

// SnoozeError asks taskmill to run the task again after a delay, without
// counting the current attempt. Create one with [Snooze]. A snooze is not a
// failure, so it has no error class.
type SnoozeError struct {
	// Delay is how long the task waits before it runs again. Zero or less
	// means right away.
	Delay time.Duration
}

// Snooze returns an error that reschedules the task to run after d, without
// using an attempt. Use it when the task cannot proceed yet, for example while
// waiting for a rate limit to reset. A delay of zero or less makes the task
// available right away. After a longer delay, the task becomes available
// within about a second of its new run time, while a maintainer runs. A task
// that always snoozes never becomes dead.
func Snooze(d time.Duration) error {
	return &SnoozeError{Delay: d}
}

// Error implements the error interface.
func (e *SnoozeError) Error() string {
	return "snooze for " + e.Delay.String()
}

// PostgreSQL's error codes (SQLSTATE) that taskmill handles. The code, not
// the message, tells an error apart: the message is in the server's
// language.
const (
	codeDataException         = "22" // a class: every code that starts with it
	codeInvalidParameterValue = "22023"
	codeCheckViolation        = "23514"
	codeDeadlockDetected      = "40P01"
	codeInsufficientPrivilege = "42501"
	codeUndefinedTable        = "42P01"
	codeLockNotAvailable      = "55P03"
	codeQueryCanceled         = "57014"
)

// pgCode returns the code of the PostgreSQL error in err's chain, or "" if
// there is none.
func pgCode(err error) string {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pgErr.Code
	}
	return ""
}
