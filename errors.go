package taskmill

import (
	"fmt"
	"time"
)

// Errors returned by taskmill. Test for them with [errors.Is], and for the
// cause of a handler's canceled context with [context.Cause].
//
// Each error names its class through an ErrorType() string method, which
// OpenTelemetry's semconv.ErrorType reports as error.type. The classes are
// part of the API and do not change: "not_found", "lease_lost",
// "invalid_state", "invalid_argument", "schema_outdated", "conflict",
// "canceled", "timeout", and "shutdown", and for failures that taskmill
// classifies itself, "panic", "invalid_payload", and "middleware", for
// middleware that returned nil without calling its next.
var (
	// ErrNotFound means the task or schedule does not exist, or no longer
	// exists.
	ErrNotFound = classError("taskmill: not found", "not_found")

	// ErrLeaseLost means the attempt no longer owns its task: its lease
	// expired and the task was rescued, canceled, or fetched again. It is
	// also the cause of the context of a handler whose attempt lost its
	// lease.
	ErrLeaseLost = classError("taskmill: lease lost", "lease_lost")

	// ErrInvalidState means the operation does not apply to the task's
	// current state, such as retrying a running task.
	ErrInvalidState = classError("taskmill: invalid state", "invalid_state")

	// ErrInvalidArgument means an argument or option is invalid: out of
	// range, too long, or text that PostgreSQL cannot store, such as a NUL
	// character.
	ErrInvalidArgument = classError("taskmill: invalid argument", "invalid_argument")

	// ErrSchemaOutdated means the database schema is missing or older than
	// this version of taskmill. Run [Migrate].
	ErrSchemaOutdated = classError("taskmill: database schema is missing or outdated", "schema_outdated")

	// ErrConflict means an update lost to concurrent updates of the same
	// settings every time it tried. Try again.
	ErrConflict = classError("taskmill: conflict", "conflict")

	// ErrTaskCanceled is the cause of a handler's context when the task was
	// canceled while it ran. The task becomes canceled when the attempt ends,
	// unless the handler returns nil, which completes it.
	ErrTaskCanceled = classError("taskmill: task canceled", "canceled")

	// ErrAttemptTimeout is the cause of a handler's context when the attempt
	// ran longer than [HandlerOptions.Timeout]. A failure that the handler
	// then returns is recorded with the code [CodeTimeout]; returning nil
	// completes the task.
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
// becomes dead at once, keeping its remaining attempts unused.
type PermanentError struct {
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
// counting the current attempt.
type SnoozeError struct {
	Delay time.Duration
}

// Snooze returns an error that reschedules the task to run after d, without
// using an attempt. Use it when the task cannot proceed yet, for example while
// waiting for a rate limit to reset. A delay of zero or less makes the task
// available at once. A task that always snoozes never becomes dead.
func Snooze(d time.Duration) error {
	return &SnoozeError{Delay: d}
}

// Error implements the error interface.
func (e *SnoozeError) Error() string {
	return "snooze for " + e.Delay.String()
}
