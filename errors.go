package taskmill

import (
	"errors"
	"fmt"
	"time"
)

// Errors returned by taskmill. Test for them with [errors.Is].
var (
	// ErrNotFound means the task or schedule does not exist, or no longer
	// exists.
	ErrNotFound = errors.New("taskmill: not found")

	// ErrLeaseLost means the attempt no longer owns its task: its lease
	// expired and the task was rescued, canceled, or fetched again.
	ErrLeaseLost = errors.New("taskmill: lease lost")

	// ErrInvalidState means the operation does not apply to the task's
	// current state, such as retrying a running task.
	ErrInvalidState = errors.New("taskmill: invalid state")

	// ErrInvalidArgument means an argument or option is invalid: out of
	// range, too long, or text that PostgreSQL cannot store, such as a NUL
	// character.
	ErrInvalidArgument = errors.New("taskmill: invalid argument")

	// ErrSchemaOutdated means the database schema is missing or older than
	// this version of taskmill. Run [Migrate].
	ErrSchemaOutdated = errors.New("taskmill: database schema is missing or outdated")
)

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
