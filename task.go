package taskmill

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// Priority orders tasks within a queue: higher priorities start first.
// The zero value means "not set" and resolves to [PriorityNormal].
type Priority int16

// Task priorities, from lowest to highest.
const (
	PriorityLow      Priority = 1
	PriorityNormal   Priority = 2
	PriorityHigh     Priority = 3
	PriorityCritical Priority = 4
)

// String returns the priority's name, such as "normal".
func (p Priority) String() string {
	switch p {
	case PriorityLow:
		return "low"
	case PriorityNormal:
		return "normal"
	case PriorityHigh:
		return "high"
	case PriorityCritical:
		return "critical"
	}
	return "Priority(" + strconv.Itoa(int(p)) + ")"
}

// State is the lifecycle state of a task. The states, and what moves a task
// from one to the next, are described in
// https://github.com/code19m/taskmill/blob/main/docs/concepts/task-lifecycle.md.
type State string

// Task states.
const (
	StateScheduled State = "scheduled" // waiting for its run time
	StatePending   State = "pending"   // waiting behind the head of its FIFO group
	StateAvailable State = "available" // ready, waiting for a worker
	StateRunning   State = "running"   // leased by an attempt
	StateRetryable State = "retryable" // failed, waiting for its backoff
	StateCompleted State = "completed" // its handler succeeded
	StateDead      State = "dead"      // will not run again unless an operator retries it
	StateCanceled  State = "canceled"  // canceled on request
)

// allStates lists every task state.
var allStates = []State{
	StateScheduled, StatePending, StateAvailable, StateRunning,
	StateRetryable, StateCompleted, StateDead, StateCanceled,
}

// Finished reports whether the state is final: completed, dead, or canceled.
func (s State) Finished() bool {
	return s == StateCompleted || s == StateDead || s == StateCanceled
}

// CanRetry reports whether tasks in state s can be retried, one with
// [Client.Retry] or many with [Client.RetryMatching]: dead and canceled
// tasks.
func (s State) CanRetry() bool {
	return s == StateDead || s == StateCanceled
}

// CanCancel reports whether tasks in state s can be canceled: unfinished
// tasks.
func (s State) CanCancel() bool {
	return s.valid() && !s.Finished()
}

// CanDelete reports whether tasks in state s can be deleted: all but running
// tasks.
func (s State) CanDelete() bool {
	return s.valid() && s != StateRunning
}

// valid reports whether s is one of the task states.
func (s State) valid() bool {
	return slices.Contains(allStates, s)
}

// TaskInfo describes a task: where it is in its lifecycle and what happened
// to its attempts.
type TaskInfo struct {
	ID       int64
	Queue    string
	Kind     string
	State    State
	Priority Priority

	// Attempt is the number of attempts used. In a handler, it is the
	// current attempt, counting from 1. A snooze gives its attempt back, and
	// [Client.Retry] starts over from 0.
	Attempt int

	// MaxAttempts is how many times the task may run before it is dead, as
	// [EnqueueOptions.MaxAttempts] set it.
	MaxAttempts int

	// GroupKey is the key of the task's FIFO group. It is empty for a task
	// in no group.
	GroupKey string

	// Metadata holds the tags from [EnqueueOptions.Metadata] and
	// [EnqueueMiddleware], and for a task that a [Schedule] enqueued,
	// taskmill's own: [MetadataSchedule] and [MetadataScheduledFor]. It is
	// empty for a task whose row holds metadata that taskmill did not write
	// and cannot read: see [CodeInvalidRow].
	Metadata map[string]string

	// Payload is the task's Args as stored, in JSON.
	Payload json.RawMessage

	// Errors is the task's error history, oldest first: at most the 20 most
	// recent entries.
	Errors []AttemptError

	// CreatedAt is when the task was enqueued, by the database's clock.
	CreatedAt time.Time

	// RunAt is the earliest time the task may start. A retry, a snooze, and
	// a lost lease move it to when the task runs again. So for a task that a
	// [Schedule] enqueued, it is the time the run is for on the first
	// attempt only: [TaskInfo.ScheduledFor] returns that time on every
	// attempt.
	RunAt time.Time

	AttemptedAt time.Time // zero if never attempted
	FinalizedAt time.Time // zero until completed, dead, or canceled
}

// AttemptError is one entry of a task's error history: why an attempt
// failed, or a note that taskmill recorded.
type AttemptError struct {
	// Attempt is the attempt that failed, counting from 1. For a note, it is
	// the number of attempts used so far, which may be 0.
	Attempt int

	// At is when the entry was recorded, by the database's clock.
	At time.Time

	// Message is the error's text, truncated to 4 KiB.
	Message string

	// Code classifies the entries that taskmill itself recorded, and marks a
	// handler's error returned after its attempt timed out as [CodeTimeout].
	// It is empty for other errors returned by a handler.
	Code ErrorCode
}

// storedError is an entry of a task's errors column, with the names that the
// database holds: errorJSON and the statements fail, snooze, and rescue
// write them. The tasks of every release hold these names, so they never
// change. Its fields are those of AttemptError, so that an entry converts to
// one.
type storedError struct {
	Attempt int       `json:"attempt"`
	At      time.Time `json:"at"`
	Message string    `json:"message"`
	Code    ErrorCode `json:"code"`
}

// ErrorCode classifies an entry of a task's error history that taskmill
// itself recorded. The codes are part of the API and do not change. A minor
// release may add a code: treat one that is not named here like any other
// failure.
type ErrorCode string

// Error codes.
const (
	// CodeLeaseLost means the worker lost the attempt's lease: it crashed,
	// stopped, or lost the database. Entries that taskmill recorded before
	// v0.3.0 have the code "lease_expired" instead.
	CodeLeaseLost ErrorCode = "lease_lost"

	// CodePanic means the handler panicked, or code of the application that
	// the worker ran after the handler returned: a method of the handler's
	// error, such as Error on a nil pointer, or [RetryPolicy.Backoff]. After
	// a panic in that later code, the task is retried with the default
	// backoff, not with the kind's policy. The entry's Message holds the
	// panic's value and the stack trace.
	CodePanic ErrorCode = "panic"

	// CodeTimeout means the attempt ran longer than [HandlerOptions.Timeout],
	// and the handler then returned an error.
	CodeTimeout ErrorCode = "timeout"

	// CodeInvalidPayload means the payload did not decode into the kind's
	// Args, so the task became dead right away.
	CodeInvalidPayload ErrorCode = "invalid_payload"

	// CodeUnknownKind means no worker with a handler for the kind took the
	// task within an hour of its [CodeNoHandler] note, so the task became
	// dead.
	CodeUnknownKind ErrorCode = "unknown_kind"

	// CodeInvalidRow means the task's row holds a value that taskmill never
	// writes, so the task became dead right away: metadata that is not a
	// JSON object of strings, a time of creation or a run time that is
	// infinite, or a lease token at its largest value. Neither the handler
	// nor the middleware ran. Another database role wrote such a row, or an
	// UPDATE by hand; only the lease token gets there by itself, after two
	// billion fetches of one task. A retry of the task ends the same way
	// until the row is corrected.
	CodeInvalidRow ErrorCode = "invalid_row"

	// CodeNoHandler marks a note, not a failure: a worker fetched the task
	// without a handler for its kind, and the task began to wait for one,
	// without using attempts. Such a worker snoozes the task 1 minute at a
	// time. If no worker with a handler takes it within 1 hour, the task
	// becomes dead, with the code [CodeUnknownKind]. The hour counts only
	// while this note is the last entry of the task's errors. A worker with
	// a handler that then snoozes the task, or gives it back when it stops,
	// removes the note, and so does a retry of the task.
	CodeNoHandler ErrorCode = "no_handler"
)

// Task is a task being run by a handler, with its decoded Args.
type Task[A Args] struct {
	TaskInfo

	Args A

	attempt *attempt
}

// attempt is one run of a task: the lease it holds and how it ended.
type attempt struct {
	client  *Client
	logger  *slog.Logger // the worker's
	id      int64
	token   int32
	discard bool // the kind keeps no history of completed tasks

	// completedInTx is set once the handler completed the task in its own
	// transaction, so the worker must not record a result.
	completedInTx atomic.Bool
}

// CompleteTx completes the task inside the caller's transaction, so that the
// handler's own writes and the completion commit together or not at all. It
// returns [ErrLeaseLost] if another attempt took the task over; return that
// error, so that the transaction rolls back.
//
// After CompleteTx, the handler should commit and return nil. If the handler
// returns an error after its transaction committed, the task stays completed
// and the error is only logged; if the transaction rolled back, the error fails
// the attempt as usual. Returning nil after a rollback records nothing: once
// the task's lease expires, the maintainer rescues it as a failed attempt,
// with the code lease_lost.
//
// At the REPEATABLE READ and SERIALIZABLE isolation levels, call CompleteTx
// as the first statement of the transaction. The worker's lease renewals
// change the task's row, and PostgreSQL fails CompleteTx with a serialization
// failure (SQLSTATE 40001) when a renewal commits after the transaction's
// first statement. The statements after CompleteTx still commit together
// with the completion.
func (t *Task[A]) CompleteTx(ctx context.Context, tx Executor) error {
	if t.attempt == nil {
		return invalidArgument("task was not created by a worker")
	}
	q, err := txQuerier(tx)
	if err != nil {
		return err
	}
	a := t.attempt
	done, err := a.client.complete(ctx, a.logger, q, []completion{{id: a.id, token: a.token, discard: a.discard}})
	if err != nil {
		return err
	}
	if len(done) == 0 {
		return fmt.Errorf("%w: task %d", ErrLeaseLost, a.id)
	}
	a.completedInTx.Store(true)
	return nil
}

// scanTaskInfo reads one row of taskColumns. It reads what does not decode
// as empty: metadata that is not an object of strings, an entry of the
// errors that is not one, and a time that is infinite. Such a row comes from
// another database role or an UPDATE by hand, and a list must still show it,
// with the tasks around it. A worker that takes the task makes it dead (see
// scanLeased).
func scanTaskInfo(r rows) (*TaskInfo, error) {
	var (
		info                                       TaskInfo
		state                                      string
		priority, attempt, maxAttempts             int16
		metadata, payload, errs                    []byte
		createdAt, runAt, attemptedAt, finalizedAt pgtype.Timestamptz
	)
	err := r.Scan(&info.ID, &info.Queue, &info.Kind, &state, &priority, &attempt, &maxAttempts, &info.GroupKey,
		&metadata, &payload, &errs, &createdAt, &runAt, &attemptedAt, &finalizedAt)
	if err != nil {
		return nil, err
	}
	info.State = State(state)
	info.Priority = Priority(priority)
	info.Attempt = int(attempt)
	info.MaxAttempts = int(maxAttempts)
	info.Payload = payload
	info.Metadata, _ = decodeMetadata(metadata)
	info.Errors = decodeErrors(errs)
	info.CreatedAt, _ = finite(createdAt)
	info.RunAt, _ = finite(runAt)
	info.AttemptedAt, _ = finite(attemptedAt)
	info.FinalizedAt, _ = finite(finalizedAt)
	return &info, nil
}

// decodeMetadata decodes a task's metadata column. taskmill writes an object
// of strings, but the column holds any JSON.
func decodeMetadata(metadata []byte) (map[string]string, error) {
	var m map[string]string
	if len(metadata) > 0 && string(metadata) != "{}" {
		if err := json.Unmarshal(metadata, &m); err != nil {
			return nil, fmt.Errorf("the metadata is not a JSON object of strings: %w", err)
		}
	}
	return m, nil
}

// decodeErrors decodes a task's errors column. taskmill writes an array of
// entries, but the column holds any JSON. The errors are a history that no
// attempt depends on, so the entries that decode are kept, and the rest is
// left out.
func decodeErrors(errs []byte) []AttemptError {
	if len(errs) == 0 || string(errs) == "[]" {
		return nil
	}
	var stored []storedError
	if json.Unmarshal(errs, &stored) != nil {
		stored = nil // it holds a zero entry for each one that did not decode
		var entries []json.RawMessage
		_ = json.Unmarshal(errs, &entries) // no entries if the column holds no array
		for _, entry := range entries {
			var e storedError
			if json.Unmarshal(entry, &e) == nil {
				stored = append(stored, e)
			}
		}
	}
	if len(stored) == 0 {
		return nil
	}
	kept := make([]AttemptError, len(stored))
	for i, e := range stored {
		kept[i] = AttemptError(e)
	}
	return kept
}

// finite returns a stored time. It reports false, with the zero time, if the
// time is infinite: a timestamp of PostgreSQL can be, and a time.Time
// cannot. NULL is the zero time.
func finite(ts pgtype.Timestamptz) (time.Time, bool) {
	return ts.Time, ts.InfinityModifier == pgtype.Finite
}

func deref(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}
