package taskmill

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"
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

// State is the lifecycle state of a task. The lifecycle and its guarantees
// are described in
// https://github.com/code19m/taskmill/blob/main/docs/design/task-lifecycle.md.
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
	switch s {
	case StateScheduled, StatePending, StateAvailable, StateRunning,
		StateRetryable, StateCompleted, StateDead, StateCanceled:
		return true
	}
	return false
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
	Attempt     int
	MaxAttempts int

	GroupKey string

	// Metadata holds the tags from [EnqueueOptions.Metadata] and
	// [ClientConfig.ContextMetadata].
	Metadata map[string]string

	// Payload is the task's Args as stored, in JSON.
	Payload json.RawMessage

	// Errors is the task's error history, oldest first: at most the 20 most
	// recent entries.
	Errors []AttemptError

	CreatedAt time.Time

	// RunAt is the earliest time the task may start: for a retryable or
	// snoozed task, when it runs again, and for a task that a [Schedule]
	// enqueued, the time it was scheduled for.
	RunAt time.Time

	AttemptedAt time.Time // zero if never attempted
	FinalizedAt time.Time // zero until completed, dead, or canceled
}

// AttemptError is one entry of a task's error history: why an attempt
// failed, or a note that taskmill recorded.
type AttemptError struct {
	Attempt int       `json:"attempt"`
	At      time.Time `json:"at"`

	// Message is the error's text, truncated to 4 KiB.
	Message string `json:"message"`

	// Code classifies the entries that taskmill itself recorded. It is empty
	// for an error returned by a handler.
	Code ErrorCode `json:"code,omitempty"`
}

// ErrorCode classifies an entry of a task's error history that taskmill
// itself recorded.
type ErrorCode string

// Error codes.
const (
	// CodeLeaseExpired means the worker lost the attempt's lease: it crashed,
	// stopped, or lost the database.
	CodeLeaseExpired ErrorCode = "lease_expired"

	// CodePanic means the handler panicked.
	CodePanic ErrorCode = "panic"

	// CodeTimeout means the attempt ran longer than [HandlerOptions.Timeout].
	CodeTimeout ErrorCode = "timeout"

	// CodeInvalidPayload means the payload did not decode into the kind's
	// Args, so the task became dead at once.
	CodeInvalidPayload ErrorCode = "invalid_payload"

	// CodeUnknownKind means no worker with a handler for the kind appeared
	// within an hour, so the task became dead.
	CodeUnknownKind ErrorCode = "unknown_kind"

	// CodeNoHandler marks a note, not a failure: a worker met the task
	// without a handler for its kind, and the task began to wait for one,
	// without using attempts.
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
// the attempt as usual.
func (t *Task[A]) CompleteTx(ctx context.Context, tx Executor) error {
	if t.attempt == nil {
		return invalidArgument("task was not created by a worker")
	}
	q, err := txQuerier(tx)
	if err != nil {
		return err
	}
	a := t.attempt
	done, err := a.client.complete(ctx, q, []completion{{id: a.id, token: a.token, discard: a.discard}})
	if err != nil {
		return err
	}
	if len(done) == 0 {
		return fmt.Errorf("%w: task %d", ErrLeaseLost, a.id)
	}
	a.completedInTx.Store(true)
	return nil
}

// scanTaskInfo reads one row of taskColumns.
func scanTaskInfo(r rows) (*TaskInfo, error) {
	var (
		info                           TaskInfo
		state                          string
		priority, attempt, maxAttempts int16
		metadata, payload, errs        []byte
		attemptedAt, finalizedAt       *time.Time
	)
	err := r.Scan(&info.ID, &info.Queue, &info.Kind, &state, &priority, &attempt, &maxAttempts, &info.GroupKey,
		&metadata, &payload, &errs, &info.CreatedAt, &info.RunAt, &attemptedAt, &finalizedAt)
	if err != nil {
		return nil, err
	}
	info.State = State(state)
	info.Priority = Priority(priority)
	info.Attempt = int(attempt)
	info.MaxAttempts = int(maxAttempts)
	info.Payload = payload
	info.AttemptedAt = deref(attemptedAt)
	info.FinalizedAt = deref(finalizedAt)
	return &info, decodeJSONColumns(&info, metadata, errs)
}

// decodeJSONColumns decodes a task's metadata and errors columns into info.
func decodeJSONColumns(info *TaskInfo, metadata, errs []byte) error {
	if len(metadata) > 0 && string(metadata) != "{}" {
		if err := json.Unmarshal(metadata, &info.Metadata); err != nil {
			return fmt.Errorf("decode metadata: %w", err)
		}
	}
	if len(errs) > 0 && string(errs) != "[]" {
		if err := json.Unmarshal(errs, &info.Errors); err != nil {
			return fmt.Errorf("decode errors: %w", err)
		}
	}
	return nil
}

func deref(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}
