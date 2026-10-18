package taskmill

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync/atomic"
	"time"
)

// TaskInfo describes a task: where it is in its lifecycle and what happened
// to its attempts.
type TaskInfo struct {
	ID          int64
	Queue       string
	Kind        string
	State       State
	Priority    Priority
	Attempt     int
	MaxAttempts int
	GroupKey    string
	Metadata    map[string]string

	// Payload is the task's Args as stored, in JSON.
	Payload json.RawMessage

	// Errors holds the errors of failed attempts, oldest first. Only the
	// most recent ones are kept.
	Errors []AttemptError

	CreatedAt   time.Time
	RunAt       time.Time
	Deadline    time.Time // zero if none
	AttemptedAt time.Time // zero if never attempted
	FinalizedAt time.Time // zero until completed, dead, or canceled
}

// AttemptError records why an attempt failed.
type AttemptError struct {
	Attempt int       `json:"attempt"`
	At      time.Time `json:"at"`

	// Message is the error's text, truncated to a fixed length.
	Message string `json:"message"`

	// Code classifies failures that taskmill itself detected:
	// "lease_expired", "deadline_exceeded", "panic", "timeout",
	// "invalid_payload", or "unknown_kind". Empty for errors returned by a
	// handler.
	Code string `json:"code,omitempty"`
}

// Task is a task being run by a handler, with its decoded Args.
type Task[A Args] struct {
	TaskInfo

	Args A

	attempt *attempt
}

// attempt is one run of a task: the lease it holds and how it ended.
type attempt struct {
	client *Client
	id     int64
	token  int32

	// completedInTx is set once the handler completed the task in its own
	// transaction, so the worker must not record a result.
	completedInTx atomic.Bool
}

// CompleteTx completes the task inside the caller's transaction, so that the
// handler's own writes and the completion commit together or not at all. It
// returns [ErrLeaseLost] if the attempt no longer owns the task; the caller
// must then roll back.
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
	done, err := a.client.complete(ctx, q, []completion{{id: a.id, token: a.token}})
	if err != nil {
		return err
	}
	if len(done) == 0 {
		return fmt.Errorf("%w: task %d", ErrLeaseLost, a.id)
	}
	a.completedInTx.Store(true)
	return nil
}

// completion identifies an attempt whose task succeeded.
type completion struct {
	id    int64
	token int32
}

// completedTask is a task that a completion finished.
type completedTask struct {
	id    int64
	queue string
	group string
}

// complete records successful attempts: it moves their tasks to history, or
// deletes them if their queue keeps no history, and advances their FIFO
// groups. It returns the tasks it completed; an attempt that lost its lease is
// left out. q must be a transaction if any task belongs to a FIFO group.
func (c *Client) complete(ctx context.Context, q querier, cs []completion) ([]completedTask, error) {
	ids := make([]int64, len(cs))
	tokens := make([]int32, len(cs))
	for i, x := range cs {
		ids[i], tokens[i] = x.id, x.token
	}

	r, err := q.query(ctx, c.sql.complete, ids, tokens)
	if err != nil {
		return nil, fmt.Errorf("taskmill: complete: %w", err)
	}
	var done []completedTask
	for r.Next() {
		var t completedTask
		if err := r.Scan(&t.id, &t.queue, &t.group); err != nil {
			r.Close()
			return nil, fmt.Errorf("taskmill: complete: %w", err)
		}
		done = append(done, t)
	}
	r.Close()
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("taskmill: complete: %w", err)
	}

	for _, t := range sortedByGroup(done) {
		if err := c.leaveGroup(ctx, q, t.queue, t.group, t.id, true, false); err != nil {
			return nil, fmt.Errorf("taskmill: complete task %d: %w", t.id, err)
		}
	}
	return done, nil
}

// sortedByGroup returns the grouped tasks in a fixed order of queue and group,
// so that concurrent transactions lock group rows in the same order and cannot
// deadlock.
func sortedByGroup(ts []completedTask) []completedTask {
	var grouped []completedTask
	for _, t := range ts {
		if t.group != "" {
			grouped = append(grouped, t)
		}
	}
	slices.SortFunc(grouped, func(a, b completedTask) int {
		return cmp.Or(cmp.Compare(a.queue, b.queue), cmp.Compare(a.group, b.group))
	})
	return grouped
}

// scanTaskInfo reads one row of taskColumns.
func scanTaskInfo(r rows) (*TaskInfo, error) {
	var (
		info                               TaskInfo
		state                              string
		priority, attempt, maxAttempts     int16
		metadata, payload, errs            []byte
		deadline, attemptedAt, finalizedAt *time.Time
	)
	err := r.Scan(&info.ID, &info.Queue, &info.Kind, &state, &priority, &attempt, &maxAttempts, &info.GroupKey,
		&metadata, &payload, &errs, &info.CreatedAt, &info.RunAt, &deadline, &attemptedAt, &finalizedAt)
	if err != nil {
		return nil, err
	}
	info.State = State(state)
	info.Priority = Priority(priority)
	info.Attempt = int(attempt)
	info.MaxAttempts = int(maxAttempts)
	info.Payload = payload
	if err := decodeMetadata(metadata, &info.Metadata); err != nil {
		return nil, err
	}
	if err := decodeErrors(errs, &info.Errors); err != nil {
		return nil, err
	}
	info.Deadline = deref(deadline)
	info.AttemptedAt = deref(attemptedAt)
	info.FinalizedAt = deref(finalizedAt)
	return &info, nil
}

func decodeMetadata(raw []byte, dst *map[string]string) error {
	if len(raw) == 0 || string(raw) == "{}" {
		return nil
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("decode metadata: %w", err)
	}
	return nil
}

func decodeErrors(raw []byte, dst *[]AttemptError) error {
	if len(raw) == 0 || string(raw) == "[]" {
		return nil
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("decode errors: %w", err)
	}
	return nil
}

func deref(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}
