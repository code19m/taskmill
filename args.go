package taskmill

// Args is the payload of a task. Its kind selects the handler that runs it,
// and it is stored as JSON with encoding/json.
//
// Implement Kind on a value receiver and return a constant: producers and
// consumers agree on the kind, so changing it orphans enqueued tasks.
type Args interface {
	// Kind returns the task kind, such as "email.send". It must not depend
	// on the value's fields: handler registration reads it from a zero value.
	Kind() string
}

// ArgsWithDefaults is implemented by Args that set default enqueue options
// for their kind. Options passed to an enqueue call override these defaults
// field by field.
type ArgsWithDefaults interface {
	Args

	// EnqueueDefaults returns the default options for this kind.
	EnqueueDefaults() EnqueueOptions
}

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

// State is the lifecycle state of a task. See docs/design/task-lifecycle.md.
type State string

// Task states.
const (
	StateScheduled State = "scheduled"
	StatePending   State = "pending"
	StateAvailable State = "available"
	StateRunning   State = "running"
	StateRetryable State = "retryable"
	StateCompleted State = "completed"
	StateDead      State = "dead"
	StateCanceled  State = "canceled"
)

// Finished reports whether the state is terminal: completed, dead, or
// canceled.
func (s State) Finished() bool {
	return s == StateCompleted || s == StateDead || s == StateCanceled
}
