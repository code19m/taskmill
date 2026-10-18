package taskmill

// Args is the payload of a task. Its kind selects the handler that runs it,
// and it is stored as JSON with encoding/json, at most 1 MiB.
//
// Implement Kind on a value receiver and return a constant: producers and
// consumers agree on the kind. If a kind is renamed, tasks enqueued under the
// old name find no handler, and become dead after an hour.
type Args interface {
	// Kind returns the task kind, such as "email.send". It must not depend
	// on the value's fields: handler registration reads it from a zero value.
	Kind() string
}

// ArgsWithDefaults is implemented by Args that set default enqueue options
// for their kind. Options passed to an enqueue call override these defaults
// field by field. A worker reads the default queue from a zero value, to
// serve it when [WorkerConfig.Queues] is empty.
type ArgsWithDefaults interface {
	Args

	// EnqueueDefaults returns the default options for this kind.
	EnqueueDefaults() EnqueueOptions
}
