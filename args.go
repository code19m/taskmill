package taskmill

import "reflect"

// Args is the payload of a task. Its kind selects the handler that runs it,
// and it is stored as JSON with encoding/json, at most 1 MiB.
//
// Implement Kind on a value receiver and return a constant: producers and
// consumers agree on the kind. If a kind is renamed, tasks enqueued under the
// old name find no handler, and become dead after [WorkerConfig.NoHandlerWait],
// an hour by default.
//
// An enqueue returns an error that wraps [ErrInvalidArgument] for nil Args,
// also a nil pointer of an Args type, and for a payload that PostgreSQL
// cannot store, or that is too large:
//
//   - The payload must not encode to null. It must be valid UTF-8, with no
//     NUL character (\u0000).
//   - Every number must fit PostgreSQL's numeric type: at most 131,072
//     digits before the decimal point, and 16,383 after it.
//   - The limit of 1 MiB counts each number as PostgreSQL stores it, written
//     in full: 1e6 counts as 7 bytes.
//   - PostgreSQL can still reject a value that passes these checks, such as
//     an unpaired surrogate escape (\ud800).
//
// The [guide to defining tasks] says what to put in a payload, and how to
// change one later.
//
// [guide to defining tasks]: https://github.com/code19m/taskmill/blob/main/docs/guides/define-tasks.md
type Args interface {
	// Kind returns the task kind, such as "email.send", in 1 to 128
	// characters of valid UTF-8, with no NUL character. It must not depend
	// on the value's fields: handler registration reads it from a zero
	// value.
	Kind() string
}

// isNil reports whether v is nil, or a nil pointer, whose methods may
// dereference it: an Args, or what an Executor wraps.
func isNil(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Pointer && rv.IsNil()
}

// ArgsWithDefaults is implemented by Args that set default enqueue options
// for their kind. Options passed to an enqueue call override these defaults
// field by field, and Metadata key by key. The default Queue must not depend
// on the value's fields: a worker reads it from a zero value, to fetch from
// that queue when [WorkerConfig.Queues] is empty.
type ArgsWithDefaults interface {
	Args

	// EnqueueDefaults returns the default options for this kind.
	EnqueueDefaults() EnqueueOptions
}
