package taskmill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

// Handler runs tasks of one kind.
//
// Returning nil completes the task. Returning an error fails the attempt: the
// task is retried with backoff until it runs out of attempts. Wrap the error
// with [Permanent] to make the task dead at once, or return [Snooze] to run it
// again later without using an attempt.
//
// Handle must return promptly once ctx is canceled: that means the task was
// canceled, its lease was lost, its attempt timed out, or the worker is
// shutting down.
type Handler[A Args] interface {
	Handle(ctx context.Context, task *Task[A]) error
}

// HandlerFunc adapts a function to the [Handler] interface.
type HandlerFunc[A Args] func(ctx context.Context, task *Task[A]) error

// Handle calls f(ctx, task).
func (f HandlerFunc[A]) Handle(ctx context.Context, task *Task[A]) error {
	return f(ctx, task)
}

// BatchHandler runs tasks of one kind in batches, for work that is cheaper in
// bulk, such as publishing messages to a broker. It returns one error per
// task, in the same order: nil completes that task, and an error fails it as
// a [Handler]'s error would. Returning a shorter slice fails the tasks
// without an entry.
//
// Batches hold the tasks of one fetch, so a batch kind works best in a queue
// of its own.
type BatchHandler[A Args] interface {
	HandleBatch(ctx context.Context, tasks []*Task[A]) []error
}

// HandlerOptions configure how a worker runs one kind.
type HandlerOptions struct {
	// Timeout bounds each attempt, or each batch. Default: 1 minute.
	Timeout time.Duration

	// Retry decides the delay before each retry. Default: the worker's
	// [WorkerConfig.DefaultRetry].
	Retry RetryPolicy

	// BatchSize is the most tasks a [BatchHandler] receives at once.
	// Default: 100. Ignored for other handlers.
	BatchSize int
}

const (
	defaultHandlerTimeout = time.Minute
	defaultBatchSize      = 100
	maxBatchSize          = 1000
	defaultBackoffBase    = time.Second
	defaultBackoffMax     = time.Hour
)

// Handlers maps task kinds to their handlers. Register handlers with
// [AddHandler] or [AddBatchHandler] before passing Handlers to [NewWorker].
// The zero value is an empty set; it is not safe for concurrent use.
type Handlers struct {
	byKind map[string]*registration
	err    error // registration errors, reported by NewWorker
}

// registration is a handler with its options, with its Args type erased.
type registration struct {
	kind string
	opts HandlerOptions

	// Exactly one of run and runBatch is set.
	run      func(ctx context.Context, info *TaskInfo, a *attempt) error
	runBatch func(ctx context.Context, infos []*TaskInfo, as []*attempt) []error
}

// AddHandler registers h for the kind of A. opts may be nil. Mistakes, such
// as registering a kind twice, are reported by [NewWorker].
func AddHandler[A Args](hs *Handlers, h Handler[A], opts *HandlerOptions) {
	var zero A
	reg := &registration{kind: zero.Kind()}
	if opts != nil {
		reg.opts = *opts
	}
	if h == nil {
		hs.fail(fmt.Errorf("handler for kind %q is nil", reg.kind))
		return
	}
	reg.run = func(ctx context.Context, info *TaskInfo, a *attempt) error {
		var args A
		if err := json.Unmarshal(info.Payload, &args); err != nil {
			return &failure{code: codeInvalidPayload, permanent: true, err: fmt.Errorf("decode payload: %w", err)}
		}
		return h.Handle(ctx, &Task[A]{TaskInfo: *info, Args: args, attempt: a})
	}
	hs.add(reg)
}

// AddBatchHandler registers h for the kind of A. opts may be nil. Mistakes,
// such as registering a kind twice, are reported by [NewWorker].
func AddBatchHandler[A Args](hs *Handlers, h BatchHandler[A], opts *HandlerOptions) {
	var zero A
	reg := &registration{kind: zero.Kind()}
	if opts != nil {
		reg.opts = *opts
	}
	if h == nil {
		hs.fail(fmt.Errorf("batch handler for kind %q is nil", reg.kind))
		return
	}
	reg.runBatch = func(ctx context.Context, infos []*TaskInfo, as []*attempt) []error {
		errs := make([]error, len(infos))
		tasks := make([]*Task[A], 0, len(infos))
		index := make([]int, 0, len(infos)) // position of each decoded task in infos
		for i, info := range infos {
			var args A
			if err := json.Unmarshal(info.Payload, &args); err != nil {
				errs[i] = &failure{code: codeInvalidPayload, permanent: true, err: fmt.Errorf("decode payload: %w", err)}
				continue
			}
			tasks = append(tasks, &Task[A]{TaskInfo: *info, Args: args, attempt: as[i]})
			index = append(index, i)
		}
		if len(tasks) == 0 {
			return errs
		}
		results := h.HandleBatch(ctx, tasks)
		for j, i := range index {
			if j < len(results) {
				errs[i] = results[j]
			} else {
				errs[i] = errors.New("the batch handler returned no result for this task")
			}
		}
		return errs
	}
	hs.add(reg)
}

func (hs *Handlers) add(reg *registration) {
	if err := checkName("kind", reg.kind, maxNameLength); err != nil {
		hs.fail(err)
		return
	}
	if reg.opts.Timeout < 0 || reg.opts.BatchSize < 0 || reg.opts.BatchSize > maxBatchSize {
		hs.fail(fmt.Errorf("handler for kind %q: invalid options", reg.kind))
		return
	}
	if hs.byKind == nil {
		hs.byKind = make(map[string]*registration)
	}
	if _, dup := hs.byKind[reg.kind]; dup {
		hs.fail(fmt.Errorf("kind %q registered twice", reg.kind))
		return
	}
	hs.byKind[reg.kind] = reg
}

func (hs *Handlers) fail(err error) {
	hs.err = errors.Join(hs.err, err)
}

// RetryPolicy decides how long to wait before retrying a failed attempt.
type RetryPolicy interface {
	// Backoff returns the delay before the retry that follows the given
	// failed attempt, counting from 1.
	Backoff(attempt int) time.Duration
}

// ExponentialBackoff doubles the delay after each failed attempt, from Base up
// to Max, and spreads retries out with ±10% random jitter. The zero value uses
// a base of 1 second and a maximum of 1 hour.
type ExponentialBackoff struct {
	Base time.Duration
	Max  time.Duration
}

// Backoff implements [RetryPolicy].
func (b ExponentialBackoff) Backoff(attempt int) time.Duration {
	base, maxDelay := b.Base, b.Max
	if base <= 0 {
		base = defaultBackoffBase
	}
	if maxDelay <= 0 {
		maxDelay = defaultBackoffMax
	}
	d := maxDelay
	if shift := attempt - 1; shift >= 0 && shift < 62 {
		if next := base << shift; next > 0 && next < maxDelay {
			d = next
		}
	}
	jitter := 0.9 + rand.Float64()*0.2 //nolint:gosec // jitter needs no cryptographic randomness
	return time.Duration(float64(d) * jitter)
}

// Failure codes that taskmill records with an attempt's error.
const (
	codeInvalidPayload = "invalid_payload"
	codePanic          = "panic"
	codeTimeout        = "timeout"
	codeUnknownKind    = "unknown_kind"
)

// failure is a failed attempt that taskmill itself classified.
type failure struct {
	code      string
	permanent bool
	err       error
}

func (f *failure) Error() string { return f.err.Error() }
func (f *failure) Unwrap() error { return f.err }
