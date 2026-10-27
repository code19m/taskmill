package taskmill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"time"

	"github.com/code19m/taskmill/internal/argkind"
)

// Handler runs tasks of one kind.
//
// Returning nil completes the task. Returning an error fails the attempt: the
// task is retried with backoff until it runs out of attempts. Wrap the error
// with [Permanent] to make the task dead at once, or return [Snooze] to run it
// again later without using an attempt.
//
// Handle must return promptly once ctx is canceled. [context.Cause] says
// why: [ErrTaskCanceled], [ErrLeaseLost], [ErrAttemptTimeout], or
// [ErrShutdown].
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
// A batch takes one slot of its queue's concurrency. When a worker fetches a
// task of a batch kind, it fills that task's batch with more available tasks
// of the kind, so a batch kind can share a queue with other kinds.
//
// ctx is canceled when the batch times out or the worker shuts down, with
// the cause [ErrAttemptTimeout] or [ErrShutdown], but not when one of its
// tasks is canceled or loses its lease: the batch runs to the end, and then
// that task becomes canceled, or its result is discarded.
type BatchHandler[A Args] interface {
	HandleBatch(ctx context.Context, tasks []*Task[A]) []error
}

// HandlerOptions configure how a worker runs one kind.
type HandlerOptions struct {
	// Timeout bounds each attempt, or each batch. Default: 1 minute.
	Timeout time.Duration

	// Retry decides the delay before each retry. A task whose worker lost its
	// lease is retried with the default instead, because the maintainer that
	// rescues it knows no handler's options. Default: [ExponentialBackoff]{}.
	Retry RetryPolicy

	// BatchSize is the most tasks a [BatchHandler] receives at once.
	// Default: 100. Maximum: 1,000. Ignored for other handlers.
	BatchSize int

	// DiscardCompleted deletes completed tasks of the kind at once, instead
	// of keeping them in history, for high-volume kinds whose history nobody
	// reads.
	DiscardCompleted bool
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
	kind  string
	queue string // the kind's default queue
	opts  HandlerOptions

	// Exactly one of run and runBatch is set.
	run      func(ctx context.Context, info *TaskInfo, a *attempt) error
	runBatch func(ctx context.Context, infos []*TaskInfo, as []*attempt) []error
}

// AddHandler registers h for the kind of A. opts may be nil. Mistakes, such
// as registering a kind twice, are reported by [NewWorker].
func AddHandler[A Args](hs *Handlers, h Handler[A], opts *HandlerOptions) {
	reg := newRegistration[A](hs, h == nil, opts)
	if reg == nil {
		return
	}
	reg.run = func(ctx context.Context, info *TaskInfo, a *attempt) error {
		t, err := newTask[A](info, a)
		if err != nil {
			return err
		}
		return h.Handle(ctx, t)
	}
	hs.add(reg)
}

// AddBatchHandler registers h for the kind of A. opts may be nil. Mistakes,
// such as registering a kind twice, are reported by [NewWorker].
func AddBatchHandler[A Args](hs *Handlers, h BatchHandler[A], opts *HandlerOptions) {
	reg := newRegistration[A](hs, h == nil, opts)
	if reg == nil {
		return
	}
	reg.runBatch = func(ctx context.Context, infos []*TaskInfo, as []*attempt) []error {
		errs := make([]error, len(infos))
		tasks := make([]*Task[A], 0, len(infos))
		index := make([]int, 0, len(infos)) // position of each decoded task in infos
		for i, info := range infos {
			t, err := newTask[A](info, as[i])
			if err != nil {
				errs[i] = err
				continue
			}
			tasks = append(tasks, t)
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

// newRegistration starts the registration of A's handler, or records why it
// cannot and returns nil.
func newRegistration[A Args](hs *Handlers, nilHandler bool, opts *HandlerOptions) *registration {
	kind, err := argkind.Of[A]()
	if err == nil {
		err = checkName("kind of "+reflect.TypeFor[A]().String(), kind, maxNameLength)
	}
	if err != nil {
		hs.fail(err)
		return nil
	}
	if nilHandler {
		hs.fail(fmt.Errorf("handler for kind %q is nil", kind))
		return nil
	}
	reg := &registration{kind: kind, queue: defaultQueueOf[A]()}
	if opts != nil {
		reg.opts = *opts
	}
	return reg
}

// defaultQueueOf returns the queue that A's tasks go to by default, read
// from a zero value as the kind is.
func defaultQueueOf[A Args]() (queue string) {
	defer func() {
		if recover() != nil {
			queue = DefaultQueue
		}
	}()
	if d, ok := any(argkind.Zero[A]()).(ArgsWithDefaults); ok && d.EnqueueDefaults().Queue != "" {
		return d.EnqueueDefaults().Queue
	}
	return DefaultQueue
}

// newTask decodes a task's payload for its handler. A payload that does not
// decode makes the task dead: retrying cannot fix it.
func newTask[A Args](info *TaskInfo, a *attempt) (*Task[A], error) {
	var args A
	if err := json.Unmarshal(info.Payload, &args); err != nil {
		return nil, &failure{code: CodeInvalidPayload, permanent: true, err: fmt.Errorf("decode payload: %w", err)}
	}
	return &Task[A]{TaskInfo: *info, Args: args, attempt: a}, nil
}

func (hs *Handlers) add(reg *registration) {
	switch {
	case reg.opts.Timeout < 0:
		hs.fail(fmt.Errorf("handler for kind %q: timeout must not be negative", reg.kind))
		return
	case reg.opts.BatchSize < 0 || reg.opts.BatchSize > maxBatchSize:
		hs.fail(fmt.Errorf("handler for kind %q: batch size %d is not between 0 and %d", reg.kind, reg.opts.BatchSize, maxBatchSize))
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

// fail records a registration error, which NewWorker reports as an
// [ErrInvalidArgument].
func (hs *Handlers) fail(err error) {
	if !errors.Is(err, ErrInvalidArgument) {
		err = fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	hs.err = errors.Join(hs.err, err)
}

// RetryPolicy decides how long to wait before retrying a failed attempt.
type RetryPolicy interface {
	// Backoff returns the delay before the retry that follows the given
	// failed attempt, counting from 1.
	Backoff(attempt int) time.Duration
}

// ExponentialBackoff doubles the delay after each failed attempt, from Base up
// to Max, and spreads retries out with ±10% random jitter, so a delay can
// reach 10% above Max. The zero value uses a base of 1 second and a maximum
// of 1 hour.
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

// failure is a failed attempt that taskmill itself classified.
type failure struct {
	code      ErrorCode
	permanent bool
	err       error
}

func (f *failure) Error() string { return f.err.Error() }
func (f *failure) Unwrap() error { return f.err }

// ErrorType returns the failure's code, which OpenTelemetry's
// semconv.ErrorType reports as the error.type of the attempt.
func (f *failure) ErrorType() string { return string(f.code) }
