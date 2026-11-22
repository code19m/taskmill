package taskmill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/code19m/taskmill/internal/argkind"
)

// Handler runs tasks of one kind.
//
// Returning nil completes the task. Returning an error fails the attempt: the
// task is retried with backoff until it runs out of attempts. Wrap the error
// with [Permanent] to make the task dead right away, or return [Snooze] to
// run it again later without using an attempt.
//
// Handle must return promptly once ctx is canceled. [context.Cause] says
// why: [ErrTaskCanceled], [ErrLeaseLost], [ErrAttemptTimeout], or
// [ErrShutdown]. A task whose lease ended before its attempt began, as on a
// slow link to the database, reaches neither Handle nor the middleware.
//
// Register a handler with [AddHandler]. The [guide to handling tasks] says
// what a handler should return, and what happens on a panic or a timeout.
//
// [guide to handling tasks]: https://github.com/code19m/taskmill/blob/main/docs/guides/handle-tasks.md
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
// a [Handler]'s error would.
//
// A task without a result fails, as after an error: HandleBatch returned nil,
// or fewer errors than tasks. So a handler that returns early completes no
// task that it did not run, and one that did all its work returns
// make([]error, len(tasks)), never nil.
//
// A batch takes one slot of its queue's concurrency. When a worker fetches a
// task of a batch kind, it fills that task's batch with more available tasks
// of the kind, so a batch kind can share a queue with other kinds.
//
// ctx is canceled when the batch times out or the worker shuts down, with
// the cause [ErrAttemptTimeout] or [ErrShutdown], but not when one of its
// tasks is canceled or loses its lease: the batch runs to the end. A task's
// [Task.CancelRequested] reports a cancel request, so that the batch can
// skip that task's work. A canceled task becomes completed if the batch
// returned nil for it, because the work was done, and canceled otherwise;
// the result of a task that lost its lease is discarded.
//
// Register a batch handler with [AddBatchHandler]. The [batch guide] shows
// one, and says when a batch is worth it.
//
// [batch guide]: https://github.com/code19m/taskmill/blob/main/docs/guides/batch-handlers.md
type BatchHandler[A Args] interface {
	HandleBatch(ctx context.Context, tasks []*Task[A]) []error
}

// HandlerOptions configure how a worker runs one kind. Pass them to
// [AddHandler] or [AddBatchHandler]. The zero value keeps the defaults.
// [NewWorker] reports a value outside its limits, as an error that wraps
// [ErrInvalidArgument].
//
// The [guide to handling tasks] shows the options at work.
//
// [guide to handling tasks]: https://github.com/code19m/taskmill/blob/main/docs/guides/handle-tasks.md
type HandlerOptions struct {
	// Timeout bounds each attempt, or each batch: the handler's context is
	// canceled with [ErrAttemptTimeout] then. A handler that has not returned
	// 5 seconds after its context was canceled, by this timeout, a cancel of
	// its task, or a lost lease, is abandoned: its attempt ends as the cause
	// says, its task is retried or canceled, and its result is ignored. Its
	// goroutine keeps its slot until it returns, and it may still finish its
	// work, while the task already runs again. The context of a batch
	// handler ends only with this timeout, so a batch is abandoned only
	// after it: each of its tasks fails with [CodeTimeout]. In a queue with
	// a [QueueSettings.MaxRunning] limit, only an attempt that lost its lease
	// is abandoned, since its task may run again anyway. Any other task there
	// stays running until the handler returns, and the worker logs a
	// warning: a task that ran again would break the limit. It must not be
	// negative. Default: 1 minute.
	Timeout time.Duration

	// Retry decides the delay before each retry. A task whose worker lost its
	// lease is rescued by the maintainer, the worker that leads maintenance:
	// it is retried with this policy if that worker has a handler for the
	// kind, and with the default otherwise. Default: [ExponentialBackoff]{}.
	Retry RetryPolicy

	// BatchSize is the most tasks a [BatchHandler] receives in one call. It
	// must not be negative. Default: 100. Maximum: 1,000. Ignored for other
	// handlers.
	BatchSize int

	// DiscardCompleted deletes completed tasks of the kind right away,
	// instead of keeping them in history, for high-volume kinds whose history
	// nobody reads.
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
		return nil, &failure{code: CodeInvalidPayload, permanent: true, err: decodeError[A](err)}
	}
	return &Task[A]{TaskInfo: *info, Args: args, attempt: a}, nil
}

// decodeError says why a payload did not decode into A, without a value of
// the payload: the error is stored with the task, logged, and set as a
// span's status, where payloads must not appear. encoding/json quotes the
// numbers and characters that it rejects, and an UnmarshalJSON method of
// the application may quote anything, so only the field, the JSON type, the
// Go type, and the place remain.
func decodeError[A Args](err error) error {
	if e, ok := errors.AsType[*json.UnmarshalTypeError](err); ok {
		kind, _, _ := strings.Cut(e.Value, " ") // "number -5" holds the number
		if field := fieldPath(reflect.TypeFor[A](), e.Field); field != "" {
			return fmt.Errorf("decode payload: the field %s holds a JSON %s, which does not fit %s", field, kind, e.Type)
		}
		return fmt.Errorf("decode payload: a JSON %s does not fit %s", kind, e.Type)
	}
	if e, ok := errors.AsType[*json.SyntaxError](err); ok {
		return fmt.Errorf("decode payload: the JSON is not valid at byte %d", e.Offset)
	}
	return fmt.Errorf("decode payload: it does not decode into %s", reflect.TypeFor[A]())
}

// fieldPath returns path, the path of a field in t as encoding/json reports
// it, without the values of the payload that it may hold: the keys of maps.
// Releases of Go differ in what the path holds: some put in the key of
// each map and the index of each list, others leave both out. So fieldPath
// follows t along the names of the fields, skips the indexes, and ends at
// a map with "[key]": a key may hold dots, so what follows it cannot be
// told from the key. It also ends at a name that t does not have. The
// result is the same on every release: "items.qty", or "scores[key]".
func fieldPath(t reflect.Type, path string) string {
	var names []string
	indexes := 0 // the lists passed since the last name, whose indexes may come next
	for name := range strings.SplitSeq(path, ".") {
		var lists int
		t, lists = elements(t)
		indexes += lists
		if _, err := strconv.Atoi(name); err == nil && indexes > 0 {
			indexes--
			continue
		}
		indexes = 0

		if t.Kind() == reflect.Map {
			return strings.Join(names, ".") + "[key]"
		}
		f, ok := jsonField(t, name)
		if !ok {
			return strings.Join(names, ".")
		}
		names = append(names, name)
		t = f.Type
	}

	// A path that leaves keys out ends at the map.
	if t, _ = elements(t); t.Kind() == reflect.Map {
		return strings.Join(names, ".") + "[key]"
	}
	return strings.Join(names, ".")
}

// elements returns the type that t holds through pointers, lists, and
// arrays, and how many lists and arrays it passed.
func elements(t reflect.Type) (reflect.Type, int) {
	lists := 0
	for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		if t.Kind() != reflect.Pointer {
			lists++
		}
		t = t.Elem()
	}
	return t, lists
}

// jsonField returns the field of t, a struct, that encoding/json names name
// in a path: its name in JSON, or the Go name of an embedded struct.
func jsonField(t reflect.Type, name string) (reflect.StructField, bool) {
	if t.Kind() != reflect.Struct {
		return reflect.StructField{}, false
	}
	for f := range t.Fields() {
		tag := f.Tag.Get("json")
		jsonName, _, _ := strings.Cut(tag, ",")
		switch {
		case tag == "-":
		case jsonName == name, jsonName == "" && f.Name == name, f.Anonymous && f.Name == name:
			return f, true
		}
	}
	return reflect.StructField{}, false
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

// RetryPolicy decides how long to wait before retrying a failed attempt. Set
// one for a kind in [HandlerOptions.Retry]. The default is the zero value of
// [ExponentialBackoff].
//
// The worker calls the policy each time a failed attempt leads to a retry,
// before it records the failure. The maintainer calls it too, in the
// transaction that rescues the tasks whose workers lost their leases. So
// Backoff must return fast, with no I/O.
//
// The [guide to retries] shows how to change the wait, and what happens to a
// task with no attempts left.
//
// [guide to retries]: https://github.com/code19m/taskmill/blob/main/docs/guides/retries-and-errors.md
type RetryPolicy interface {
	// Backoff returns the delay before the retry that follows the given
	// failed attempt, counting from 1. A delay of zero or less makes the
	// task due right away: the maintainer makes it available within about a
	// second. If Backoff panics, the attempt fails with the code [CodePanic]
	// instead of the handler's error, and the task is retried after the
	// default backoff.
	Backoff(attempt int) time.Duration
}

// ExponentialBackoff doubles the delay after each failed attempt, from Base up
// to Max, and spreads retries out with ±10% random jitter, so a delay can
// reach 10% above Max. The zero value uses a base of 1 second and a maximum
// of 1 hour: the delays are 1, 2, 4, and 8 seconds, and so on, and 1 hour
// from the 13th failed attempt on.
type ExponentialBackoff struct {
	// Base is the delay after the first failed attempt. Zero or less means
	// the default, 1 second.
	Base time.Duration

	// Max is the longest delay, before jitter. Zero or less means the
	// default, 1 hour.
	Max time.Duration
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
