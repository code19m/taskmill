package taskmill

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// Worker defaults.
const (
	defaultLeaseDuration      = 30 * time.Second
	defaultPollInterval       = time.Second
	defaultDrainTimeout       = 30 * time.Second
	defaultUnknownKindTimeout = time.Hour
	defaultConcurrency        = 10

	minLeaseDuration   = 100 * time.Millisecond
	unknownKindDelay   = time.Minute
	settingsInterval   = 10 * time.Second
	shutdownGrace      = 5 * time.Second
	recordTimeout      = 30 * time.Second
	maxFetch           = 1000
	completionBatch    = 500
	errorMessageLimit  = 4096
	reconnectBackoffLo = time.Second
	reconnectBackoffHi = 30 * time.Second
)

// Causes of a canceled attempt context, which decide how its result is
// recorded.
var (
	errShutdown        = errors.New("taskmill: worker shutting down")
	errCancelRequested = errors.New("taskmill: task canceled")
	errAttemptTimeout  = errors.New("taskmill: attempt timed out")
)

// Worker fetches tasks from its queues and runs their handlers. One worker per
// process is typical; it also takes part in electing the maintainer, which
// promotes due tasks, rescues expired leases, and applies retention.
type Worker struct {
	client   *Client
	cfg      WorkerConfig
	handlers map[string]*registration
	hasBatch bool
	logger   *slog.Logger
	listen   *pgx.ConnConfig
	version  string

	running atomic.Bool

	mu       sync.Mutex
	attempts map[int64]*tracked
	queues   map[string]*queueState

	completions chan *tracked
	completerUp chan struct{} // closed when the completer stops
}

// WorkerConfig configures a [Worker]. Every field except Handlers and Queues
// has a default; a worker with no queues only takes part in maintenance.
type WorkerConfig struct {
	// Handlers runs the tasks this worker fetches.
	Handlers *Handlers

	// Queues lists the queues to fetch from, with per-process options.
	Queues map[string]QueueOptions

	// ID identifies this worker in the database and in logs.
	// Default: hostname, process ID, and a random suffix.
	ID string

	// LeaseDuration is how long a fetched task stays owned without a
	// heartbeat. Heartbeats are sent every third of it. Default: 30 seconds.
	LeaseDuration time.Duration

	// PollInterval is how often idle queues are checked when no notification
	// arrives. Default: 1 second.
	PollInterval time.Duration

	// DrainTimeout is how long Run waits for running tasks after its context
	// is canceled, before canceling their contexts and releasing them.
	// Default: 30 seconds.
	DrainTimeout time.Duration

	// UnknownKindTimeout is how long a task whose kind has no handler keeps
	// waiting for a worker that has one, before it becomes dead. This keeps
	// tasks safe during rolling deploys. Default: 1 hour.
	UnknownKindTimeout time.Duration

	// DefaultRetry is the retry policy for kinds that set none.
	// Default: ExponentialBackoff{} (1 second doubling up to 1 hour).
	DefaultRetry RetryPolicy

	// Middleware wraps every attempt, in order: the first is the outermost.
	Middleware []Middleware

	// Schedules are recurring tasks that the maintainer enqueues.
	Schedules []Schedule

	// NoMaintainer keeps this worker out of the maintainer election. At least
	// one worker of an installation must take part.
	NoMaintainer bool

	// ListenConfig is the connection that receives notifications. Set it to
	// a direct connection when the pool goes through a transaction-pooling
	// proxy, which cannot deliver them. Default: the pool's configuration.
	ListenConfig *pgx.ConnConfig

	// Logger receives the worker's logs. Default: the client's logger.
	Logger *slog.Logger
}

// QueueOptions configure how one worker process serves one queue.
// Cluster-wide settings, such as a concurrency limit, are [QueueSettings].
type QueueOptions struct {
	// Concurrency is how many of the queue's tasks this process runs at once.
	// A batch counts as one. Default: 10.
	Concurrency int
}

// Middleware wraps the execution of an attempt, for logging, metrics,
// tracing, or recovering from panics. It must call next at most once, and must
// not modify task. Returning without calling next fails the attempt with the
// returned error.
type Middleware func(ctx context.Context, task *TaskInfo, next func(context.Context) error) error

// NewWorker returns a worker that uses client to reach the database. It
// reports invalid configuration and handler registration errors.
func NewWorker(client *Client, cfg *WorkerConfig) (*Worker, error) {
	if client == nil {
		return nil, invalidArgument("client is nil")
	}
	if cfg == nil {
		cfg = &WorkerConfig{}
	}
	c := *cfg
	if err := resolveWorkerConfig(&c); err != nil {
		return nil, err
	}

	w := &Worker{
		client:   client,
		cfg:      c,
		handlers: map[string]*registration{},
		logger:   cmp.Or(c.Logger, client.logger),
		listen:   c.ListenConfig,
		version:  moduleVersion(),
	}
	if c.Handlers != nil {
		for kind, reg := range c.Handlers.byKind {
			r := *reg
			r.opts.Timeout = cmp.Or(r.opts.Timeout, defaultHandlerTimeout)
			r.opts.BatchSize = cmp.Or(r.opts.BatchSize, defaultBatchSize)
			if r.opts.Retry == nil {
				r.opts.Retry = c.DefaultRetry
			}
			w.handlers[kind] = &r
			w.hasBatch = w.hasBatch || r.runBatch != nil
		}
	}
	if w.listen == nil {
		w.listen = client.pool.Config().ConnConfig.Copy()
	}
	w.logger = w.logger.With(slog.String(logKeyWorker, c.ID))
	return w, nil
}

func resolveWorkerConfig(c *WorkerConfig) error {
	if c.Handlers != nil && c.Handlers.err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidArgument, c.Handlers.err)
	}
	if len(c.Queues) > 0 && (c.Handlers == nil || len(c.Handlers.byKind) == 0) {
		return invalidArgument("a worker with queues needs handlers")
	}
	queues := make(map[string]QueueOptions, len(c.Queues))
	for name, o := range c.Queues {
		if err := checkName("queue", name, maxNameLength); err != nil {
			return err
		}
		if o.Concurrency < 0 {
			return invalidArgument("queue %q: concurrency must not be negative", name)
		}
		o.Concurrency = cmp.Or(o.Concurrency, defaultConcurrency)
		queues[name] = o
	}
	c.Queues = queues

	if c.LeaseDuration == 0 {
		c.LeaseDuration = defaultLeaseDuration
	}
	if c.LeaseDuration < minLeaseDuration {
		return invalidArgument("lease duration must be at least %s", minLeaseDuration)
	}
	for _, d := range []time.Duration{c.PollInterval, c.DrainTimeout, c.UnknownKindTimeout} {
		if d < 0 {
			return invalidArgument("durations must not be negative")
		}
	}
	c.PollInterval = cmp.Or(c.PollInterval, defaultPollInterval)
	c.DrainTimeout = cmp.Or(c.DrainTimeout, defaultDrainTimeout)
	c.UnknownKindTimeout = cmp.Or(c.UnknownKindTimeout, defaultUnknownKindTimeout)
	if c.DefaultRetry == nil {
		c.DefaultRetry = ExponentialBackoff{}
	}
	if c.ID == "" {
		c.ID = defaultWorkerID()
	}
	if err := checkName("worker ID", c.ID, maxNameLength); err != nil {
		return err
	}
	return validateSchedules(c.Schedules)
}

// queueState is one queue that the worker serves.
type queueState struct {
	name       string
	slots      chan struct{} // one per running attempt or batch
	wake       chan struct{} // nudges the fetch loop
	paused     atomic.Bool
	maxRunning atomic.Int32 // cluster-wide limit; 0 means none
}

func (q *queueState) nudge() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// tracked is an attempt the worker is running or recording.
type tracked struct {
	att   *attempt
	info  *TaskInfo
	reg   *registration // nil if no handler knows the kind
	queue *queueState

	mu       sync.Mutex
	cancel   context.CancelCauseFunc // set once a single attempt starts
	state    trackedState
	lastBeat time.Time // last confirmation that the lease is held

	lost            atomic.Bool // the lease is gone: the result must be discarded
	cancelRequested atomic.Bool
}

type trackedState int

const (
	stateRunning   trackedState = iota // the handler is running
	stateRecording                     // the handler returned; its result is being recorded
	stateAbandoned                     // released at shutdown; the result is ignored
)

// Run fetches and runs tasks until ctx is canceled. It then stops fetching,
// waits up to DrainTimeout for running tasks, cancels and releases those still
// running, and returns. It returns nil after a clean shutdown.
func (w *Worker) Run(ctx context.Context) error {
	if !w.running.CompareAndSwap(false, true) {
		return errors.New("taskmill: worker is already running")
	}
	defer w.running.Store(false)

	// Work that must finish during shutdown runs on base, which keeps ctx's
	// values but not its cancellation.
	base := context.WithoutCancel(ctx)

	w.attempts = make(map[int64]*tracked)
	w.queues = make(map[string]*queueState, len(w.cfg.Queues))
	names := make([]string, 0, len(w.cfg.Queues))
	for name, o := range w.cfg.Queues {
		w.queues[name] = &queueState{
			name:  name,
			slots: make(chan struct{}, o.Concurrency),
			wake:  make(chan struct{}, 1),
		}
		names = append(names, name)
	}
	slices.Sort(names)

	hostname, _ := os.Hostname()
	if _, err := w.client.pool.Exec(ctx, w.client.sql.upsertWorker, w.cfg.ID, names, hostname, w.version); err != nil {
		return fmt.Errorf("taskmill: register worker: %w", err)
	}
	w.refreshQueueSettings(ctx)
	w.logger.LogAttrs(ctx, slog.LevelInfo, "worker started", slog.Any(logKeyQueues, names))

	w.completions = make(chan *tracked, completionBatch)
	w.completerUp = make(chan struct{})
	completerCtx, stopCompleter := context.WithCancel(base)
	go w.completer(completerCtx)

	loopsCtx, stopLoops := context.WithCancel(base)
	var loops sync.WaitGroup
	loops.Go(func() { w.listenLoop(loopsCtx) })
	loops.Go(func() { w.heartbeatLoop(loopsCtx) })
	loops.Go(func() { w.settingsLoop(loopsCtx) })
	if !w.cfg.NoMaintainer {
		loops.Go(func() { newMaintainer(w).run(loopsCtx) })
	}

	runCtx, stopRun := context.WithCancelCause(base)
	var inflight sync.WaitGroup
	var fetchers sync.WaitGroup
	for _, q := range w.queues {
		fetchers.Go(func() { w.fetchLoop(ctx, runCtx, q, &inflight) })
	}

	<-ctx.Done()
	fetchers.Wait()
	w.drain(base, &inflight, stopRun)

	stopCompleter()
	<-w.completerUp
	stopLoops()
	loops.Wait()

	stopCtx, cancel := context.WithTimeout(base, recordTimeout)
	defer cancel()
	if _, err := w.client.pool.Exec(stopCtx, w.client.sql.deleteWorker, w.cfg.ID); err != nil {
		w.logger.LogAttrs(stopCtx, slog.LevelWarn, "unregister worker failed", slog.Any(logKeyError, err))
	}
	w.logger.LogAttrs(stopCtx, slog.LevelInfo, "worker stopped")
	return nil
}

// drain waits for running attempts, then cancels them and releases the tasks
// of those that do not return in time.
func (w *Worker) drain(ctx context.Context, inflight *sync.WaitGroup, stopRun context.CancelCauseFunc) {
	done := make(chan struct{})
	go func() {
		inflight.Wait()
		close(done)
	}()

	select {
	case <-done:
		return
	case <-time.After(w.cfg.DrainTimeout):
	}

	stopRun(errShutdown)
	select {
	case <-done:
		return
	case <-time.After(shutdownGrace):
	}

	// Handlers that ignore their context keep running, but their results are
	// ignored and their tasks go back to the queue.
	w.mu.Lock()
	var abandoned []*tracked
	for _, t := range w.attempts {
		t.mu.Lock()
		if t.state == stateRunning {
			t.state = stateAbandoned
			abandoned = append(abandoned, t)
		}
		t.mu.Unlock()
	}
	w.mu.Unlock()
	for _, t := range abandoned {
		w.logger.LogAttrs(ctx, slog.LevelWarn, "handler ignored shutdown; releasing its task",
			slog.Int64(logKeyTaskID, t.att.id), slog.String(logKeyKind, t.info.Kind))
		w.record(ctx, t, outcome{kind: outcomeRelease})
	}
}

// fetchLoop fetches tasks for one queue whenever it has free capacity, until
// ctx is canceled.
func (w *Worker) fetchLoop(ctx, runCtx context.Context, q *queueState, inflight *sync.WaitGroup) {
	poll := time.NewTicker(w.cfg.PollInterval)
	defer poll.Stop()

	wait := func() bool {
		select {
		case <-ctx.Done():
			return false
		case <-q.wake:
		case <-poll.C:
		}
		return true
	}

	for ctx.Err() == nil {
		free := cap(q.slots) - len(q.slots)
		if q.paused.Load() || free == 0 {
			if !wait() {
				return
			}
			continue
		}
		limit := free
		if w.hasBatch {
			limit = min(free*maxBatchSize, maxFetch)
		}

		tasks, err := w.fetch(ctx, q, limit)
		if err != nil {
			w.logger.LogAttrs(ctx, slog.LevelError, "fetch failed",
				slog.String(logKeyQueue, q.name), slog.Any(logKeyError, err))
			if !wait() {
				return
			}
			continue
		}
		w.dispatch(ctx, runCtx, q, tasks, inflight)
		if len(tasks) < limit && !wait() {
			return
		}
	}
}

// fetch leases up to limit tasks of q. A queue with a cluster-wide
// concurrency limit fetches only up to its remaining capacity.
func (w *Worker) fetch(ctx context.Context, q *queueState, limit int) ([]*tracked, error) {
	// The statement runs on a context that outlives ctx: once tasks are
	// leased, they must reach the worker even if it is stopping.
	dbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recordTimeout)
	defer cancel()
	lease := w.cfg.LeaseDuration.Microseconds()

	var r pgx.Rows
	var tx pgx.Tx
	if q.maxRunning.Load() > 0 {
		var err error
		if tx, err = w.client.pool.Begin(dbCtx); err != nil {
			return nil, err
		}
		defer func() { _ = tx.Rollback(dbCtx) }() // a no-op after commit
		allowed, err := w.capacity(dbCtx, tx, q.name)
		if err != nil {
			return nil, err
		}
		if limit = min(limit, allowed); limit == 0 {
			return nil, nil
		}
		r, err = tx.Query(dbCtx, w.client.sql.fetch, q.name, limit, lease, w.cfg.ID)
		if err != nil {
			return nil, err
		}
	} else {
		var err error
		if r, err = w.client.pool.Query(dbCtx, w.client.sql.fetch, q.name, limit, lease, w.cfg.ID); err != nil {
			return nil, err
		}
	}

	now := time.Now()
	var out []*tracked
	for r.Next() {
		t, err := w.scanFetched(r, q, now)
		if err != nil {
			r.Close()
			return nil, err
		}
		out = append(out, t)
	}
	r.Close()
	if err := r.Err(); err != nil {
		return nil, err
	}
	if tx != nil {
		if err := tx.Commit(dbCtx); err != nil {
			return nil, err
		}
	}

	slices.SortFunc(out, func(a, b *tracked) int {
		return cmp.Or(
			cmp.Compare(b.info.Priority, a.info.Priority),
			a.info.RunAt.Compare(b.info.RunAt),
			cmp.Compare(a.info.ID, b.info.ID),
		)
	})
	return out, nil
}

// capacity locks a limited queue's row and returns how many more of its tasks
// may run. It counts running tasks in a statement of its own, after taking the
// lock, so that it sees what concurrent fetches committed.
func (w *Worker) capacity(ctx context.Context, tx pgx.Tx, queue string) (int, error) {
	var maxRunning *int
	if err := tx.QueryRow(ctx, w.client.sql.lockQueue, queue).Scan(&maxRunning); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return maxFetch, nil
		}
		return 0, err
	}
	if maxRunning == nil {
		return maxFetch, nil
	}
	var running int
	if err := tx.QueryRow(ctx, w.client.sql.countRunning, queue).Scan(&running); err != nil {
		return 0, err
	}
	return max(*maxRunning-running, 0), nil
}

func (w *Worker) scanFetched(r pgx.Rows, q *queueState, now time.Time) (*tracked, error) {
	var (
		info                    TaskInfo
		priority, att, maxAtt   int16
		token                   int32
		metadata, payload, errs []byte
		deadline, attemptedAt   *time.Time
	)
	err := r.Scan(&info.ID, &info.Kind, &priority, &att, &maxAtt, &token, &info.GroupKey,
		&metadata, &payload, &errs, &info.CreatedAt, &info.RunAt, &deadline, &attemptedAt)
	if err != nil {
		return nil, err
	}
	info.Queue = q.name
	info.State = StateRunning
	info.Priority = Priority(priority)
	info.Attempt = int(att)
	info.MaxAttempts = int(maxAtt)
	info.Payload = payload
	info.Deadline = deref(deadline)
	info.AttemptedAt = deref(attemptedAt)
	if err := decodeMetadata(metadata, &info.Metadata); err != nil {
		return nil, err
	}
	if err := decodeErrors(errs, &info.Errors); err != nil {
		return nil, err
	}
	return &tracked{
		att:      &attempt{client: w.client, id: info.ID, token: token},
		info:     &info,
		reg:      w.handlers[info.Kind],
		queue:    q,
		lastBeat: now,
	}, nil
}

// dispatch starts the fetched tasks: each one alone, or in batches for batch
// handlers. Each start takes a slot of the queue's concurrency. Tasks that
// cannot start before the worker stops go back to the queue.
func (w *Worker) dispatch(ctx, runCtx context.Context, q *queueState, tasks []*tracked, inflight *sync.WaitGroup) {
	if len(tasks) == 0 {
		return
	}
	w.mu.Lock()
	for _, t := range tasks {
		w.attempts[t.att.id] = t
	}
	w.mu.Unlock()

	for _, unit := range w.units(tasks) {
		select {
		case q.slots <- struct{}{}:
		case <-ctx.Done():
			for _, t := range unit {
				t.mu.Lock()
				t.state = stateRecording
				t.mu.Unlock()
				w.record(context.WithoutCancel(ctx), t, outcome{kind: outcomeRelease})
			}
			continue
		}
		inflight.Add(len(unit))
		go func() {
			defer func() {
				<-q.slots
				q.nudge()
			}()
			if len(unit) == 1 && (unit[0].reg == nil || unit[0].reg.run != nil) {
				w.runOne(runCtx, unit[0])
			} else {
				w.runBatch(runCtx, unit)
			}
			inflight.Add(-len(unit))
		}()
	}
}

// units splits fetched tasks into what runs together: each task alone, or
// tasks of a batch kind in batches of its batch size.
func (w *Worker) units(tasks []*tracked) [][]*tracked {
	var units [][]*tracked
	batches := map[string][]*tracked{}
	for _, t := range tasks {
		if t.reg != nil && t.reg.runBatch != nil {
			batches[t.info.Kind] = append(batches[t.info.Kind], t)
			continue
		}
		units = append(units, []*tracked{t})
	}
	for kind, ts := range batches {
		for chunk := range slices.Chunk(ts, w.handlers[kind].opts.BatchSize) {
			units = append(units, chunk)
		}
	}
	return units
}

// runOne runs a single attempt and records its outcome.
func (w *Worker) runOne(runCtx context.Context, t *tracked) {
	ctx, cancel := context.WithCancelCause(runCtx)
	defer cancel(nil)
	t.mu.Lock()
	t.cancel = cancel
	t.mu.Unlock()
	if t.cancelRequested.Load() {
		cancel(errCancelRequested)
	}
	if t.lost.Load() {
		cancel(ErrLeaseLost)
	}
	if t.reg == nil {
		w.finish(ctx, t, w.unknownKind(ctx, t), nil)
		return
	}

	ctx, cancelTimeout := context.WithTimeoutCause(ctx, t.reg.opts.Timeout, errAttemptTimeout)
	start := time.Now()
	err := w.invoke(ctx, t, func(ctx context.Context) error { return t.reg.run(ctx, t.info, t.att) })
	cause := context.Cause(ctx)
	cancelTimeout()
	w.finish(ctx, t, w.classify(ctx, t, err, cause), &start)
}

// runBatch runs a batch handler for tasks. Middleware still wraps each task:
// each one's chain runs in its own goroutine, and the batch starts once every
// chain has either reached the handler or returned early.
func (w *Worker) runBatch(runCtx context.Context, tasks []*tracked) {
	reg := tasks[0].reg
	ctx, cancel := context.WithCancelCause(runCtx)
	defer cancel(nil)
	ctx, cancelTimeout := context.WithTimeoutCause(ctx, reg.opts.Timeout, errAttemptTimeout)
	defer cancelTimeout()

	type arrival struct {
		index  int
		result chan error
	}
	arrivals := make(chan arrival, len(tasks))
	early := make(chan struct{}, len(tasks))
	errs := make([]error, len(tasks))
	var chains sync.WaitGroup
	start := time.Now()
	for i, t := range tasks {
		chains.Go(func() {
			var reached atomic.Bool
			errs[i] = w.invoke(ctx, t, func(context.Context) error {
				if reached.Swap(true) {
					return errors.New("taskmill: middleware called next twice")
				}
				a := arrival{index: i, result: make(chan error, 1)}
				arrivals <- a
				return <-a.result
			})
			if !reached.Load() {
				early <- struct{}{}
			}
		})
	}

	var arrived []arrival
	for pending := len(tasks); pending > 0; pending-- {
		select {
		case a := <-arrivals:
			arrived = append(arrived, a)
		case <-early:
		}
	}
	if len(arrived) > 0 {
		slices.SortFunc(arrived, func(a, b arrival) int { return cmp.Compare(a.index, b.index) })
		infos := make([]*TaskInfo, len(arrived))
		atts := make([]*attempt, len(arrived))
		for j, a := range arrived {
			infos[j], atts[j] = tasks[a.index].info, tasks[a.index].att
		}
		results := safeBatch(ctx, reg, infos, atts)
		for j, a := range arrived {
			a.result <- results[j]
		}
	}
	chains.Wait()

	cause := context.Cause(ctx)
	for i, t := range tasks {
		w.finish(ctx, t, w.classify(ctx, t, errs[i], cause), &start)
	}
}

// safeBatch calls a batch handler and turns a panic into an error for every
// task.
func safeBatch(ctx context.Context, reg *registration, infos []*TaskInfo, atts []*attempt) (results []error) {
	defer func() {
		if r := recover(); r != nil {
			err := panicFailure(r)
			results = make([]error, len(infos))
			for i := range results {
				results[i] = err
			}
		}
	}()
	return reg.runBatch(ctx, infos, atts)
}

// invoke runs call through the middleware chain, turning panics into errors.
func (w *Worker) invoke(ctx context.Context, t *tracked, call func(context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicFailure(r)
		}
	}()
	next := func(ctx context.Context) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = panicFailure(r)
			}
		}()
		return call(ctx)
	}
	for _, m := range slices.Backward(w.cfg.Middleware) {
		inner := next
		next = func(ctx context.Context) error { return m(ctx, t.info, inner) }
	}
	return next(ctx)
}

func panicFailure(r any) *failure {
	stack := debug.Stack()
	return &failure{code: codePanic, err: fmt.Errorf("panic: %v\n%s", r, stack)}
}

// outcome is how an attempt ended, and what to record for it.
type outcome struct {
	kind  outcomeKind
	delay time.Duration // before the next run, for retries and snoozes
	err   error
	code  string
}

type outcomeKind int

const (
	outcomeComplete outcomeKind = iota
	outcomeRetry
	outcomeDead
	outcomeSnooze
	outcomeCancel
	outcomeRelease
	outcomeDiscard // nothing to record: the lease is gone, or the handler completed the task itself
)

// classify turns a handler's result into the outcome to record.
func (w *Worker) classify(ctx context.Context, t *tracked, err, cause error) outcome {
	switch {
	case t.lost.Load():
		return outcome{kind: outcomeDiscard}
	case t.att.completedInTx.Load():
		if err != nil {
			w.logger.LogAttrs(ctx, slog.LevelWarn, "handler failed after completing its task",
				slog.Int64(logKeyTaskID, t.att.id), slog.Any(logKeyError, err))
		}
		return outcome{kind: outcomeDiscard}
	case err == nil:
		return outcome{kind: outcomeComplete}
	case t.cancelRequested.Load():
		return outcome{kind: outcomeCancel}
	case errors.Is(cause, errShutdown):
		return outcome{kind: outcomeRelease}
	}

	if s, ok := errors.AsType[*SnoozeError](err); ok {
		return outcome{kind: outcomeSnooze, delay: max(s.Delay, 0)}
	}

	o := outcome{kind: outcomeRetry, err: err}
	if f, ok := errors.AsType[*failure](err); ok {
		o.code = f.code
		if f.permanent {
			o.kind = outcomeDead
		}
	}
	if _, ok := errors.AsType[*PermanentError](err); ok {
		o.kind = outcomeDead
	}
	if errors.Is(cause, errAttemptTimeout) && o.code == "" {
		o.code = codeTimeout
	}
	if o.kind == outcomeRetry {
		if t.info.Attempt >= t.info.MaxAttempts {
			o.kind = outcomeDead
		} else {
			o.delay = t.reg.opts.Retry.Backoff(t.info.Attempt)
		}
	}
	return o
}

// unknownKind decides what to do with a task that no handler of this worker
// knows: wait for a worker that has one, for a while, then give up.
func (w *Worker) unknownKind(ctx context.Context, t *tracked) outcome {
	waited := time.Since(t.info.CreatedAt)
	if waited >= w.cfg.UnknownKindTimeout {
		return outcome{
			kind: outcomeDead,
			code: codeUnknownKind,
			err:  fmt.Errorf("no handler for kind %q appeared within %s", t.info.Kind, w.cfg.UnknownKindTimeout),
		}
	}
	w.logger.LogAttrs(ctx, slog.LevelWarn, "no handler for task kind; snoozing",
		slog.Int64(logKeyTaskID, t.att.id), slog.String(logKeyKind, t.info.Kind))
	return outcome{kind: outcomeSnooze, delay: min(unknownKindDelay, w.cfg.UnknownKindTimeout-waited)}
}

// finish records an attempt's outcome, unless shutdown already released its
// task.
func (w *Worker) finish(ctx context.Context, t *tracked, o outcome, start *time.Time) {
	t.mu.Lock()
	if t.state != stateRunning {
		t.mu.Unlock()
		return
	}
	t.state = stateRecording
	t.mu.Unlock()

	if start != nil && (o.kind == outcomeRetry || o.kind == outcomeDead) {
		attrs := []slog.Attr{
			slog.Int64(logKeyTaskID, t.att.id), slog.String(logKeyKind, t.info.Kind),
			slog.Int(logKeyAttempt, t.info.Attempt), slog.Duration(logKeyDuration, time.Since(*start)),
			slog.Any(logKeyError, o.err),
		}
		if o.kind == outcomeDead {
			w.logger.LogAttrs(ctx, slog.LevelWarn, "attempt failed; the task is dead", attrs...)
		} else {
			w.logger.LogAttrs(ctx, slog.LevelInfo, "attempt failed; retrying", attrs...)
		}
	}

	if o.kind == outcomeComplete {
		select {
		case w.completions <- t:
			return
		case <-w.completerUp:
			// The completer stopped: the lease expires and the task is
			// rescued, so it runs again rather than being lost.
			w.untrack(t)
			return
		}
	}
	w.record(context.WithoutCancel(ctx), t, o)
}

// record writes an outcome other than completion to the database.
func (w *Worker) record(ctx context.Context, t *tracked, o outcome) {
	defer w.untrack(t)
	if o.kind == outcomeDiscard {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, recordTimeout)
	defer cancel()

	err := w.client.inTx(ctx, func(q querier) error { return w.recordTx(ctx, q, t, o) })
	if err != nil {
		w.logger.LogAttrs(ctx, slog.LevelError, "record attempt failed; the task is rescued when its lease expires",
			slog.Int64(logKeyTaskID, t.att.id), slog.Any(logKeyError, err))
	}
}

func (w *Worker) recordTx(ctx context.Context, q querier, t *tracked, o outcome) error {
	sql := w.client.sql
	id, token := t.att.id, t.att.token
	var queue, group string
	var found bool
	var err error

	switch o.kind {
	case outcomeRetry, outcomeDead:
		state := StateRetryable
		if o.kind == outcomeDead {
			state = StateDead
		}
		found, err = queryRow(ctx, q, sql.fail, []any{id, token, string(state), o.delay.Microseconds(), errorJSON(o)},
			&queue, &group)
		if err == nil && found && state == StateDead && group != "" {
			err = w.client.leaveGroup(ctx, q, queue, group, id, true, true)
		}
	case outcomeCancel:
		found, err = queryRow(ctx, q, sql.cancelRunning, []any{id, token}, &queue, &group)
		if err == nil && found && group != "" {
			err = w.client.leaveGroup(ctx, q, queue, group, id, true, false)
		}
	case outcomeSnooze:
		found, err = queryRow(ctx, q, sql.snooze, []any{id, token, o.delay.Microseconds()}, &queue)
		if err == nil && found && o.delay == 0 {
			w.client.notifyAvailable(ctx, q, queue)
		}
	case outcomeRelease:
		found, err = queryRow(ctx, q, sql.release, []any{id, token}, &queue)
		if err == nil && found {
			w.client.notifyAvailable(ctx, q, queue)
		}
	case outcomeComplete, outcomeDiscard:
		return nil
	}
	if err == nil && !found {
		w.logger.LogAttrs(ctx, slog.LevelDebug, "result discarded: the attempt lost its lease",
			slog.Int64(logKeyTaskID, id))
	}
	return err
}

// completer records successful attempts in batches: it takes every result
// that is ready and completes them in one statement, so a busy worker needs
// far fewer round trips while an idle one adds no delay.
func (w *Worker) completer(ctx context.Context) {
	defer close(w.completerUp)
	batch := make([]*tracked, 0, completionBatch)
	for {
		select {
		case t := <-w.completions:
			batch = append(batch[:0], t)
		case <-ctx.Done():
			// Drain what finished before shutdown, then stop.
			for {
				select {
				case t := <-w.completions:
					w.flush(context.WithoutCancel(ctx), []*tracked{t})
				default:
					return
				}
			}
		}
	more:
		for len(batch) < completionBatch {
			select {
			case t := <-w.completions:
				batch = append(batch, t)
			default:
				break more
			}
		}
		w.flush(context.WithoutCancel(ctx), batch)
	}
}

// flush completes a batch of successful attempts, retrying briefly on errors.
func (w *Worker) flush(ctx context.Context, batch []*tracked) {
	defer func() {
		for _, t := range batch {
			w.untrack(t)
		}
	}()
	cs := make([]completion, len(batch))
	grouped := false
	for i, t := range batch {
		cs[i] = completion{id: t.att.id, token: t.att.token}
		grouped = grouped || t.info.GroupKey != ""
	}

	ctx, cancel := context.WithTimeout(ctx, recordTimeout)
	defer cancel()
	var err error
	for try := range 3 {
		if grouped {
			err = w.client.inTx(ctx, func(q querier) error {
				_, err := w.client.complete(ctx, q, cs)
				return err
			})
		} else {
			_, err = w.client.complete(ctx, pgxQuerier{q: w.client.pool}, cs)
		}
		if err == nil {
			return
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(try+1) * 100 * time.Millisecond):
		}
	}
	w.logger.LogAttrs(ctx, slog.LevelError, "complete tasks failed; they are rescued when their leases expire",
		slog.Int(logKeyTasks, len(batch)), slog.Any(logKeyError, err))
}

func (w *Worker) untrack(t *tracked) {
	w.mu.Lock()
	if w.attempts[t.att.id] == t {
		delete(w.attempts, t.att.id)
	}
	w.mu.Unlock()
}

// heartbeatLoop renews the leases of every tracked attempt, and cancels the
// attempts that lost theirs or whose tasks were canceled.
func (w *Worker) heartbeatLoop(ctx context.Context) {
	tick := time.NewTicker(w.cfg.LeaseDuration / 3)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			w.heartbeat(ctx)
		}
	}
}

func (w *Worker) heartbeat(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, w.cfg.LeaseDuration/3)
	defer cancel()

	w.mu.Lock()
	ts := make([]*tracked, 0, len(w.attempts))
	for _, t := range w.attempts {
		ts = append(ts, t)
	}
	w.mu.Unlock()

	if _, err := w.client.pool.Exec(ctx, w.client.sql.beatWorker, w.cfg.ID); err != nil {
		w.logger.LogAttrs(ctx, slog.LevelWarn, "worker heartbeat failed", slog.Any(logKeyError, err))
	}
	if len(ts) == 0 {
		return
	}

	ids := make([]int64, len(ts))
	tokens := make([]int32, len(ts))
	for i, t := range ts {
		ids[i], tokens[i] = t.att.id, t.att.token
	}
	sent := time.Now()
	owned, err := w.renew(ctx, ids, tokens)
	if err != nil {
		w.logger.LogAttrs(ctx, slog.LevelWarn, "lease heartbeat failed", slog.Any(logKeyError, err))
		// Without a heartbeat, an attempt cannot know that it still owns its
		// task once its lease could have expired.
		for _, t := range ts {
			t.mu.Lock()
			expired := time.Since(t.lastBeat) >= w.cfg.LeaseDuration
			t.mu.Unlock()
			if expired {
				w.loseLease(ctx, t)
			}
		}
		return
	}

	for _, t := range ts {
		cancelRequested, ok := owned[t.att.id]
		if !ok {
			w.loseLease(ctx, t)
			continue
		}
		t.mu.Lock()
		t.lastBeat = sent
		t.mu.Unlock()
		if cancelRequested {
			t.requestCancel()
		}
	}
}

// renew extends the leases of the given attempts and returns those it found,
// with whether their tasks' cancellation was requested.
func (w *Worker) renew(ctx context.Context, ids []int64, tokens []int32) (map[int64]bool, error) {
	r, err := w.client.pool.Query(ctx, w.client.sql.heartbeat, ids, tokens, w.cfg.LeaseDuration.Microseconds())
	if err != nil {
		return nil, err
	}
	defer r.Close()
	owned := make(map[int64]bool, len(ids))
	for r.Next() {
		var id int64
		var cancelRequested bool
		if err := r.Scan(&id, &cancelRequested); err != nil {
			return nil, err
		}
		owned[id] = cancelRequested
	}
	return owned, r.Err()
}

func (w *Worker) loseLease(ctx context.Context, t *tracked) {
	t.mu.Lock()
	running := t.state == stateRunning
	t.mu.Unlock()
	if !running || t.lost.Swap(true) {
		return
	}
	w.logger.LogAttrs(ctx, slog.LevelWarn, "attempt lost its lease; canceling it",
		slog.Int64(logKeyTaskID, t.att.id), slog.String(logKeyKind, t.info.Kind))
	t.cancelAttempt(ErrLeaseLost)
}

// requestCancel cancels the attempt because its task's cancellation was
// requested.
func (t *tracked) requestCancel() {
	if !t.cancelRequested.Swap(true) {
		t.cancelAttempt(errCancelRequested)
	}
}

// cancelAttempt cancels the attempt's context, if it has started. Batches
// keep running: a canceled or lost task's result is handled when it ends.
func (t *tracked) cancelAttempt(cause error) {
	t.mu.Lock()
	cancel := t.cancel
	t.mu.Unlock()
	if cancel != nil {
		cancel(cause)
	}
}

// settingsLoop reloads the cluster-wide settings of the worker's queues.
func (w *Worker) settingsLoop(ctx context.Context) {
	tick := time.NewTicker(settingsInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			w.refreshQueueSettings(ctx)
		}
	}
}

func (w *Worker) refreshQueueSettings(ctx context.Context) {
	if len(w.queues) == 0 {
		return
	}
	names := make([]string, 0, len(w.queues))
	for name := range w.queues {
		names = append(names, name)
	}
	r, err := w.client.pool.Query(ctx, w.client.sql.queueSettings, names)
	if err != nil {
		w.logger.LogAttrs(ctx, slog.LevelWarn, "load queue settings failed", slog.Any(logKeyError, err))
		return
	}
	defer r.Close()
	seen := make(map[string]bool, len(names))
	for r.Next() {
		var name string
		var paused bool
		var maxRunning int32
		if err := r.Scan(&name, &paused, &maxRunning); err != nil {
			return
		}
		if q := w.queues[name]; q != nil {
			seen[name] = true
			wasPaused := q.paused.Swap(paused)
			q.maxRunning.Store(maxRunning)
			if wasPaused && !paused {
				q.nudge()
			}
		}
	}
	for name, q := range w.queues {
		if !seen[name] && q.paused.Swap(false) {
			q.nudge()
		}
		if !seen[name] {
			q.maxRunning.Store(0)
		}
	}
}

// listenLoop receives notifications on a dedicated connection, reconnecting
// with backoff when it breaks.
func (w *Worker) listenLoop(ctx context.Context) {
	backoff := reconnectBackoffLo
	for ctx.Err() == nil {
		err := w.listenOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		w.logger.LogAttrs(ctx, slog.LevelWarn, "notification connection lost; polling until it is back",
			slog.Any(logKeyError, err), slog.Duration(logKeyRetryIn, backoff))
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, reconnectBackoffHi)
	}
}

func (w *Worker) listenOnce(ctx context.Context) error {
	conn, err := pgx.ConnectConfig(ctx, w.listen)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()
	if _, err := conn.Exec(ctx, "LISTEN "+w.client.schema); err != nil {
		return err
	}
	// Notifications may have been missed while disconnected.
	for _, q := range w.queues {
		q.nudge()
	}
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		w.handleNotification(ctx, n.Payload)
	}
}

func (w *Worker) handleNotification(ctx context.Context, payload string) {
	kind, arg, ok := strings.Cut(payload, "|")
	if !ok {
		return
	}
	switch kind {
	case "a":
		if q := w.queues[arg]; q != nil {
			q.nudge()
		}
	case "c":
		var id int64
		if _, err := fmt.Sscan(arg, &id); err != nil {
			return
		}
		w.mu.Lock()
		t := w.attempts[id]
		w.mu.Unlock()
		if t != nil {
			t.requestCancel()
		}
	case "q":
		if w.queues[arg] != nil {
			w.refreshQueueSettings(ctx)
		}
	}
}

// errorJSON encodes an attempt's error for the errors column.
func errorJSON(o outcome) string {
	msg := "unknown error"
	if o.err != nil {
		msg = o.err.Error()
	}
	if len(msg) > errorMessageLimit {
		msg = strings.ToValidUTF8(msg[:errorMessageLimit], "") + "…"
	}
	e := struct {
		Message string `json:"message"`
		Code    string `json:"code,omitempty"`
	}{msg, o.code}
	b, err := json.Marshal(e)
	if err != nil {
		return `{"message": "error could not be encoded"}`
	}
	return string(b)
}

func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "worker"
	}
	var b [3]byte
	_, _ = rand.Read(b[:]) // never returns an error
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(b[:]))
}

// moduleVersion returns taskmill's version as built into the binary.
func moduleVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	if info.Main.Path == "github.com/code19m/taskmill" {
		return info.Main.Version
	}
	for _, dep := range info.Deps {
		if dep.Path == "github.com/code19m/taskmill" {
			return dep.Version
		}
	}
	return "unknown"
}
