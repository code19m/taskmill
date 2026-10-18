package taskmill

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"slices"
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
	recordTimeout      = 30 * time.Second       // bound on recording one result
	cleanupTimeout     = 5 * time.Second        // bound on best-effort cleanup, such as closing a connection
	maxFetch           = 1000                   // most tasks one fetch leases
	completionBatch    = 500                    // most completions recorded in one statement
	completeAttempts   = 3                      // tries to record a batch of completions
	completeRetryDelay = 100 * time.Millisecond // grows with each try
	completionLinger   = 2 * time.Millisecond   // wait for more completions to batch with
	heldRetryDelay     = 20 * time.Millisecond  // first wait before retrying completions whose rows were held
	heldRetryMax       = time.Second            // longest wait before retrying them
	emptyFetchCooldown = 20 * time.Millisecond  // pause after a notification that found nothing
	errorMessageLimit  = 4096                   // bytes of an error message that are kept
)

// timing holds a worker's fixed intervals. They are not configurable, but
// tests shorten them.
type timing struct {
	maintainer    time.Duration // how often the maintainer works, at most
	election      time.Duration // how often workers that do not lead try to take over
	leaderLease   time.Duration // how long a maintainer leads without renewing
	retention     time.Duration // how often retention runs
	stats         time.Duration // how often queue statistics are refreshed
	settings      time.Duration // how often queue settings are reloaded
	shutdownGrace time.Duration // how long canceled attempts get to return before release
	unknownKind   time.Duration // how long a task of an unknown kind is snoozed
	reconnectMin  time.Duration // first wait before reconnecting the listener
	reconnectMax  time.Duration // longest wait before reconnecting the listener
}

var defaultTiming = timing{
	maintainer:    time.Second,
	election:      2 * time.Second,
	leaderLease:   10 * time.Second,
	retention:     time.Minute,
	stats:         10 * time.Second,
	settings:      10 * time.Second,
	shutdownGrace: 5 * time.Second,
	unknownKind:   time.Minute,
	reconnectMin:  time.Second,
	reconnectMax:  30 * time.Second,
}

// Causes of a canceled attempt context, which decide how its result is
// recorded.
var (
	errShutdown        = errors.New("taskmill: worker shutting down")
	errCancelRequested = errors.New("taskmill: task canceled")
	errAttemptTimeout  = errors.New("taskmill: attempt timed out")
)

// errListenConfig reports a ListenConfig that pgx cannot connect with.
var errListenConfig = errors.New("taskmill: invalid ListenConfig; create it with pgx.ParseConfig")

// Worker fetches tasks from its queues and runs their handlers. One worker per
// process is typical; it also takes part in electing the maintainer, which
// promotes due tasks, rescues expired leases, and applies retention.
type Worker struct {
	client    *Client
	cfg       WorkerConfig
	handlers  map[string]*registration
	timing    timing
	logger    *slog.Logger
	listen    *pgx.ConnConfig
	version   string
	schedules []compiledSchedule

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
	// Default: ExponentialBackoff{} (1 second doubling up to 1 hour). A task
	// whose worker lost its lease is retried with that default, whatever
	// this says: the maintainer that rescues it knows no worker's settings.
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
	// proxy, which cannot deliver them. Create it with [pgx.ParseConfig].
	// Default: the pool's configuration.
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
	err := resolveWorkerConfig(&c)
	if err != nil {
		return nil, err
	}

	w := &Worker{
		client:   client,
		cfg:      c,
		handlers: map[string]*registration{},
		timing:   defaultTiming,
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
		}
	}
	if w.listen == nil {
		w.listen = client.pool.Config().ConnConfig.Copy()
	}
	w.logger = w.logger.With(slog.String(logKeyWorker, c.ID))
	if w.schedules, err = compileSchedules(c.Schedules); err != nil {
		return nil, err
	}
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
	return checkName("worker ID", c.ID, maxNameLength)
}

// queueState is one queue that the worker serves.
type queueState struct {
	name       string
	slots      chan struct{} // one per running attempt or batch
	wake       chan struct{} // nudges the fetch loop: tasks may be available
	freed      chan struct{} // nudges the fetch loop: a slot was freed
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
	att    *attempt
	info   *TaskInfo
	reg    *registration // nil if no handler knows the kind
	direct bool          // run by RunTask, which records its completion at once

	// completedDirectly is set once RunTask recorded the completion.
	completedDirectly bool

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
			freed: make(chan struct{}, 1),
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
	case <-time.After(w.timing.shutdownGrace):
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

// RunTask runs one task now, in the calling goroutine, the way [Worker.Run]
// runs tasks: it leases the task, runs the middleware and the handler of its
// kind while renewing the lease, and records the outcome. It ignores the
// task's run time, queue pauses, and concurrency limits, but not FIFO order
// or deadlines. It does not need Run to be running, which makes it the tool
// for tests and for operators who need one task run now. If ctx is canceled
// before the handler returns, the handler's context is canceled too, and the
// task goes back to its queue without using an attempt.
//
// It returns the task afterwards. It returns [ErrNotFound] if no task has the
// ID, and [ErrInvalidState] if the task is running, waits behind its FIFO
// group, has finished, or its deadline passed.
func (w *Worker) RunTask(ctx context.Context, id int64) (*TaskInfo, error) {
	t, err := w.leaseTask(ctx, id)
	if err != nil {
		return nil, err
	}
	t.direct = true

	// Heartbeats renew the lease while the handler runs.
	beatCtx, stopBeats := context.WithCancel(context.WithoutCancel(ctx))
	var beats sync.WaitGroup
	beats.Go(func() {
		tick := time.NewTicker(w.cfg.LeaseDuration / 3)
		defer tick.Stop()
		for {
			select {
			case <-beatCtx.Done():
				return
			case <-tick.C:
				renewCtx, cancel := context.WithTimeout(beatCtx, w.cfg.LeaseDuration/3)
				w.renewLeases(renewCtx, []*tracked{t})
				cancel()
			}
		}
	})

	runCtx, stopRun := context.WithCancelCause(context.WithoutCancel(ctx))
	stopAfter := context.AfterFunc(ctx, func() { stopRun(errShutdown) })
	if t.reg != nil && t.reg.runBatch != nil {
		w.runBatch(runCtx, []*tracked{t})
	} else {
		w.runOne(runCtx, t)
	}
	stopAfter()
	stopRun(nil)
	stopBeats()
	beats.Wait()

	info, err := w.client.Get(context.WithoutCancel(ctx), id)
	if errors.Is(err, ErrNotFound) && t.completedDirectly {
		// The queue keeps no history: the task was deleted as it completed.
		done := *t.info
		done.State, done.FinalizedAt = StateCompleted, time.Now()
		return &done, nil
	}
	return info, err
}

// leaseTask leases one task for RunTask, or explains why it cannot.
func (w *Worker) leaseTask(ctx context.Context, id int64) (*tracked, error) {
	r, err := w.client.pool.Query(ctx, w.client.sql.leaseTask, id, w.cfg.LeaseDuration.Microseconds(), w.cfg.ID)
	if err != nil {
		return nil, fmt.Errorf("taskmill: run task %d: %w", id, err)
	}
	defer r.Close()
	if r.Next() {
		t, err := w.scanLeased(r, time.Now())
		if err != nil {
			return nil, fmt.Errorf("taskmill: run task %d: %w", id, err)
		}
		return t, nil
	}
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("taskmill: run task %d: %w", id, err)
	}
	info, err := w.client.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if !info.State.Finished() && info.State != StatePending && info.State != StateRunning {
		return nil, fmt.Errorf("%w: cannot run task %d: its deadline passed", ErrInvalidState, id)
	}
	return nil, fmt.Errorf("%w: cannot run task %d in state %s", ErrInvalidState, id, info.State)
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
