package taskmill

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
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
	defaultLeaseDuration = 30 * time.Second
	defaultPollInterval  = time.Second
	defaultDrainTimeout  = 20 * time.Second
	defaultConcurrency   = 10

	minLeaseDuration   = 100 * time.Millisecond
	recordTimeout      = 30 * time.Second       // bound on one try to record a result
	recordRetryFirst   = 100 * time.Millisecond // first wait before trying to record a result again
	recordRetryMax     = 2 * time.Second        // longest wait between those tries
	cleanupTimeout     = 5 * time.Second        // bound on best-effort cleanup, such as closing a connection
	maxFetch           = 1000                   // most tasks one fetch leases
	completionBatch    = 500                    // most completions recorded in one statement
	completionLinger   = 2 * time.Millisecond   // wait for more completions to batch with
	heldRetryDelay     = 20 * time.Millisecond  // first wait before retrying completions whose rows were held
	heldRetryMax       = time.Second            // longest wait before retrying them
	emptyFetchCooldown = 20 * time.Millisecond  // pause after a notification that found nothing
	errorMessageLimit  = 4096                   // bytes of an error message that are kept
)

// timing holds a worker's fixed intervals. They are not configurable, but
// tests shorten them.
type timing struct {
	maintainer      time.Duration // how often the maintainer works, at most
	heartbeat       time.Duration // how often the maintainer sends its heartbeat
	election        time.Duration // how often workers that do not lead try to take over
	leaderLease     time.Duration // how long a maintainer leads without renewing
	retention       time.Duration // how often retention runs
	stats           time.Duration // how often queue statistics are refreshed
	activity        time.Duration // how often a worker writes the attempts it counted
	settings        time.Duration // how often queue settings are reloaded
	shutdownGrace   time.Duration // how long canceled attempts get to return before release
	noHandlerSnooze time.Duration // how long a task of an unknown kind is snoozed at a time
	noHandlerWait   time.Duration // how long it waits for a handler before it becomes dead
	reconnectMin    time.Duration // first wait before reconnecting the listener
	reconnectMax    time.Duration // longest wait before reconnecting the listener
	failureReport   time.Duration // how often an operation that keeps failing logs, at most
	idlePoll        time.Duration // how often idle queues poll while the maintainer's heartbeats arrive
	listen          time.Duration // bound on opening the notification connection, and on its answer when heartbeats stop
}

var defaultTiming = timing{
	maintainer:      time.Second,
	heartbeat:       5 * time.Second,
	election:        2 * time.Second,
	leaderLease:     10 * time.Second,
	retention:       time.Minute,
	stats:           10 * time.Second,
	activity:        10 * time.Second,
	settings:        10 * time.Second,
	shutdownGrace:   5 * time.Second,
	noHandlerSnooze: time.Minute,
	noHandlerWait:   time.Hour,
	reconnectMin:    time.Second,
	reconnectMax:    30 * time.Second,
	failureReport:   time.Minute,
	idlePoll:        time.Minute,
	listen:          10 * time.Second,
}

// errListenConfig reports a ListenConfig that pgx cannot connect with.
var errListenConfig = errors.New("taskmill: invalid ListenConfig; create it with pgx.ParseConfig")

// Worker fetches tasks from its queues and runs their handlers. One worker per
// process is typical; it also takes part in electing the maintainer, which
// promotes due tasks, rescues expired leases, runs schedules, and applies
// retention.
type Worker struct {
	client    *Client
	cfg       WorkerConfig
	id        string
	host      string
	handlers  map[string]*registration
	queues    map[string]*queueState
	timing    timing
	logger    *slog.Logger
	listen    *pgx.ConnConfig
	version   string
	schedules []compiledSchedule

	running   atomic.Bool
	reached   atomic.Pointer[time.Time] // when Run last reached the database; nil while it does not run
	released  atomic.Int64              // tasks given back during the current Run's shutdown
	notifying atomic.Bool               // the maintainer's heartbeats arrive, so notifications reach the worker
	resigned  chan struct{}             // the maintainer resigned: this worker's maintainer tries to take over now
	silent    atomic.Bool               // the worker logged that heartbeats do not arrive, and none has since
	tally     tally                     // ended attempts not yet added to the activity
	recording sync.WaitGroup            // Run's attempts whose results are being recorded

	mu       sync.Mutex
	attempts map[int64]*tracked // attempts that Run and RunTask are running or recording

	completions   chan *tracked // successful attempts of Run, for the completer
	completerDone chan struct{} // closed when the current Run's completer returns

	poolWarned atomic.Int64 // when renewing leases last warned that the pool was full, in Unix nanoseconds

	// The failures of operations that the worker repeats, which it logs
	// only now and then. Leases and results are renewed and recorded by
	// RunTask too; Run's loops get theirs anew for each run, and each
	// queue's fetches and the maintainer's work have their own.
	leaseFailures, resultFailures                                       *failures
	checkInFailures, settingsFailures, activityFailures, listenFailures *failures
}

// WorkerConfig configures a [Worker]. The zero value is valid: a worker
// without handlers only takes part in maintenance.
type WorkerConfig struct {
	// Handlers runs the tasks this worker fetches.
	Handlers *Handlers

	// Queues lists the queues to fetch from, with per-process options.
	// Default: the default queue of every registered kind, from
	// [ArgsWithDefaults] or else [DefaultQueue], with default options. If
	// Queues leaves out a registered kind's default queue, Run logs a warning
	// when it starts: tasks enqueued with the kind's defaults would wait for
	// another worker.
	Queues map[string]QueueOptions

	// LeaseDuration is how long a fetched task stays owned without a
	// renewal. The worker checks in every third of it, and renews the
	// leases of its running tasks then. Minimum: 100 milliseconds. Default:
	// 30 seconds.
	LeaseDuration time.Duration

	// PollInterval is how often idle queues are checked while the
	// maintainer's heartbeats do not reach the worker: until the first one,
	// within 5 seconds of the start, once none came for 15 seconds, and
	// while no maintainer of this version or later runs. While they arrive,
	// every 5 seconds, notifications wake the worker, and the maintainer
	// wakes each queue whose next task waited more than a second, so idle
	// queues are checked only once a minute. So a task whose notification
	// came too early or was lost starts within about 2 seconds while
	// heartbeats arrive, within about 16 seconds if the maintainer or the
	// notification connection fails without a word, and within PollInterval
	// while no maintainer of this version runs. Default: 1 second.
	PollInterval time.Duration

	// DrainTimeout is how long Run waits for running tasks after its context
	// is canceled, before canceling their contexts and, 5 seconds later,
	// releasing them. Keep it at least 10 seconds below the time the platform
	// gives a stopping process. Default: 20 seconds.
	DrainTimeout time.Duration

	// Middleware wraps every attempt, in order: the first is the outermost.
	Middleware []Middleware

	// Schedules are recurring tasks that the maintainer enqueues.
	Schedules []Schedule

	// NoMaintainer keeps this worker out of the maintainer election. At least
	// one worker of an installation must take part.
	NoMaintainer bool

	// ListenConfig is the connection that receives notifications. Set it to
	// a direct connection when the pool goes through a transaction-pooling
	// proxy, which cannot deliver them: without it, the worker finds that
	// out when it starts, logs an error, and polls every PollInterval.
	// Create it with [pgx.ParseConfig]. Default: the pool's configuration.
	ListenConfig *pgx.ConnConfig
}

// QueueOptions configure how one worker process serves one queue.
// Installation-wide settings, such as a concurrency limit, are
// [QueueSettings].
type QueueOptions struct {
	// Concurrency is how many of the queue's tasks this process runs at once.
	// A batch counts as one. Default: 10.
	Concurrency int
}

// Middleware wraps the execution of an attempt, for logging, metrics, or
// tracing. It must call next at most once, and must not modify task.
// Returning without calling next fails the attempt with the returned error,
// or with an error that says so if it returns nil: the task is retried with
// backoff, as after a handler's error, and the middleware outside sees that
// error from its next.
// A handler's panic reaches middleware as an error. For a batch handler,
// middleware runs once per task, but the context it passes to next does not
// reach the handler.
type Middleware func(ctx context.Context, task *TaskInfo, next func(context.Context) error) error

// NewWorker returns a worker that uses client to reach the database. It
// reports invalid configuration, schedules, and handler registrations, such
// as a kind registered twice, as errors that wrap [ErrInvalidArgument].
func NewWorker(client *Client, cfg *WorkerConfig) (*Worker, error) {
	if client == nil {
		return nil, invalidArgument("client is nil")
	}
	if cfg == nil {
		cfg = &WorkerConfig{}
	}
	c := *cfg
	c.Middleware = slices.Clone(c.Middleware)
	if err := resolveWorkerConfig(&c); err != nil {
		return nil, err
	}

	host, _ := os.Hostname()
	id := workerID(host)
	w := &Worker{
		client:      client,
		cfg:         c,
		id:          id,
		host:        host,
		handlers:    map[string]*registration{},
		queues:      make(map[string]*queueState, len(c.Queues)),
		timing:      defaultTiming,
		logger:      client.logger.With(slog.String(logKeyWorker, id)),
		listen:      c.ListenConfig,
		version:     moduleVersion(),
		attempts:    make(map[int64]*tracked),
		resigned:    make(chan struct{}, 1),
		completions: make(chan *tracked, completionBatch),
	}
	if c.Handlers != nil {
		for kind, reg := range c.Handlers.byKind {
			r := *reg
			r.opts.Timeout = cmp.Or(r.opts.Timeout, defaultHandlerTimeout)
			r.opts.BatchSize = cmp.Or(r.opts.BatchSize, defaultBatchSize)
			if r.opts.Retry == nil {
				r.opts.Retry = ExponentialBackoff{}
			}
			w.handlers[kind] = &r
		}
	}
	for name, o := range c.Queues {
		w.queues[name] = &queueState{
			name:  name,
			slots: make(chan struct{}, o.Concurrency),
			wake:  make(chan struct{}, 1),
			freed: make(chan struct{}, 1),
		}
	}
	if w.listen == nil {
		w.listen = client.pool.Config().ConnConfig.Copy()
	}
	w.leaseFailures = newFailures(w, "renew leases")
	w.resultFailures = newFailures(w, "record results")
	var err error
	if w.schedules, err = compileSchedules(c.Schedules); err != nil {
		return nil, err
	}
	return w, nil
}

func resolveWorkerConfig(c *WorkerConfig) error {
	var kinds map[string]*registration
	if c.Handlers != nil {
		if c.Handlers.err != nil {
			return c.Handlers.err
		}
		kinds = c.Handlers.byKind
	}
	if len(c.Queues) > 0 && len(kinds) == 0 {
		return invalidArgument("a worker with queues needs handlers")
	}
	queues := make(map[string]QueueOptions)
	if len(c.Queues) == 0 {
		for _, reg := range kinds {
			queues[reg.queue] = QueueOptions{Concurrency: defaultConcurrency}
		}
	}
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

	switch {
	case c.LeaseDuration != 0 && c.LeaseDuration < minLeaseDuration:
		return invalidArgument("LeaseDuration must be at least %s", minLeaseDuration)
	case c.PollInterval < 0:
		return invalidArgument("PollInterval must not be negative")
	case c.DrainTimeout < 0:
		return invalidArgument("DrainTimeout must not be negative")
	}
	c.LeaseDuration = cmp.Or(c.LeaseDuration, defaultLeaseDuration)
	c.PollInterval = cmp.Or(c.PollInterval, defaultPollInterval)
	c.DrainTimeout = cmp.Or(c.DrainTimeout, defaultDrainTimeout)
	return nil
}

// warnUnservedKinds logs the registered kinds whose default queue the worker
// does not serve: tasks enqueued with their defaults wait for another worker.
func (w *Worker) warnUnservedKinds(ctx context.Context) {
	var kinds []string
	for kind, reg := range w.handlers {
		if w.queues[reg.queue] == nil {
			kinds = append(kinds, kind+" (queue "+reg.queue+")")
		}
	}
	if len(kinds) == 0 {
		return
	}
	slices.Sort(kinds)
	w.logger.LogAttrs(ctx, slog.LevelWarn,
		"the worker does not serve the default queue of these registered kinds", slog.Any(logKeyKinds, kinds))
}

// queueState is one queue that the worker serves.
type queueState struct {
	name       string
	slots      chan struct{} // one per running attempt or batch
	wake       chan struct{} // nudges the fetch loop: tasks may be available
	freed      chan struct{} // nudges the fetch loop: a slot was freed
	paused     atomic.Bool
	maxRunning atomic.Int32 // installation-wide limit; 0 means none
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

	mu        sync.Mutex
	cancel    context.CancelCauseFunc // set once a single attempt starts
	state     trackedState
	renewedAt time.Time // last confirmation that the lease is held

	lost            atomic.Bool // the lease is gone: the result must be discarded
	cancelRequested atomic.Bool
}

type trackedState int

const (
	stateRunning   trackedState = iota // the handler is running
	stateRecording                     // the handler returned; its result is being recorded
	stateAbandoned                     // released at shutdown; the result is ignored
)

// run is the state of one call to [Worker.Run].
type run struct {
	attemptCtx context.Context // parent of every attempt; canceled with ErrShutdown once DrainTimeout passes
	inflight   sync.WaitGroup  // the running attempts and batches
}

// register lists the worker in the workers table, or lists it again, and
// returns its queues' names.
func (w *Worker) register(ctx context.Context) ([]string, error) {
	names := make([]string, 0, len(w.queues)) // not nil: the column holds an array
	names = slices.AppendSeq(names, maps.Keys(w.queues))
	slices.Sort(names)
	kinds := slices.Sorted(maps.Keys(w.handlers))
	if kinds == nil {
		kinds = []string{} // the column holds an array
	}
	_, err := w.client.pool.Exec(ctx, w.client.sql.upsertWorker, w.id, names, w.host, w.version, kinds)
	return names, err
}

// prepareHistory creates the history partitions that the next week lacks,
// and logs a failure. Each worker does it when it starts, because a schema
// applied with Migrations has none, and completions may come before the
// maintainer's first pass, or from workers that never lead.
func (w *Worker) prepareHistory(ctx context.Context) {
	if err := ensureHistoryPartitions(ctx, w.client.pool, w.client.schema); err != nil && ctx.Err() == nil {
		w.logger.LogAttrs(ctx, slog.LevelWarn, "prepare history partitions failed", slog.Any(logKeyError, err))
	}
}

// Run fetches and runs tasks until ctx is canceled. It then stops fetching,
// waits up to DrainTimeout for running tasks, cancels and releases those
// still running, and returns nil. If ctx ends before the worker registered,
// it returns nil at once. It returns an error only if the worker cannot
// start: it is already running, which is [ErrInvalidState], or it cannot
// register in the database.
//
// Before it fetches, Run creates the history partitions that the next week
// lacks, as [Migrate] does, so that a schema applied with [Migrations] keeps
// the history of its first completed tasks. A failure is only logged: the
// maintainer tries again.
func (w *Worker) Run(ctx context.Context) error {
	if !w.running.CompareAndSwap(false, true) {
		return fmt.Errorf("%w: the worker is already running", ErrInvalidState)
	}
	defer w.running.Store(false)
	defer w.reached.Store(nil)

	// Work that must finish during shutdown runs on base, which keeps ctx's
	// values but not its cancellation.
	base := context.WithoutCancel(ctx)

	names, err := w.register(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil // stopped before it started
		}
		return fmt.Errorf("taskmill: register worker: %w", err)
	}
	now := time.Now()
	w.reached.Store(&now)
	w.checkInFailures = newFailures(w, "record check-in")
	w.settingsFailures = newFailures(w, "load queue settings")
	w.activityFailures = newFailures(w, "record activity")
	w.listenFailures = newFailures(w, "listen for notifications")
	w.refreshQueueSettings(ctx)
	w.prepareHistory(ctx)
	w.logger.LogAttrs(ctx, slog.LevelInfo, "worker started", slog.Any(logKeyQueues, names))
	w.warnUnservedKinds(ctx)

	w.released.Store(0)
	w.completerDone = make(chan struct{})
	completerCtx, stopCompleter := context.WithCancel(base)
	go w.completer(completerCtx)

	loopsCtx, stopLoops := context.WithCancel(base)
	var loops sync.WaitGroup
	loops.Go(func() { w.listenLoop(loopsCtx) })
	loops.Go(func() { w.checkInLoop(loopsCtx) })
	loops.Go(func() { w.activityLoop(loopsCtx) })
	loops.Go(func() { w.settingsLoop(loopsCtx) })
	if !w.cfg.NoMaintainer {
		loops.Go(func() { newMaintainer(w).run(loopsCtx) })
	}

	attemptCtx, stopAttempts := context.WithCancelCause(base)
	r := &run{attemptCtx: attemptCtx}
	var fetchers sync.WaitGroup
	for _, q := range w.queues {
		fetchers.Go(func() { w.fetchLoop(ctx, r, q) })
	}

	<-ctx.Done()
	fetchers.Wait()
	w.drain(base, r, stopAttempts)
	// Attempts that ended before drain gave up may still be recording their
	// results, through the completer too.
	w.recording.Wait()

	stopCompleter()
	<-w.completerDone
	stopLoops()
	loops.Wait()

	stopCtx, cancel := context.WithTimeout(base, recordTimeout)
	defer cancel()
	w.flushActivity(stopCtx)
	if _, err := w.client.pool.Exec(stopCtx, w.client.sql.deleteWorker, w.id); err != nil {
		w.logger.LogAttrs(stopCtx, slog.LevelWarn, "unregister worker failed", slog.Any(logKeyError, err))
	}
	w.logger.LogAttrs(stopCtx, slog.LevelInfo, "worker stopped", slog.Int64(logKeyReleased, w.released.Load()))
	return nil
}

// drain waits for running attempts, then cancels them and releases the tasks
// of those that do not return in time.
func (w *Worker) drain(ctx context.Context, r *run, stopAttempts context.CancelCauseFunc) {
	done := make(chan struct{})
	go func() {
		r.inflight.Wait()
		close(done)
	}()
	select {
	case <-done:
		return
	default:
	}
	w.logger.LogAttrs(ctx, slog.LevelInfo, "worker stopping; waiting for running tasks",
		slog.Int(logKeyTasks, w.tracking()), slog.Duration(logKeyTimeout, w.cfg.DrainTimeout))

	select {
	case <-done:
		return
	case <-time.After(w.cfg.DrainTimeout):
	}

	stopAttempts(ErrShutdown)
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
		if t.state == stateRunning && !t.direct {
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

// tracking returns how many attempts the worker tracks.
func (w *Worker) tracking() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.attempts)
}

// RunTask runs one task now, in the calling goroutine, the way [Worker.Run]
// runs tasks: it leases the task, runs the middleware and the handler of its
// kind while renewing the lease, and records the outcome. It ignores the
// task's run time, queue pauses, and concurrency limits, but not FIFO order.
// It does not need Run to be running, which makes it the tool for tests. If
// ctx is canceled before the handler returns, the handler's context is
// canceled too, and the task goes back to its queue without using an attempt.
//
// A task of a kind that the worker has no handler for is snoozed, with a
// no_handler note in its errors, as Run does, and returned.
//
// It returns the task afterwards. It returns [ErrNotFound] if no task has the
// ID, and [ErrInvalidState] if the task is running, waits behind its FIFO
// group, or has finished.
func (w *Worker) RunTask(ctx context.Context, id int64) (*TaskInfo, error) {
	t, err := w.leaseTask(ctx, id)
	if err != nil {
		return nil, err
	}
	t.direct = true
	w.track(t)

	// The lease is renewed every third of it while the handler runs, and
	// while its result is recorded.
	renewCtx, stopRenewing := context.WithCancel(context.WithoutCancel(ctx))
	var renewing sync.WaitGroup
	renewing.Go(func() {
		w.everyThirdOfLease(renewCtx, func(ctx context.Context) { w.renewLeases(ctx, []*tracked{t}) })
	})

	attemptCtx, stopAttempt := context.WithCancelCause(context.WithoutCancel(ctx))
	stopAfter := context.AfterFunc(ctx, func() { stopAttempt(ErrShutdown) })
	if t.reg != nil && t.reg.runBatch != nil {
		w.runBatch(attemptCtx, []*tracked{t})
	} else {
		w.runOne(attemptCtx, t)
	}
	stopAfter()
	stopAttempt(nil)
	stopRenewing()
	renewing.Wait()

	info, err := w.client.Get(context.WithoutCancel(ctx), id)
	if errors.Is(err, ErrNotFound) && t.completedDirectly {
		// The kind discards completed tasks: the task was deleted as it
		// completed.
		done := *t.info
		done.State, done.FinalizedAt = StateCompleted, time.Now()
		return &done, nil
	}
	return info, err
}

// leaseTask leases one task for RunTask, or explains why it cannot.
func (w *Worker) leaseTask(ctx context.Context, id int64) (*tracked, error) {
	r, err := w.client.pool.Query(ctx, w.client.sql.leaseTask, id, w.cfg.LeaseDuration.Microseconds(), w.id)
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
	return nil, w.client.stateError(ctx, pgxQuerier{q: w.client.pool}, id, "run")
}

// workerID names a worker after its host and process, with a random suffix
// that keeps restarted processes apart.
func workerID(host string) string {
	if host == "" {
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
