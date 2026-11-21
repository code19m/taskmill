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
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/code19m/taskmill/internal/buildinfo"
)

// Worker defaults.
const (
	defaultLeaseDuration = 30 * time.Second
	defaultPollInterval  = time.Second
	defaultDrainTimeout  = 20 * time.Second
	defaultNoHandlerWait = time.Hour
	defaultConcurrency   = 10
	poolSpare            = 5 // connections of the pool for the worker's own statements, beyond its Concurrency

	minLeaseDuration  = 100 * time.Millisecond
	minNoHandlerWait  = time.Minute
	maxNoHandlerWait  = 7 * 24 * time.Hour
	recordTimeout     = 30 * time.Second       // bound on one try to record a result
	recordRetryFirst  = 100 * time.Millisecond // first wait before trying to record a result again
	recordRetryMax    = 2 * time.Second        // longest wait between those tries
	cleanupTimeout    = 5 * time.Second        // bound on best-effort cleanup, such as closing a connection
	maxFetch          = 1000                   // most tasks one fetch leases
	completionBatch   = 500                    // most completions recorded in one statement
	completionLinger  = 2 * time.Millisecond   // wait for more completions to batch with
	blockedRetryDelay = 20 * time.Millisecond  // first wait before retrying completions whose rows other transactions held
	blockedRetryMax   = time.Second            // longest wait before retrying them
	hintCooldown      = 20 * time.Millisecond  // pause after a read of cancel requests or settings that notifications asked for
	errorMessageLimit = 4096                   // bytes of an error message that are kept
)

// timing holds a worker's fixed intervals. They are not configurable, but
// tests change them.
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
	reconnectMin    time.Duration // first wait before reconnecting the listener
	reconnectMax    time.Duration // longest wait before reconnecting the listener
	failureReport   time.Duration // how often an operation that keeps failing logs, at most
	idlePoll        time.Duration // how often idle queues poll while the maintainer's heartbeats arrive
	quiet           time.Duration // how long a queue leaves notifications waiting after the fetch for one found nothing
	listen          time.Duration // bound on opening the notification connection, and on its answer when heartbeats stop
	lockWait        time.Duration // how long a schedule's run, or a grouped task's outcome, waits for a row that another transaction holds, such as a FIFO group's
	scheduleCheck   time.Duration // how often the maintainer compares the workers' schedules with its own
	scheduleGrace   time.Duration // how long they may differ before it logs an error
	scheduleReport  time.Duration // how often it logs again while they differ, at most
}

var defaultTiming = timing{
	maintainer:      time.Second,
	heartbeat:       5 * time.Second,
	election:        2 * time.Second,
	leaderLease:     10 * time.Second, // at most doubled or halved by a release: see electMaintainerSQL
	retention:       time.Minute,
	stats:           10 * time.Second,
	activity:        10 * time.Second,
	settings:        10 * time.Second,
	shutdownGrace:   5 * time.Second,
	noHandlerSnooze: time.Minute,
	reconnectMin:    time.Second,
	reconnectMax:    30 * time.Second,
	failureReport:   time.Minute,
	idlePoll:        time.Minute,
	quiet:           20 * time.Millisecond,
	listen:          10 * time.Second,
	lockWait:        time.Second,
	scheduleCheck:   time.Minute,
	scheduleGrace:   10 * time.Minute, // longer than a rolling deploy that changes a schedule takes
	scheduleReport:  time.Hour,
}

// lockTimeout returns lockWait as a value of the setting lock_timeout.
func (t *timing) lockTimeout() string {
	return strconv.FormatInt(t.lockWait.Milliseconds(), 10) + "ms"
}

// errListenConfig reports a ListenConfig that pgx cannot connect with.
var errListenConfig = errors.New("taskmill: invalid ListenConfig; create it with pgx.ParseConfig")

// Worker fetches tasks from its queues and runs their handlers. Create one
// with [NewWorker], and run it with [Worker.Run]. One worker per process is
// typical; it also takes part in electing the maintainer, which promotes due
// tasks, rescues expired leases, runs schedules, and applies retention.
//
// A worker uses the pool of its [Client]. While Run runs, if the pool has at
// least the worker's total Concurrency plus 5 connections, the worker keeps
// one of them idle in reserve: a renewal of leases that finds every other
// connection in use, as when handlers that share the pool hold them, runs
// on it, so that leases do not run out. The worker also has one connection
// of its own for notifications.
// Run may be called again after it returned, but not while it runs.
//
// The [guide to workers and queues] shows how to choose the queues, limit
// how many tasks run, and stop a worker cleanly.
//
// [guide to workers and queues]: https://github.com/code19m/taskmill/blob/main/docs/guides/workers-and-queues.md
type Worker struct {
	client   *Client
	cfg      WorkerConfig
	id       string
	host     string
	handlers map[string]*registration
	// batchKinds reports that a handler is a batch handler: fetches may
	// lease more tasks to fill its batches.
	batchKinds bool
	queues     map[string]*queueState
	timing     timing
	logger     *slog.Logger
	listen     *pgx.ConnConfig
	version    string
	schedules  []compiledSchedule

	// The names of the schedules, sorted, and the hash of each one's
	// definition, in the same order.
	scheduleNames, scheduleHashes []string

	running   atomic.Bool
	reached   atomic.Pointer[time.Time] // when Run last reached the database; nil while it does not run
	settled   atomic.Bool               // Run has read the settings of its queues: until then, they fetch nothing
	released  atomic.Int64              // tasks given back during the current Run's shutdown
	notifying atomic.Bool               // the maintainer's heartbeats arrive, so notifications reach the worker
	resigned  chan struct{}             // the maintainer resigned: this worker's maintainer tries to take over now
	silent    atomic.Bool               // the worker logged that heartbeats do not arrive, and none has since
	tally     tally                     // ended attempts not yet added to the activity
	recording sync.WaitGroup            // Run's attempts whose results are being recorded

	mu       sync.Mutex
	attempts map[int64]*liveAttempt // attempts that Run and RunTask are running or recording

	// What notifications said since the worker last read the database for
	// them: the listener only signals here, and a loop of its own reads.
	cancelHints chan struct{} // cancellations of attempts' tasks were requested: see liveAttempt.hinted
	queueHints  chan struct{} // settings of the worker's queues changed

	completions   chan *liveAttempt // successful attempts of Run, for the completer
	completerDone chan struct{}     // closed when the current Run's completer returns

	counts     counters     // what Stats reports
	reserve    reserve      // the connection that Run keeps for renewals and check-ins that find the pool full
	poolWarned atomic.Int64 // when the worker last warned that it waited for a full pool, in Unix nanoseconds
	noHandler  kindWarnings // the kinds without a handler whose tasks the worker met

	// The failures of operations that the worker repeats, which it logs
	// only now and then. Leases and results are renewed and recorded by
	// RunTask too; Run's loops get theirs anew for each run, and each
	// queue's fetches and the maintainer's work have their own.
	leaseFailures, resultFailures                                       *incidentLog
	checkInFailures, settingsFailures, activityFailures, listenFailures *incidentLog
}

// WorkerConfig configures a [Worker]. The zero value is valid: a worker
// without handlers only takes part in maintenance. A config that sets Queues
// needs handlers too: without them, [NewWorker] returns an error that wraps
// [ErrInvalidArgument], as it does for a field outside its limits.
//
// The [guide to workers and queues] says which setting to use for which
// limit.
//
// [guide to workers and queues]: https://github.com/code19m/taskmill/blob/main/docs/guides/workers-and-queues.md
type WorkerConfig struct {
	// Handlers runs the tasks this worker fetches. Default: none, so the
	// worker only takes part in maintenance. Required when Queues is set.
	Handlers *Handlers

	// Queues lists the queues to fetch from, with per-process options. A
	// queue's name has 1 to 128 characters.
	// Default: the default queue of every registered kind, from
	// [ArgsWithDefaults] or else [DefaultQueue], with default options. If
	// Queues leaves out a registered kind's default queue, Run logs a warning
	// when it starts: tasks enqueued with the kind's defaults would wait for
	// another worker.
	Queues map[string]QueueOptions

	// LeaseDuration is how long a fetched task stays owned without a
	// renewal. The worker checks in every third of it, 10 seconds by
	// default, and renews the leases of its running tasks then. A lease
	// starts when the task is taken, by the database's clock, not when the
	// fetch began. Minimum: 100 milliseconds. Default: 30 seconds.
	//
	// The worker stores it when it registers, and the installation judges
	// by it how long the worker may make no check-in. After one
	// LeaseDuration, [Client.Workers] and the dashboard show the worker as
	// stale. After two, but not before 5 minutes, the maintainer removes it
	// from the list of workers. If it still runs, it registers again at its
	// next check-in.
	LeaseDuration time.Duration

	// PollInterval is how often the worker checks an idle queue while the
	// maintainer's heartbeats do not reach it. It must not be negative.
	// Default: 1 second.
	//
	// The maintainer sends a heartbeat every 5 seconds. While heartbeats
	// arrive, notifications wake the worker, and the maintainer wakes each
	// queue whose next task waited more than a second. So the worker checks
	// an idle queue only once a minute, ±10%, or every PollInterval if that
	// is longer, and a task whose notification came too early or was lost
	// starts within about 2 seconds.
	//
	// The worker uses PollInterval from its start until the first heartbeat,
	// which a running maintainer sends within 5 seconds, and again once no
	// heartbeat came for 15 seconds. So if the maintainer or the
	// notification connection fails silently, such a task starts within
	// about 16 seconds, and then within PollInterval while no heartbeat
	// arrives.
	//
	// A queue that its [QueueSettings.MaxRunning] limit keeps from fetching
	// more tasks is checked every PollInterval too, whether heartbeats
	// arrive or not: no notification says when another worker's task of the
	// queue ends. When one of its own tasks ends, the worker fetches right
	// away.
	PollInterval time.Duration

	// DrainTimeout is how long Run waits for running tasks after its context
	// is canceled, before canceling their contexts and, 5 seconds later,
	// releasing them. Run then has 5 seconds more at most to record the
	// results that are left, release those tasks, give up the maintainer's
	// role, and unregister. What it cannot record by then, as when the
	// database does not answer, is rescued once its lease runs out, and the
	// task runs again. So Run returns at most about DrainTimeout plus 10
	// seconds after its context is canceled. Keep
	// DrainTimeout at least 10 seconds below the time the platform gives a
	// stopping process. It must not be negative. Default: 20 seconds.
	DrainTimeout time.Duration

	// NoHandlerWait is how long a task of a kind that this worker has no
	// handler for waits for a worker that has one, before this worker makes
	// it dead with [CodeUnknownKind]. Meanwhile the worker snoozes the task 1
	// minute at a time, without using its attempts. The wait counts from
	// the first time a worker met the task without a handler: see
	// [CodeNoHandler]. In a rolling deploy that adds a kind, the workers of
	// the old release meet its tasks, so their wait is the one that counts:
	// give them, a release ahead, a wait longer than the whole rollout,
	// canary included. Minimum: 1 minute. Maximum: 7 days. Default: 1 hour.
	NoHandlerWait time.Duration

	// Middleware wraps every attempt, in order: the first is the outermost.
	// A nil entry is an error that wraps [ErrInvalidArgument]. Default: none.
	Middleware []Middleware

	// Schedules are recurring tasks that the maintainer enqueues. Each name
	// may appear once. Every worker that takes part in the maintainer
	// election must define the same schedules: see [Schedule]. Default: none.
	Schedules []Schedule

	// NoMaintainer keeps this worker out of the maintainer election. At least
	// one worker of an installation must take part. Set it on a worker that
	// does not define the schedules of the others.
	NoMaintainer bool

	// ListenConfig is the connection that receives notifications. Set it to
	// a direct connection when the pool goes through a pooler in transaction
	// mode, which cannot deliver them: without it, the worker finds that
	// out when it starts, logs an error, and polls every PollInterval.
	// Create it with [pgx.ParseConfig]. Default: the pool's configuration.
	// The setup for each pooler is in
	// https://github.com/code19m/taskmill/blob/main/docs/production/connection-poolers.md.
	ListenConfig *pgx.ConnConfig
}

// QueueOptions configure how one worker process reads one queue.
// Installation-wide settings, such as a limit on running tasks, are
// [QueueSettings].
type QueueOptions struct {
	// Concurrency is how many of the queue's tasks this process runs at the
	// same time. A batch counts as one. It must not be negative. Default: 10.
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
// reach the handler. Middleware does not run for a task that the worker ends
// without a handler: one of a kind that it has no handler for, or one whose
// row is invalid, which becomes dead with [CodeInvalidRow].
//
// Add middleware in [WorkerConfig.Middleware]. The [middleware guide] has
// examples.
//
// [middleware guide]: https://github.com/code19m/taskmill/blob/main/docs/guides/middleware.md
type Middleware func(ctx context.Context, task *TaskInfo, next func(context.Context) error) error

// NewWorker returns a worker that uses client to reach the database. cfg may
// be nil: the zero [WorkerConfig] is valid. It reports a nil client, and
// invalid configuration, schedules, and handler registrations, such as a
// kind registered twice, as errors that wrap [ErrInvalidArgument].
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
		version:     buildinfo.Version(),
		attempts:    make(map[int64]*liveAttempt),
		resigned:    make(chan struct{}, 1),
		cancelHints: make(chan struct{}, 1),
		queueHints:  make(chan struct{}, 1),
		completions: make(chan *liveAttempt, completionBatch),
	}
	w.reserve = reserve{w: w, turn: make(chan struct{}, 1)}

	if c.Handlers != nil {
		for kind, reg := range c.Handlers.byKind {
			r := *reg
			r.opts.Timeout = cmp.Or(r.opts.Timeout, defaultHandlerTimeout)
			r.opts.BatchSize = cmp.Or(r.opts.BatchSize, defaultBatchSize)
			if r.opts.Retry == nil {
				r.opts.Retry = ExponentialBackoff{}
			}
			w.handlers[kind] = &r
			w.batchKinds = w.batchKinds || r.runBatch != nil
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
	w.leaseFailures = newIncidentLog(w, "renew leases")
	w.resultFailures = newIncidentLog(w, "record results")

	var err error
	if w.schedules, err = compileSchedules(c.Schedules); err != nil {
		return nil, err
	}
	w.scheduleNames, w.scheduleHashes = scheduleSums(w.schedules)
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
	if slices.ContainsFunc(c.Middleware, func(m Middleware) bool { return m == nil }) {
		return invalidArgument("a middleware is nil")
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
	case c.NoHandlerWait != 0 && (c.NoHandlerWait < minNoHandlerWait || c.NoHandlerWait > maxNoHandlerWait):
		return invalidArgument("NoHandlerWait must be between %s and %s", minNoHandlerWait, maxNoHandlerWait)
	}
	c.LeaseDuration = cmp.Or(c.LeaseDuration, defaultLeaseDuration)
	c.PollInterval = cmp.Or(c.PollInterval, defaultPollInterval)
	c.DrainTimeout = cmp.Or(c.DrainTimeout, defaultDrainTimeout)
	c.NoHandlerWait = cmp.Or(c.NoHandlerWait, defaultNoHandlerWait)
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
		"the worker does not read the default queue of these registered kinds", slog.Any(logKeyKinds, kinds))
}

// sizedPool reports whether the pool has at least the worker's total
// Concurrency plus poolSpare connections, the size that the docs ask for,
// and warns if not: handlers that share the pool can then take every
// connection, and the worker's own statements wait for them. pgx's default
// size, the larger of 4 and the number of CPUs, is below the default
// Concurrency. It only warns: the pool is the application's to size.
func (w *Worker) sizedPool(ctx context.Context) bool {
	concurrency := 0
	for _, q := range w.queues {
		concurrency += cap(q.slots)
	}

	conns := int(w.client.pool.Stat().MaxConns())
	if conns >= concurrency+poolSpare {
		return true
	}
	w.logger.LogAttrs(ctx, slog.LevelWarn, "the pool has fewer connections than the worker's total Concurrency plus 5; "+
		"see https://github.com/code19m/taskmill/blob/main/docs/production/database.md#size-the-connection-pool",
		slog.Int(logKeyMaxConns, conns), slog.Int(logKeyConcurrency, concurrency))
	return false
}

// queueState is one queue that the worker serves.
type queueState struct {
	name       string
	slots      chan struct{} // one per running attempt or batch
	wake       chan struct{} // nudges the fetch loop: tasks may be available
	freed      chan struct{} // nudges the fetch loop: a slot was freed, or a recorded completion made room
	paused     atomic.Bool   // the queue is paused, or Run has not read its settings yet
	maxRunning atomic.Int32  // installation-wide limit; 0 means none
	stuck      atomic.Int32  // slots whose handlers did not return shutdownGrace after their context ended
}

func (q *queueState) nudge() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// liveAttempt is an attempt the worker is running or recording.
type liveAttempt struct {
	att     *attempt
	info    *TaskInfo
	reg     *registration // nil if no handler knows the kind, or the task is invalid
	invalid error         // why the task's row cannot run: it holds what taskmill never writes
	direct  bool          // run by RunTask, which records its completion right away

	// completedDirectly is set once RunTask recorded the completion.
	completedDirectly bool

	// Only the completer uses these: since when the completion has found the
	// task's row, or its group's, held by other transactions, and whether
	// it warned of that.
	blockedSince  time.Time
	blockedWarned bool

	mu        sync.Mutex
	cancel    context.CancelCauseFunc // set once a single attempt starts
	state     attemptState
	renewedAt time.Time // last confirmation that the lease is held

	lost   atomic.Bool // the lease is gone: the result must be discarded
	hinted atomic.Bool // a notification said that the task's cancellation was requested, and the row is not read yet
}

type attemptState int

const (
	attemptRunning   attemptState = iota // the handler is running
	attemptRecording                     // the handler returned; its result is being recorded
	attemptAbandoned                     // released at shutdown, or its task was leased again; the result is ignored
)

// runState is the state of one call to [Worker.Run].
type runState struct {
	attemptCtx context.Context // parent of every attempt; canceled with ErrShutdown once DrainTimeout passes
	recordCtx  context.Context // where results are recorded; canceled cleanupTimeout after the drain
	fetching   sync.WaitGroup  // the fetch loops
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

	// A worker that never leads stores no schedules, so that none compares
	// them: NULL.
	schedules, hashes := w.scheduleNames, w.scheduleHashes
	if w.cfg.NoMaintainer {
		schedules, hashes = nil, nil
	}

	_, err := w.client.pool.Exec(ctx, w.client.sql.upsertWorker,
		w.id,                               // $1
		names,                              // $2
		kinds,                              // $3
		w.host,                             // $4
		w.version,                          // $5
		w.cfg.LeaseDuration.Microseconds(), // $6
		schedules,                          // $7
		hashes,                             // $8
	)
	return names, err
}

// Run fetches and runs tasks until ctx is canceled. It then stops fetching
// and, if it is the maintainer, resigns, so that another worker takes over
// right away. It waits up to DrainTimeout for running tasks, cancels and
// releases those still running, and returns nil, at most about DrainTimeout
// plus 10 seconds after ctx ended, even when the database does not answer:
// see [WorkerConfig.DrainTimeout]. If ctx ends before the
// worker registered, it returns nil right away. It returns an error only if
// the worker cannot start: it is already running, which is
// [ErrInvalidState], or it cannot register in the database.
//
// Before it fetches, Run creates the history partitions that the next week
// lacks, as [Migrate] does, so that a schema applied with [Migrations] keeps
// the history of its first completed tasks. A failure is only logged: the
// maintainer tries again.
//
// Run fetches from no queue before it has read the queues' settings, so a
// paused queue stays paused and a [QueueSettings.MaxRunning] limit holds
// from the first fetch. If that read fails, Run logs it and reads again:
// 100 milliseconds later, then waiting twice as long each time, up to 10
// seconds. Each read is given up after 10 seconds.
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

	// No queue fetches before the worker knows its settings, which the
	// settings loop reads first: a paused queue stays paused, and a limit
	// holds, also when that read fails or takes long. Health says so until
	// then. A worker without queues has nothing to read.
	w.settled.Store(len(w.queues) == 0)
	for _, q := range w.queues {
		q.paused.Store(true)
	}

	now := time.Now()
	w.reached.Store(&now)
	w.checkInFailures = newIncidentLog(w, "record check-in")
	w.settingsFailures = newIncidentLog(w, "load queue settings")
	w.activityFailures = newIncidentLog(w, "record activity")
	w.listenFailures = newIncidentLog(w, "listen for notifications")

	w.prepareHistory(ctx)
	sized := w.sizedPool(ctx)
	w.logger.LogAttrs(ctx, slog.LevelInfo, "worker started", slog.Any(logKeyQueues, names))
	w.warnUnservedKinds(ctx)

	// Results are recorded on recordCtx, which outlives ctx, and the drain,
	// by cleanupTimeout.
	recordCtx, stopRecording := context.WithCancel(base)
	defer stopRecording()

	// A pool of that size spares the worker a reserve, which it takes before
	// any handler can take the pool's, and gives back once nothing more uses
	// it. A smaller pool cannot spare one: workers that share it could hold
	// all of it, and then none could fetch.
	if sized {
		w.reserve.keep(ctx)
		defer w.reserve.release()
	}

	w.released.Store(0)
	w.completerDone = make(chan struct{})
	completerCtx, stopCompleter := context.WithCancel(base)
	go w.completer(completerCtx, recordCtx)

	loopsCtx, stopLoops := context.WithCancel(base)
	var loops sync.WaitGroup
	loops.Go(func() { w.listenLoop(loopsCtx) })
	loops.Go(func() { followHints(loopsCtx, w.cancelHints, w.cancelHinted) })
	loops.Go(func() { followHints(loopsCtx, w.queueHints, w.refreshQueueSettings) })
	loops.Go(func() { w.checkInLoop(loopsCtx) })
	loops.Go(func() { w.activityLoop(loopsCtx) })
	loops.Go(func() { w.settingsLoop(loopsCtx) })
	if !w.cfg.NoMaintainer {
		// The maintainer stops with ctx, as the fetches do: a worker that
		// drains resigns, so that one that stays leads meanwhile.
		loops.Go(func() { newMaintainer(w).run(ctx, recordCtx) })
	}

	attemptCtx, stopAttempts := context.WithCancelCause(base)
	r := &runState{attemptCtx: attemptCtx, recordCtx: recordCtx}
	for _, q := range w.queues {
		r.fetching.Go(func() { w.fetchLoop(ctx, r, q) })
	}

	<-ctx.Done()
	stopped := time.Now()
	abandoned := w.drain(base, r, stopAttempts)

	// What follows has one deadline, so that Run returns soon also when the
	// database does not answer: the release of the tasks whose handlers did
	// not return, the results that attempts that ended before are still
	// recording, also through the completer, the maintainer's resignation,
	// the activity, and the worker's row. What is not recorded by then is
	// rescued when its lease runs out. The deadline comes cleanupTimeout
	// after the drain, but no later than the bound that DrainTimeout's doc
	// gives: a fetch under way can hold the drain up until cleanupTimeout
	// after ctx ended.
	latest := time.Until(stopped.Add(w.cfg.DrainTimeout + w.timing.shutdownGrace + cleanupTimeout))
	stop := time.AfterFunc(min(cleanupTimeout, latest), stopRecording)
	defer stop.Stop()
	w.release(recordCtx, abandoned)
	w.recording.Wait()

	stopCompleter()
	<-w.completerDone
	stopLoops()
	loops.Wait()

	w.flushActivity(recordCtx)
	if _, err := w.client.pool.Exec(recordCtx, w.client.sql.deleteWorker, w.id); err != nil {
		w.logger.LogAttrs(base, slog.LevelWarn, "unregister worker failed", slog.Any(logKeyError, err))
	}
	w.logger.LogAttrs(base, slog.LevelInfo, "worker stopped", slog.Int64(logKeyReleased, w.released.Load()))
	return nil
}

// drain ends a run whose context has ended. It waits for the fetch loops and
// for the running attempts. Once DrainTimeout has passed, it cancels the
// attempts, and it returns those that do not return in time, abandoned: their
// tasks are to be released.
func (w *Worker) drain(ctx context.Context, r *runState, stopAttempts context.CancelCauseFunc) []*liveAttempt {
	done := make(chan struct{})
	go func() {
		r.fetching.Wait() // the fetch loops start attempts, so they end first
		r.inflight.Wait()
		close(done)
	}()

	if n := w.runningTasks(); n > 0 {
		w.logger.LogAttrs(ctx, slog.LevelInfo, "worker stopping; waiting for running tasks",
			slog.Int(logKeyTasks, n), slog.Duration(logKeyTimeout, w.cfg.DrainTimeout))
	}

	// The timeout counts from here, when the worker was told to stop, and not
	// from the end of the fetch loops. A fetch that is in flight can take
	// long: it may wait for a connection that only a handler's return frees,
	// and so for the very attempts that this timeout cancels.
	select {
	case <-done:
		return nil
	case <-time.After(w.cfg.DrainTimeout):
	}

	stopAttempts(ErrShutdown)
	select {
	case <-done:
		return nil
	case <-time.After(w.timing.shutdownGrace):
	}

	// Handlers that ignore their context keep running, but their results are
	// ignored and their tasks go back to the queue. No attempt starts once
	// the fetch loops have returned, so none escapes this.
	r.fetching.Wait()
	w.mu.Lock()
	var abandoned []*liveAttempt
	for _, t := range w.attempts {
		t.mu.Lock()
		if t.state == attemptRunning && !t.direct {
			t.state = attemptAbandoned
			abandoned = append(abandoned, t)
		}
		t.mu.Unlock()
	}
	w.mu.Unlock()

	w.counts.abandoned.Add(int64(len(abandoned)))
	for _, t := range abandoned {
		w.logger.LogAttrs(ctx, slog.LevelWarn, "handler ignored shutdown; releasing its task",
			slog.Int64(logKeyTaskID, t.att.id), slog.String(logKeyKind, t.info.Kind))
	}
	return abandoned
}

// release gives tasks back to their queues while the worker stops. The
// deadline of ctx bounds all of it: a database that does not answer then
// delays the shutdown once, and not once for each task. A task that is not
// released by then is rescued when its lease runs out.
func (w *Worker) release(ctx context.Context, ts []*liveAttempt) {
	for _, t := range ts {
		w.record(ctx, t, outcome{kind: outcomeRelease})
	}
}

// runningTasks returns how many tasks the handlers of Run are running.
func (w *Worker) runningTasks() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, t := range w.attempts {
		t.mu.Lock()
		if t.state == attemptRunning && !t.direct {
			n++
		}
		t.mu.Unlock()
	}
	return n
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
