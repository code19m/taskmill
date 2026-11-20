package taskmill_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

// idleQueues are the queues of the idle-polling tests.
var idleQueues = map[string]taskmill.QueueOptions{"default": {}, "a": {}, "b": {}, "c": {}}

// newCountedEnv returns an env whose pool counts the fetches its connections
// send.
func newCountedEnv(t *testing.T) (*env, *fetchCounter) {
	t.Helper()
	fetches := &fetchCounter{}
	return newTracedEnv(t, fetches), fetches
}

// startIdleWorker runs a worker from newIdleWorker.
func startIdleWorker(t *testing.T, e *env, cfg *taskmill.WorkerConfig) (*taskmill.Worker, <-chan struct{}) {
	t.Helper()
	w, started := newIdleWorker(t, e, cfg)
	startWorker(t, w)
	return w, started
}

// newIdleWorker returns a worker of idleQueues that polls every 20 ms while
// the maintainer's heartbeats do not reach it, and every hour, as tests do,
// while they do. cfg, which may be nil, sets the rest of its configuration.
// The channel gets a value for each task that starts.
func newIdleWorker(t *testing.T, e *env, cfg *taskmill.WorkerConfig) (*taskmill.Worker, <-chan struct{}) {
	t.Helper()
	if cfg == nil {
		cfg = &taskmill.WorkerConfig{}
	}
	started := make(chan struct{}, 10)
	cfg.Handlers = handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
		started <- struct{}{}
		return nil
	}, nil)
	cfg.Queues, cfg.PollInterval = idleQueues, 20*time.Millisecond
	return newWorker(t, e, cfg), started
}

func TestIdleWorkerFetchesRarelyWhileHeartbeatsArrive(t *testing.T) {
	t.Parallel()
	e, fetches := newCountedEnv(t)
	w, started := startIdleWorker(t, e, nil)
	waitFor(t, "heartbeats to arrive", func() bool { return taskmill.Notifying(w) })

	// The worker then leaves its idle queues alone. The test counts its
	// fetches against the maintainer's heartbeats, not against the clock:
	// polling every 20 ms would fetch some 40 times per heartbeat. The
	// fetches of the start, of the listener's start, and of the polls that
	// were already waiting end by the next heartbeat. A stretch in which a
	// busy machine held the heartbeats up does not count either.
	heartbeats := func(n int64) {
		sent := fetches.heartbeats.Load()
		waitFor(t, "more heartbeats", func() bool { return fetches.heartbeats.Load() >= sent+n })
	}
	waitFor(t, "three heartbeats in a row without a fetch", func() bool {
		heartbeats(1)
		fetched := fetches.n.Load()
		heartbeats(3)
		return fetches.n.Load() == fetched
	})

	// A notification still wakes it: it would not fetch again for an hour.
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "b"})
	recv(t, started, "the task to start")
}

// heartbeatsMissing is what a worker logs when the maintainer's heartbeats do
// not arrive.
const heartbeatsMissing = "maintainer heartbeats do not arrive; idle queues poll every PollInterval"

func TestIdleWorkerPollsWhenNotificationsDoNotArrive(t *testing.T) {
	t.Parallel()
	e, fetches := newCountedEnv(t)
	// A listener on another database, as one through a transaction-pooling
	// proxy, connects and listens, but receives no notification.
	listen, err := pgx.ParseConfig(testdb.DirectURL(t))
	if err != nil {
		t.Fatal(err)
	}
	listen.Database = "postgres"
	logs := &logRecorder{}
	e = e.withLogger(t, slog.New(logs))
	w, started := startIdleWorker(t, e, &taskmill.WorkerConfig{ListenConfig: listen})
	waitFor(t, "the worker to find that heartbeats do not arrive", func() bool {
		return logs.count(heartbeatsMissing) == 1
	})
	if taskmill.Notifying(w) {
		t.Fatal("the worker counts on notifications that do not arrive")
	}

	// It keeps fetching, though no task waits; counting on notifications, it
	// would not fetch again for an hour. The test counts fetches rather than
	// timing them: on a busy machine, each fetch can take far longer than
	// the 20 ms poll interval.
	before := fetches.n.Load()
	waitFor(t, "the worker to keep polling", func() bool { return fetches.n.Load()-before >= 20 })
	// A task whose notification does not arrive starts by the next poll.
	e.enqueue(t, sendEmail{}, nil)
	recv(t, started, "the task to start")
	if n := logs.count(heartbeatsMissing); n != 1 {
		t.Fatalf("the worker said %d times that heartbeats do not arrive, want once", n)
	}
}

func TestIdleWorkerPollsWhileTheMaintainerFailsOver(t *testing.T) {
	t.Parallel()
	e, _ := newCountedEnv(t)
	// A maintainer that crashed holds the role until its lease runs out, so
	// no heartbeat comes, and nothing wakes a queue whose notification was
	// lost. The lease is a minute here, and the worker's own too: it takes a
	// lease that ends later than two of its own for none.
	e.exec(t, `INSERT INTO {s}.leader (name, worker_id, elected_at, expires_at)
	           VALUES ('maintainer', 'crashed', now(), now() + interval '1 minute')`)
	w, started := newIdleWorker(t, e, nil)
	taskmill.SetTiming(w, taskmill.Timing{LeaderLease: time.Minute})
	startWorker(t, w)
	time.Sleep(500 * time.Millisecond)
	if taskmill.Notifying(w) {
		t.Fatal("the worker counts on notifications without heartbeats")
	}

	// The task starts by the next poll. Nothing else finds it before the
	// crashed maintainer's lease runs out, a minute from now.
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
	           VALUES ('a', 'email.send', 'available', now(), '{}', 1)`)
	recv(t, started, "the task to start")
}

func TestIdleWorkerPollsWhileItsListenerIsCut(t *testing.T) {
	t.Parallel()
	e, _ := newCountedEnv(t)
	direct := newProxy(t, testdb.DirectURL(t))
	listen, err := pgx.ParseConfig(testdb.DirectURL(t))
	if err != nil {
		t.Fatal(err)
	}
	listen.Host, listen.Port, listen.Fallbacks = direct.host, direct.port, nil
	w, started := startIdleWorker(t, e, &taskmill.WorkerConfig{ListenConfig: listen})
	waitFor(t, "heartbeats to arrive", func() bool { return taskmill.Notifying(w) })

	// While the listener is cut, notifications are lost, and the worker
	// finds tasks by polling.
	direct.cut(true)
	waitFor(t, "the worker to stop counting on notifications", func() bool { return !taskmill.Notifying(w) })
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "c"})
	recv(t, started, "the task to start")

	direct.cut(false)
	waitFor(t, "heartbeats to arrive again", func() bool { return taskmill.Notifying(w) })
}

// A worker hears of a pause, a resume, or a limit through a notification.
// One that comes while the worker does not listen is lost: at its start, and
// while it reconnects. So the worker reads its queues' settings again once
// it listens. Here it is a resume, because a task that runs shows that the
// worker has read it.
func TestWorkerReadsItsQueuesSettingsAgainOnceItListens(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	direct := newProxy(t, testdb.DirectURL(t))
	listen, err := pgx.ParseConfig(testdb.DirectURL(t))
	if err != nil {
		t.Fatal(err)
	}
	listen.Host, listen.Port, listen.Fallbacks = direct.host, direct.port, nil
	if err := e.client.PauseQueue(t.Context(), taskmill.DefaultQueue); err != nil {
		t.Fatal(err)
	}
	ran := make(chan struct{}, 1)
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			ran <- struct{}{}
			return nil
		}, nil),
		Queues:       defaultQueue(1),
		NoMaintainer: true,
		ListenConfig: listen,
	})
	// No later read of the settings stands in for the one that the test
	// waits for.
	taskmill.SetTiming(w, taskmill.Timing{Settings: time.Hour})

	// The worker starts, and reads that the queue is paused, while it cannot
	// listen: it is healthy once it has read the settings. A resume that
	// commits now notifies nobody.
	direct.cut(true)
	startWorker(t, w)
	waitFor(t, "the worker to read its queues' settings", func() bool { return w.Health() == nil })
	if err := e.client.ResumeQueue(t.Context(), taskmill.DefaultQueue); err != nil {
		t.Fatal(err)
	}
	e.enqueue(t, sendEmail{}, nil)

	direct.cut(false)
	recv(t, ran, "the task of the resumed queue to run once the worker listens")
}

func TestIdleWorkerReplacesAListenerThatFallsSilent(t *testing.T) {
	t.Parallel()
	e, _ := newCountedEnv(t)
	direct := newProxy(t, testdb.DirectURL(t))
	listen, err := pgx.ParseConfig(testdb.DirectURL(t))
	if err != nil {
		t.Fatal(err)
	}
	listen.Host, listen.Port, listen.Fallbacks = direct.host, direct.port, nil
	logs := &logRecorder{}
	e = e.withLogger(t, slog.New(logs))
	w, started := startIdleWorker(t, e, &taskmill.WorkerConfig{ListenConfig: listen})
	waitFor(t, "heartbeats to arrive", func() bool { return taskmill.Notifying(w) })

	silent := e.listenerPID(t)

	// The listener's connection stays open but carries nothing, as after a
	// failover or a partition that drops packets: no error says so.
	direct.frozen.Store(true)
	waitFor(t, "the worker to stop counting on notifications", func() bool { return !taskmill.Notifying(w) })
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "c"})
	recv(t, started, "the task to start")
	waitFor(t, "the worker to give up on the listener", func() bool {
		return logs.count("operation failed") == 1
	})

	// Once the network carries packets again, the worker listens on a new
	// connection.
	direct.frozen.Store(false)
	waitFor(t, "heartbeats to arrive on a new listener", func() bool {
		return taskmill.Notifying(w) && slices.ContainsFunc(e.listenerPIDs(t), func(pid int) bool { return pid != silent })
	})
}

func TestIdleWorkerKeepsAListenerThatAnswers(t *testing.T) {
	t.Parallel()
	e, _ := newCountedEnv(t)
	stopMaintainer := runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: noopHandlers(), Queues: map[string]taskmill.QueueOptions{"m": {}},
	})
	logs := &logRecorder{}
	e = e.withLogger(t, slog.New(logs))
	w, started := startIdleWorker(t, e, &taskmill.WorkerConfig{NoMaintainer: true})
	waitFor(t, "heartbeats to arrive", func() bool { return taskmill.Notifying(w) })

	// The only maintainer stops, so heartbeats stop, but the listener
	// still works: the worker polls, and keeps it. On a busy machine, the
	// worker may have warned already, while the maintainer started.
	missing := logs.count(heartbeatsMissing)
	stopMaintainer()
	pid := e.listenerPID(t)
	// Without heartbeats, the worker asks the listener each 600 ms whether
	// it still answers, with a statement of its own. A listener that it
	// replaced after an answer would not get a second such statement.
	statements := map[time.Time]bool{} // of the listener, by when they started
	seen := func() int {
		if got := e.listenerPID(t); got != pid {
			t.Fatalf("the worker replaced a listener that answers: pid %d, then %d", pid, got)
		}
		var at time.Time
		if err := e.pool.QueryRow(t.Context(), "SELECT query_start FROM pg_stat_activity WHERE pid = $1", pid).Scan(&at); err != nil {
			t.Fatal(err)
		}
		statements[at] = true
		return len(statements)
	}
	seen() // its last statement from while heartbeats arrived
	waitFor(t, "the worker to find that heartbeats do not arrive", func() bool {
		return logs.count(heartbeatsMissing) == missing+1 && !taskmill.Notifying(w)
	})
	waitFor(t, "the listener to be asked twice", func() bool { return seen() > 2 })
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "a"})
	recv(t, started, "the task to start")
}

// TestMaintainerHandsOverWithoutAGap checks that when the maintainer stops,
// another takes over at once, before workers miss its heartbeats, though
// workers that do not lead try to take over only once an hour here.
func TestMaintainerHandsOverWithoutAGap(t *testing.T) {
	t.Parallel()
	e, fetches := newCountedEnv(t)
	logs := &logRecorder{}
	eligible := func() (stop func()) {
		w := newWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers(), Queues: map[string]taskmill.QueueOptions{"m": {}}})
		taskmill.SetTiming(w, taskmill.Timing{Election: time.Hour})
		return startWorker(t, w)
	}
	stopLeader := eligible()
	waitFor(t, "a maintainer", func() bool { return e.count(t, "SELECT count(*) FROM {s}.leader") == 1 })
	eligible()
	// The other workers wait three heartbeat intervals, three seconds here,
	// for the next heartbeat: far longer than a handover takes, even on a
	// busy machine, and far shorter than the hour that the eligible worker
	// waits to take over unless the maintainer resigns.
	logged := e.withLogger(t, slog.New(logs))
	var idle []*taskmill.Worker
	for range 3 {
		w := newWorker(t, logged, &taskmill.WorkerConfig{Handlers: noopHandlers(), Queues: idleQueues, NoMaintainer: true})
		taskmill.SetTiming(w, taskmill.Timing{Heartbeat: time.Second})
		startWorker(t, w)
		idle = append(idle, w)
	}
	notifying := func() bool {
		for _, w := range idle {
			if !taskmill.Notifying(w) {
				return false
			}
		}
		return true
	}
	waitFor(t, "heartbeats to arrive", notifying)
	// Every worker listens before the handover. On a busy machine, the
	// eligible worker's listener can still be starting when the idle workers
	// hear heartbeats, and would then look like a replacement.
	var listeners []int
	waitFor(t, "every worker to listen", func() bool {
		listeners = e.listenerPIDs(t)
		return len(listeners) == 5
	})

	// The handover is over once the next maintainer has sent heartbeats of
	// its own. A worker that waited its three seconds for one meanwhile has
	// said so, and has stopped counting on notifications.
	leader := e.count(t, "SELECT count(*) FROM {s}.leader")
	stopLeader()
	sent := fetches.heartbeats.Load()
	waitFor(t, "the next maintainer to send two heartbeats", func() bool { return fetches.heartbeats.Load() >= sent+2 })
	if !notifying() || logs.count(heartbeatsMissing) > 0 {
		t.Fatal("workers stopped counting on notifications while the maintainer changed")
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.leader"); n != leader {
		t.Fatal("no worker took over")
	}
	for _, pid := range e.listenerPIDs(t) {
		if !slices.Contains(listeners, pid) {
			t.Fatal("a worker replaced its listener while the maintainer changed")
		}
	}
}

// TestWorkerDoesNotListenThroughTransactionPooling runs only through a
// transaction-pooling proxy, as `make test-pgbouncer` does.
func TestWorkerDoesNotListenThroughTransactionPooling(t *testing.T) {
	t.Parallel()
	if os.Getenv(testdb.EnvListenURL) == "" {
		t.Skip("the test database is not behind a transaction-pooling proxy")
	}
	e := newEnv(t)
	logs := &logRecorder{}
	e = e.withLogger(t, slog.New(logs))
	_, started := startIdleWorker(t, e, &taskmill.WorkerConfig{ListenConfig: e.pool.Config().ConnConfig.Copy()})
	waitFor(t, "the worker to give up listening", func() bool {
		return logs.count("notifications disabled; polling only") == 1
	})
	if n := e.count(t, "SELECT count(*) FROM pg_stat_activity WHERE query = 'LISTEN ' || $1", e.schema); n != 0 {
		t.Fatalf("%d server connections listen", n)
	}
	e.enqueue(t, sendEmail{}, nil)
	recv(t, started, "the task to start")
}

// listenerPIDs returns the server processes that listen for the env's
// notifications.
func (e *env) listenerPIDs(t *testing.T) []int {
	t.Helper()
	r, err := e.pool.Query(t.Context(), "SELECT pid FROM pg_stat_activity WHERE query = 'LISTEN ' || $1", e.schema)
	if err != nil {
		t.Fatal(err)
	}
	pids, err := pgx.CollectRows(r, pgx.RowTo[int])
	if err != nil {
		t.Fatal(err)
	}
	return pids
}

func TestIdleWorkerFindsATaskWhoseNotificationWasLost(t *testing.T) {
	t.Parallel()
	e, _ := newCountedEnv(t)
	w, started := startIdleWorker(t, e, nil)
	waitFor(t, "heartbeats to arrive", func() bool { return taskmill.Notifying(w) })

	// The task becomes available without a notification, as when the
	// producer that owed it crashed. The maintainer wakes its queue: the
	// worker would not fetch from it again for an hour.
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
	           VALUES ('a', 'email.send', 'available', now(), '{}', 1)`)
	recv(t, started, "the task to start")
}

func TestIdleWorkerFindsATaskCommittedAfterItsNotification(t *testing.T) {
	t.Parallel()
	e, _ := newCountedEnv(t)
	w, started := startIdleWorker(t, e, nil)
	waitFor(t, "heartbeats to arrive", func() bool { return taskmill.Notifying(w) })

	// The first enqueue notifies; the second, within the throttle's 50 ms,
	// leaves its notification to the end of that window, which comes before
	// its transaction commits. The maintainer then wakes the queue.
	e.enqueue(t, sendEmail{}, nil)
	tx := e.begin(t)
	if _, err := e.client.EnqueueTx(t.Context(), taskmill.Pgx(tx), sendEmail{}, nil); err != nil {
		t.Fatal(err)
	}
	recv(t, started, "the first task to start")
	time.Sleep(200 * time.Millisecond)
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	recv(t, started, "the second task to start")
}

// TestEveryWayATaskBecomesAvailableNotifies checks that each way a task
// becomes available notifies its queue. Without a maintainer, no heartbeat
// or wake comes, and the workers poll once an hour, so only a notification
// starts a task in time. Each way uses a queue of its own, and acts well
// after the queue's last notification, so that the throttle lets it notify
// in its own transaction.
func TestEveryWayATaskBecomesAvailableNotifies(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const gap = 200 * time.Millisecond // longer than the throttle's window
	ran := make(chan int64, 10)
	var mu sync.Mutex
	runs := map[int64]int{}
	queues := map[string]taskmill.QueueOptions{}
	for _, q := range []string{"enqueue", "retry", "bulk", "group", "snooze"} {
		queues[q] = taskmill.QueueOptions{}
	}
	idle := func(hs *taskmill.Handlers, queues map[string]taskmill.QueueOptions) *taskmill.WorkerConfig {
		return &taskmill.WorkerConfig{Handlers: hs, Queues: queues, NoMaintainer: true, PollInterval: time.Hour}
	}
	runWorker(t, e, idle(handlers(func(_ context.Context, task *taskmill.Task[sendEmail]) error {
		mu.Lock()
		runs[task.ID]++
		first := runs[task.ID] == 1
		mu.Unlock()
		ran <- task.ID
		switch {
		case !first:
			return nil
		case task.Queue == "retry" || task.Queue == "bulk":
			return errors.New("fails once") // dead after its one attempt
		case task.Queue == "snooze":
			time.Sleep(gap)
			return taskmill.Snooze(0)
		case task.Queue == "group":
			time.Sleep(gap) // then the next task of its group becomes its head
		}
		return nil
	}, nil), queues))
	e.waitListening(t)

	// runsSoon fails the test unless task id is the next to run, long before
	// the workers' hourly poll.
	runsSoon := func(how string, id int64) {
		t.Helper()
		if got := recv(t, ran, "a task to run ("+how+")"); got != id {
			t.Fatalf("%s: task %d ran, want %d", how, got, id)
		}
	}
	once := &taskmill.EnqueueOptions{MaxAttempts: 1}

	id := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "enqueue"})
	runsSoon("enqueue", id)

	once.Queue = "retry"
	id = e.enqueue(t, sendEmail{}, once)
	runsSoon("first run", id)
	e.waitState(t, id, taskmill.StateDead)
	time.Sleep(gap)
	if err := e.client.Retry(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	runsSoon("retry", id)

	once.Queue = "bulk"
	id = e.enqueue(t, sendEmail{}, once)
	runsSoon("first run", id)
	e.waitState(t, id, taskmill.StateDead)
	time.Sleep(gap)
	if _, err := e.client.RetryMatching(t.Context(), &taskmill.BulkParams{Filter: taskmill.TaskFilter{Queues: []string{"bulk"}}}); err != nil {
		t.Fatal(err)
	}
	runsSoon("bulk retry", id)

	head := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "group", GroupKey: "g"})
	next := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "group", GroupKey: "g"})
	runsSoon("enqueue", head)
	runsSoon("group head promotion", next)

	id = e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "snooze"})
	runsSoon("first run", id)
	runsSoon("snooze", id)

	// A worker that stops gives its running task back.
	started := make(chan struct{}, 1)
	stop := runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, _ *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}, nil),
		Queues:       map[string]taskmill.QueueOptions{"release": {}},
		NoMaintainer: true,
		DrainTimeout: 50 * time.Millisecond,
	})
	id = e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "release"})
	recv(t, started, "the task to start")
	runWorker(t, e, idle(handlers(func(_ context.Context, task *taskmill.Task[sendEmail]) error {
		ran <- task.ID
		return nil
	}, nil), map[string]taskmill.QueueOptions{"release": {}}))
	waitFor(t, "three workers to listen", func() bool {
		return e.count(t, "SELECT count(*) FROM pg_stat_activity WHERE query = 'LISTEN ' || $1", e.schema) == 3
	})
	stop()
	runsSoon("release", id)
}

// fetchCounter counts the fetches that a pool's connections send, and the
// heartbeats that a maintainer sends on them.
type fetchCounter struct {
	n          atomic.Int64
	heartbeats atomic.Int64
}

func (c *fetchCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	// The statement that wakes the queues sends the heartbeat too, if its
	// last argument says so.
	if strings.Contains(d.SQL, "pg_notify($1, 'h|')") && slices.Contains(d.Args, any(true)) {
		c.heartbeats.Add(1)
	}
	return ctx
}

func (*fetchCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (*fetchCounter) TraceBatchStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	return ctx
}

func (c *fetchCounter) TraceBatchQuery(_ context.Context, _ *pgx.Conn, d pgx.TraceBatchQueryData) {
	if strings.Contains(d.SQL, "WITH picked AS") {
		c.n.Add(1)
	}
}

func (*fetchCounter) TraceBatchEnd(context.Context, *pgx.Conn, pgx.TraceBatchEndData) {}

func TestWorkerCancelsRunningTask(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	started := make(chan struct{})
	cause := make(chan error, 1)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, _ *taskmill.Task[sendEmail]) error {
			close(started)
			<-ctx.Done()
			cause <- context.Cause(ctx)
			return context.Cause(ctx)
		}, &taskmill.HandlerOptions{Timeout: time.Minute}),
		Queues: defaultQueue(1),
	})

	id := e.enqueue(t, sendEmail{}, nil)
	recv(t, started, "a handler to start")
	e.waitListening(t) // a missed notification would wait for the next heartbeat
	if err := e.client.Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if err := recv(t, cause, "the handler's context to end"); !errors.Is(err, taskmill.ErrTaskCanceled) {
		t.Fatalf("got cause %v, want ErrTaskCanceled", err)
	}
	e.waitState(t, id, taskmill.StateCanceled)
}

// Any role in the database can send a notification, with no right on
// taskmill's tables, so a notification alone must not cancel a task.
func TestCancelNotificationAloneDoesNotCancel(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	started := make(chan struct{}, 2)
	ended := make(chan int64, 2)
	proceed := make(chan struct{})
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			select {
			case <-ctx.Done():
				ended <- task.ID
				return context.Cause(ctx)
			case <-proceed:
				return nil
			}
		}, &taskmill.HandlerOptions{Timeout: time.Minute}),
		Queues: defaultQueue(2),
	})
	unasked := e.enqueue(t, sendEmail{}, nil)
	asked := e.enqueue(t, sendEmail{}, nil)
	recv(t, started, "the first handler to start")
	recv(t, started, "the second handler to start")
	e.waitListening(t)

	// Nobody asked to cancel the first task. The worker reads the row of a
	// task that a notification named no later than the rows of the tasks
	// named after it, so once the second task's context ends, it has read
	// the first task's row too.
	e.exec(t, "SELECT pg_notify($1, $2)", e.schema, "c|"+strconv.FormatInt(unasked, 10))
	if err := e.client.Cancel(t.Context(), asked); err != nil {
		t.Fatal(err)
	}
	if id := recv(t, ended, "the canceled task's context to end"); id != asked {
		t.Fatalf("a notification alone ended the context of task %d", id)
	}
	close(proceed)
	e.waitState(t, unasked, taskmill.StateCompleted)
	e.waitState(t, asked, taskmill.StateCanceled)
}

// Any role can send notifications faster than a worker reads the database
// for each: one statement commits thousands. They must neither hold up a
// real cancellation nor cost a read each.
func TestFloodOfNotificationsDoesNotHoldUpACancel(t *testing.T) {
	t.Parallel()
	const flood = 20000
	for _, tc := range []struct {
		name    string
		payload func(running int64) string // of every notification of the flood
		reads   string                     // a part of the statement that a worker sends to follow it, and of no other
		every   time.Duration              // the workers send that statement at most once in this time
	}{
		{"cancel", func(running int64) string { return "c|" + strconv.FormatInt(running, 10) }, "AND t.cancel_requested_at IS NOT NULL", 20 * time.Millisecond},
		{"settings", func(int64) string { return "q|" + taskmill.DefaultQueue }, "paused_at IS NOT NULL, coalesce(max_running", 20 * time.Millisecond},
		// Per election interval, the worker that leads renews its lease once,
		// whatever arrives, and the other one tries to lead once, and once
		// more for a notification.
		{"resign", func(int64) string { return "r|" }, ".leader AS l", testTiming.Election / 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sent := &statementLog{}
			e := newTracedEnv(t, sent)
			started := make(chan struct{}, 2)
			ended := make(chan int64, 2)
			proceed := make(chan struct{})
			runWorker(t, e, &taskmill.WorkerConfig{
				Handlers: handlers(func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
					started <- struct{}{}
					select {
					case <-ctx.Done():
						ended <- task.ID
						return context.Cause(ctx)
					case <-proceed:
						return nil
					}
				}, &taskmill.HandlerOptions{Timeout: time.Minute}),
				Queues: defaultQueue(2),
			})
			// A second worker only takes part in the election, so one of the
			// two leads, and the other one does not.
			runWorker(t, e, &taskmill.WorkerConfig{})
			running := e.enqueue(t, sendEmail{}, nil)
			asked := e.enqueue(t, sendEmail{}, nil)
			recv(t, started, "the first handler to start")
			recv(t, started, "the second handler to start")
			waitFor(t, "both workers to listen", func() bool { return len(e.listenerPIDs(t)) == 2 })
			waitFor(t, "a maintainer", func() bool { return e.count(t, "SELECT count(*) FROM {s}.leader") == 1 })
			sent.reset()

			// Each notification commits alone: PostgreSQL delivers equal ones
			// of one transaction only once.
			flooded := time.Now()
			e.exec(t, "DO $$ BEGIN FOR i IN 1.."+strconv.Itoa(flood)+" LOOP PERFORM pg_notify('"+e.schema+"', '"+
				tc.payload(running)+"'); COMMIT; END LOOP; END $$")
			start := time.Now()
			if err := e.client.Cancel(t.Context(), asked); err != nil {
				t.Fatal(err)
			}
			if id := recv(t, ended, "the canceled task's context to end"); id != asked {
				t.Fatalf("a notification alone ended the context of task %d", id)
			}
			if waited := time.Since(start); waited > 2*time.Second {
				t.Errorf("the cancellation waited %s behind %d notifications", waited.Round(time.Millisecond), flood)
			}
			// The workers read at their own pace, however long a slow database
			// takes to commit the flood.
			if n, most := sent.count(tc.reads), int(time.Since(flooded)/tc.every)+flood/1000; n > most {
				t.Errorf("the workers read the database %d times for %d notifications, want at most %d", n, flood, most)
			}
			close(proceed)
			e.waitState(t, running, taskmill.StateCompleted)
		})
	}
}

// The worker that completes the head of a FIFO group fetches the group's
// next task itself: it does not wait for a notification, which a client
// sends for a queue at most once in 50 ms. And the moment for which a queue
// leaves notifications waiting, after one's fetch found nothing, holds back
// notifications only, not that fetch. Here the moment never ends, and each
// task sends a notification. The group's tasks follow each other all the
// same, with two fetches each, and the notifications add none.
func TestFIFOGroupRunsWhileNotificationsWait(t *testing.T) {
	t.Parallel()
	const tasks = 20
	e, fetches := newCountedEnv(t)
	params := make([]taskmill.EnqueueParams, tasks)
	for i := range params {
		params[i] = taskmill.EnqueueParams{Args: sendEmail{UserID: int64(i)}, Options: &taskmill.EnqueueOptions{GroupKey: "account-7"}}
	}
	if _, err := e.client.EnqueueMany(t.Context(), params); err != nil {
		t.Fatal(err)
	}
	notify := replaceSchema("SELECT pg_notify('{s}', 'a|"+taskmill.DefaultQueue+"')", e.schema)
	started, release := make(chan int64, tasks), make(chan struct{})
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
			if _, err := e.pool.Exec(ctx, notify); err != nil {
				return err
			}
			started <- task.Args.UserID
			<-release
			return nil
		}, nil),
		Queues:       defaultQueue(2),
		PollInterval: time.Hour,
		NoMaintainer: true,
	})
	taskmill.SetTiming(w, taskmill.Timing{Quiet: time.Hour})
	startWorker(t, w)

	// While the group's first task runs, a notification's fetch finds
	// nothing: the first task's own, or the one of the listener's start.
	// From that second fetch on, the queue hears no notification.
	recv(t, started, "the group's first task to start")
	waitFor(t, "a notification's fetch to find nothing", func() bool { return fetches.n.Load() >= 2 })

	before := fetches.n.Load()
	close(release)
	for i := 1; i < tasks; i++ {
		if got := recv(t, started, "the group's next task to start"); got != int64(i) {
			t.Fatalf("task %d of the group ran in place %d", got, i)
		}
	}
	// One fetch when a task frees its slot, which comes too early to find
	// the next task, and one when its completion is recorded.
	if n := fetches.n.Load() - before; n > 2*tasks {
		t.Errorf("the worker fetched %d times for %d tasks, want at most %d: notifications got through", n, tasks, 2*tasks)
	}
}

func TestNotificationsWakeIdleWorkers(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	started := make(chan struct{}, 1)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			return nil
		}, nil),
		Queues:       defaultQueue(1),
		PollInterval: time.Minute,
	})
	e.waitListening(t)

	// Without a notification, the worker would next fetch in a minute.
	e.enqueue(t, sendEmail{}, nil)
	recv(t, started, "a notification to wake the worker")
}

func TestWorkerWithUnusableListenConfigPolls(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers:     noopHandlers(),
		Queues:       defaultQueue(1),
		ListenConfig: &pgx.ConnConfig{}, // not created by pgx.ParseConfig
	})

	id := e.enqueue(t, sendEmail{}, nil)
	e.waitState(t, id, taskmill.StateCompleted)
}

func TestListenerReconnects(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	started := make(chan struct{}, 1)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			return nil
		}, nil),
		Queues:       defaultQueue(1),
		PollInterval: time.Minute, // only a notification can wake the worker in time
	})
	e.waitListening(t)
	old := e.listenerPID(t)
	e.exec(t, "SELECT pg_terminate_backend($1)", old)
	waitFor(t, "the listener to reconnect", func() bool {
		pid := e.listenerPID(t)
		return pid != 0 && pid != old
	})

	e.enqueue(t, sendEmail{}, nil)
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the reconnected listener did not wake the worker")
	}
}
