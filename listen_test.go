package taskmill_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

// idleQueues are the queues of the idle-polling tests.
var idleQueues = map[string]taskmill.QueueOptions{"default": {}, "a": {}, "b": {}, "c": {}}

// newCountedEnv returns an env whose pool counts the fetches its connections
// send.
func newCountedEnv(t *testing.T) (*env, *fetchCounter) {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(testdb.URL(t))
	if err != nil {
		t.Fatal(err)
	}
	fetches := &fetchCounter{}
	cfg.ConnConfig.Tracer = fetches
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return newEnvWith(t, pool, nil), fetches
}

// startIdleWorker runs a worker of idleQueues that polls every 20 ms while
// the maintainer's heartbeats do not reach it, and every hour, as tests do,
// while they do. cfg, which may be nil, sets the rest of its configuration.
func startIdleWorker(t *testing.T, e *env, cfg *taskmill.WorkerConfig) (*taskmill.Worker, <-chan struct{}) {
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
	w := newWorker(t, e, cfg)
	startWorker(t, w)
	return w, started
}

func TestIdleWorkerFetchesRarelyWhileHeartbeatsArrive(t *testing.T) {
	t.Parallel()
	e, fetches := newCountedEnv(t)
	w, started := startIdleWorker(t, e, nil)
	waitFor(t, "heartbeats to arrive", func() bool { return taskmill.Notifying(w) })
	// The fetches of the start, of the listener's start, and of the polls
	// that were already waiting end first.
	time.Sleep(200 * time.Millisecond)

	before := fetches.n.Load()
	time.Sleep(time.Second)
	if n := fetches.n.Load() - before; n > int64(len(idleQueues)) {
		t.Fatalf("an idle worker with 4 queues fetched %d times in a second; polling every 20 ms would be 200", n)
	}

	// A notification still wakes it at once.
	begin := time.Now()
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "b"})
	recv(t, started, "the task to start")
	if d := time.Since(begin); d > time.Second {
		t.Fatalf("pickup took %s", d)
	}
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
	// A task whose notification does not arrive starts by the next poll. A
	// second is 50 poll intervals: room for slow fetches, and far less than
	// the hour of idle polling.
	begin := time.Now()
	e.enqueue(t, sendEmail{}, nil)
	recv(t, started, "the task to start")
	if d := time.Since(begin); d > time.Second {
		t.Fatalf("pickup took %s", d)
	}
	if n := logs.count(heartbeatsMissing); n != 1 {
		t.Fatalf("the worker said %d times that heartbeats do not arrive, want once", n)
	}
}

func TestIdleWorkerPollsWhileTheMaintainerFailsOver(t *testing.T) {
	t.Parallel()
	e, _ := newCountedEnv(t)
	// A maintainer that crashed holds the role until its lease runs out, so
	// no heartbeat comes, and nothing wakes a queue whose notification was
	// lost.
	e.exec(t, `INSERT INTO {s}.leader (name, worker_id, elected_at, expires_at)
	           VALUES ('maintainer', 'crashed', now(), now() + interval '1 minute')`)
	w, started := startIdleWorker(t, e, nil)
	time.Sleep(500 * time.Millisecond)
	if taskmill.Notifying(w) {
		t.Fatal("the worker counts on notifications without heartbeats")
	}

	begin := time.Now()
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
	           VALUES ('a', 'email.send', 'available', now(), '{}', 1)`)
	recv(t, started, "the task to start")
	if d := time.Since(begin); d > time.Second {
		t.Fatalf("the task started after %s, beyond PollInterval", d)
	}
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
	begin := time.Now()
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "c"})
	recv(t, started, "the task to start")
	if d := time.Since(begin); d > time.Second {
		t.Fatalf("pickup took %s", d)
	}

	direct.cut(false)
	waitFor(t, "heartbeats to arrive again", func() bool { return taskmill.Notifying(w) })
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
	begin := time.Now()
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "c"})
	recv(t, started, "the task to start")
	if d := time.Since(begin); d > time.Second {
		t.Fatalf("pickup took %s", d)
	}
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
	waitFor(t, "the worker to find that heartbeats do not arrive", func() bool {
		return logs.count(heartbeatsMissing) == missing+1 && !taskmill.Notifying(w)
	})
	time.Sleep(2 * time.Second) // the worker checks the listener each 600 ms
	if got := e.listenerPID(t); got != pid {
		t.Fatalf("the worker replaced a listener that answers: pid %d, then %d", pid, got)
	}
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "a"})
	recv(t, started, "the task to start")
}

// TestMaintainerHandsOverWithoutAGap checks that when the maintainer stops,
// another takes over at once, before workers miss its heartbeats, though
// workers that do not lead try to take over only once an hour here.
func TestMaintainerHandsOverWithoutAGap(t *testing.T) {
	t.Parallel()
	e, _ := newCountedEnv(t)
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

	var fellBack atomic.Bool
	watched := make(chan struct{})
	go func() {
		defer close(watched)
		// Longer than the workers wait for a heartbeat.
		for end := time.Now().Add(4 * time.Second); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
			if !notifying() {
				fellBack.Store(true)
			}
		}
	}()
	leader := e.count(t, "SELECT count(*) FROM {s}.leader")
	stopLeader()
	<-watched
	if fellBack.Load() || logs.count(heartbeatsMissing) > 0 {
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
	begin := time.Now()
	e.enqueue(t, sendEmail{}, nil)
	recv(t, started, "the task to start")
	if d := time.Since(begin); d > time.Second {
		t.Fatalf("pickup took %s", d)
	}
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
	// producer that owed it crashed. The maintainer wakes its queue.
	begin := time.Now()
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
	           VALUES ('a', 'email.send', 'available', now(), '{}', 1)`)
	recv(t, started, "the task to start")
	if d := time.Since(begin); d > time.Second {
		t.Fatalf("the task started after %s", d)
	}
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
	begin := time.Now()
	recv(t, started, "the second task to start")
	if d := time.Since(begin); d > time.Second {
		t.Fatalf("the task started %s after its transaction committed", d)
	}
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

	// runsSoon fails the test unless task id runs within a second.
	runsSoon := func(how string, id int64) {
		t.Helper()
		select {
		case got := <-ran:
			if got != id {
				t.Fatalf("%s: task %d ran, want %d", how, got, id)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s: task %d did not run within a second", how, id)
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

// fetchCounter counts the fetches that a pool's connections send.
type fetchCounter struct {
	n atomic.Int64
}

func (*fetchCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
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
