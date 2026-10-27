package taskmill_test

import (
	"context"
	"log/slog"
	"strings"
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
// notifications do not reach it, and every idlePoll while they do. cfg, which
// may be nil, sets the rest of its configuration.
func startIdleWorker(t *testing.T, e *env, idlePoll time.Duration, cfg *taskmill.WorkerConfig) (*taskmill.Worker, <-chan struct{}) {
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
	timing := testTiming
	timing.IdlePoll = idlePoll
	taskmill.SetTiming(w, timing)
	startWorker(t, w)
	return w, started
}

func TestIdleWorkerFetchesRarelyWhileNotificationsArrive(t *testing.T) {
	t.Parallel()
	e, fetches := newCountedEnv(t)
	w, started := startIdleWorker(t, e, time.Minute, nil)
	waitFor(t, "notifications to arrive", func() bool { return taskmill.Notifying(w) })
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
	w, started := startIdleWorker(t, e, 200*time.Millisecond, &taskmill.WorkerConfig{ListenConfig: listen})
	waitFor(t, "the worker to find that notifications do not arrive", func() bool {
		return logs.count("notifications do not arrive; idle queues poll every PollInterval") == 1
	})
	if taskmill.Notifying(w) {
		t.Fatal("the worker counts on notifications that do not arrive")
	}

	before := fetches.n.Load()
	time.Sleep(500 * time.Millisecond)
	if n := fetches.n.Load() - before; n < 20 {
		t.Fatalf("the worker fetched %d times in 500 ms; polling every 20 ms, 4 queues would be about 100", n)
	}
	begin := time.Now()
	e.enqueue(t, sendEmail{}, nil)
	recv(t, started, "the task to start")
	if d := time.Since(begin); d > time.Second {
		t.Fatalf("pickup took %s", d)
	}
	if n := logs.count("notifications do not arrive; idle queues poll every PollInterval"); n != 1 {
		t.Fatalf("the worker said %d times that notifications do not arrive, want once", n)
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
	w, started := startIdleWorker(t, e, time.Minute, &taskmill.WorkerConfig{ListenConfig: listen})
	waitFor(t, "notifications to arrive", func() bool { return taskmill.Notifying(w) })

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
	waitFor(t, "notifications to arrive again", func() bool { return taskmill.Notifying(w) })
}

func TestIdleWorkerFindsATaskWhoseNotificationWasLost(t *testing.T) {
	t.Parallel()
	e, _ := newCountedEnv(t)
	// Without a maintainer to wake its queues, only the idle poll finds it.
	w, started := startIdleWorker(t, e, time.Second, &taskmill.WorkerConfig{NoMaintainer: true})
	waitFor(t, "notifications to arrive", func() bool { return taskmill.Notifying(w) })

	// The task becomes available without a notification, as when the
	// producer that owed it crashed.
	begin := time.Now()
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
	           VALUES ('a', 'email.send', 'available', now(), '{}', 1)`)
	recv(t, started, "the task to start")
	if d := time.Since(begin); d > 1500*time.Millisecond {
		t.Fatalf("the task started after %s, beyond the idle poll of 1 s", d)
	}
}

func TestIdleWorkerFindsATaskCommittedAfterItsNotification(t *testing.T) {
	t.Parallel()
	e, _ := newCountedEnv(t)
	w, started := startIdleWorker(t, e, time.Minute, nil)
	waitFor(t, "notifications to arrive", func() bool { return taskmill.Notifying(w) })

	// The first enqueue notifies; the second, within the throttle's 50 ms,
	// leaves its notification to the end of that window, which comes before
	// its transaction commits. The maintainer then wakes the queue.
	e.enqueue(t, sendEmail{}, nil)
	recv(t, started, "the first task to start")
	tx := e.begin(t)
	if _, err := e.client.EnqueueTx(t.Context(), taskmill.Pgx(tx), sendEmail{}, nil); err != nil {
		t.Fatal(err)
	}
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
