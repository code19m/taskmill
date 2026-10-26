package taskmill_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

func TestDatabaseOutageLogsAFewLines(t *testing.T) {
	t.Parallel()
	// The worker reaches the database through proxies, which the test cuts:
	// one for its pool, and one for its notifications, which may need a
	// direct connection of their own.
	db, direct := newProxy(t, testdb.URL(t)), newProxy(t, testdb.DirectURL(t))
	listen, err := pgx.ParseConfig(testdb.DirectURL(t))
	if err != nil {
		t.Fatal(err)
	}
	listen.Host, listen.Port, listen.Fallbacks = direct.host, direct.port, nil
	cfg, err := pgxpool.ParseConfig(testdb.URL(t))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Host, cfg.ConnConfig.Port, cfg.ConnConfig.Fallbacks = db.host, db.port, nil
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	logs := &logRecorder{}
	e := newEnvWith(t, pool, &taskmill.ClientConfig{Logger: slog.New(logs)})

	// Two tasks run across the outage: one succeeds and one fails, while the
	// database is gone. Their next attempts take long enough for a heartbeat.
	started, release := make(chan struct{}, 2), make(chan struct{})
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(_ context.Context, task *taskmill.Task[sendEmail]) error {
			if task.Attempt > 1 {
				time.Sleep(500 * time.Millisecond)
				return nil
			}
			started <- struct{}{}
			<-release
			if task.Args.UserID == 2 {
				return errors.New("try again")
			}
			return nil
		}, nil),
		Queues:        defaultQueue(2),
		PollInterval:  20 * time.Millisecond,
		LeaseDuration: 600 * time.Millisecond,
		ListenConfig:  listen,
	})
	timing := testTiming
	timing.FailureReport = time.Second
	taskmill.SetTiming(w, timing)
	startWorker(t, w)
	e.waitListening(t)
	ids := []int64{e.enqueue(t, sendEmail{UserID: 1}, nil), e.enqueue(t, sendEmail{UserID: 2}, nil)}
	recv(t, started, "the first task to start")
	recv(t, started, "the second task to start")

	down := time.Now()
	db.cut(true)
	direct.cut(true)
	close(release)
	time.Sleep(3 * time.Second)
	db.cut(false)
	direct.cut(false)

	// The tasks run again, and the operations that the worker repeats log
	// their recovery. Others, such as retention, recover at their next try.
	for _, id := range ids {
		e.waitState(t, id, taskmill.StateCompleted)
	}
	var ops map[string][]slog.Record
	waitFor(t, "the worker's operations to recover", func() bool {
		ops = operationLines(logs.all())
		return recovered(ops, "fetch tasks", "listen for notifications", "record heartbeat", "elect maintainer",
			"renew leases", "record results")
	})
	// The results that could not be recorded say so, once each.
	for _, msg := range []string{
		"complete tasks failed; they are rescued when their leases expire",
		"record attempt failed; the task is rescued when its lease expires",
	} {
		if n := logs.count(msg); n != 1 {
			t.Errorf("%q was logged %d times, want once", msg, n)
		}
	}

	// Each operation warns first, then logs at most one error per
	// FailureReport, however often it fails.
	limit := 1 + int(time.Since(down)/timing.FailureReport)
	for op, lines := range ops {
		checkIncident(t, op, lines, limit)
	}
	fetches := ops["fetch tasks"]
	escalated := slices.ContainsFunc(fetches, func(r slog.Record) bool { return r.Level == slog.LevelError })
	if n, _ := strconv.Atoi(attr(fetches[len(fetches)-1], "failures")); n < 25 || !escalated {
		t.Errorf("fetch tasks failed %d times, escalated %v; want at least 25 failures and an error", n, escalated)
	}
	for _, r := range logs.all() {
		if line := recordText(r); strings.Contains(line, testdb.URL(t)) || strings.Contains(line, "password") {
			t.Errorf("a log line shows the connection string: %s", line)
		}
	}
}

func TestMaintainerWorkStartsCleanInANewTerm(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	logs := &logRecorder{}
	w := newWorker(t, e.withLogger(t, slog.New(logs)), &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{
		{Name: "ledger", Cron: "@hourly", Args: sendEmail{}, Options: &taskmill.EnqueueOptions{GroupKey: "g"}},
	}})
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"})
	startWorker(t, w)
	waitFor(t, "the schedule to be stored", func() bool { return e.count(t, "SELECT count(*) FROM {s}.schedules") == 1 })
	e.exec(t, "UPDATE {s}.schedules SET next_run_at = now() - interval '1 second'")

	// While a transaction holds the schedule's group, its run fails.
	tx := e.begin(t)
	e.txExec(t, tx, "SELECT FROM {s}.groups WHERE group_key = 'g' FOR UPDATE")
	failed := func() int { return len(operationLines(logs.all())["run schedules"]) }
	waitFor(t, "the schedule run to fail", func() bool { return failed() == 1 })

	// Another worker leads for a while, then this one again: the failures of
	// its new term are an incident of their own, not the old one going on.
	e.exec(t, "UPDATE {s}.leader SET worker_id = 'another', expires_at = now() + interval '1 hour'")
	waitFor(t, "the worker to lose the maintainer role", func() bool { return logs.count("no longer maintainer") == 1 })
	e.exec(t, "DELETE FROM {s}.leader")
	waitFor(t, "the worker to lead again", func() bool { return logs.count("elected maintainer") == 2 })
	waitFor(t, "the schedule run to fail in the new term", func() bool { return failed() == 2 })
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the schedule run to recover", func() bool { return failed() == 3 })
	want := []string{"operation failed", "operation failed", "operation recovered"}
	if got := operationLines(logs.all())["run schedules"]; !slices.EqualFunc(got, want, func(r slog.Record, m string) bool {
		return r.Message == m
	}) {
		t.Errorf("run schedules logged %v, want %q", got, want)
	}
}

func TestLogsLeaveOutPayloadsAndMetadata(t *testing.T) {
	t.Parallel()
	const secret = "s3cret-payload"
	const metaSecret = "s3cret-metadata"
	var out syncBuffer
	e := newEnv(t).withLogger(t, slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})))
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(_ context.Context, task *taskmill.Task[sendEmail]) error {
			switch task.Args.UserID {
			case 1: // fails once, then succeeds
				if task.Attempt == 1 {
					return errors.New("try again")
				}
				return nil
			case 2:
				return taskmill.Permanent(errors.New("bad address"))
			default:
				panic("handler bug")
			}
		}, &taskmill.HandlerOptions{Retry: fastRetry}),
		Queues: defaultQueue(3),
	})
	opts := &taskmill.EnqueueOptions{Metadata: map[string]string{"token": metaSecret}, MaxAttempts: 2}
	retried := e.enqueue(t, sendEmail{To: secret, UserID: 1}, opts)
	dead := e.enqueue(t, sendEmail{To: secret, UserID: 2}, opts)
	panicked := e.enqueue(t, sendEmail{To: secret, UserID: 3}, opts)
	e.enqueue(t, report{Name: secret}, &taskmill.EnqueueOptions{Queue: taskmill.DefaultQueue, Metadata: opts.Metadata})
	e.waitState(t, retried, taskmill.StateCompleted)
	e.waitState(t, dead, taskmill.StateDead)
	e.waitState(t, panicked, taskmill.StateDead)
	waitFor(t, "the task without a handler to be snoozed", func() bool {
		return strings.Contains(out.String(), `msg="no handler for task kind; snoozing"`)
	})

	logged := out.String()
	for _, want := range []string{
		`level=DEBUG msg="attempt failed; retrying"`,
		`level=ERROR msg="attempt failed; the task is dead"`,
		`level=WARN msg="no handler for task kind; snoozing"`,
	} {
		if !strings.Contains(logged, want) {
			t.Errorf("no line has %s", want)
		}
	}
	if strings.Contains(logged, secret) || strings.Contains(logged, metaSecret) {
		t.Errorf("the logs show a payload or metadata:\n%s", logged)
	}
}

// checkIncident checks an operation's lines about its incidents: a warning
// first, each line at its message's level, and at most limit about failures.
func checkIncident(t *testing.T, op string, lines []slog.Record, limit int) {
	t.Helper()
	levels := map[string]slog.Level{
		"operation failed": slog.LevelWarn, "operation still failing": slog.LevelError, "operation recovered": slog.LevelInfo,
	}
	failing := 0
	for i, r := range lines {
		// A later line may warn again: on a slow machine a call can fail once
		// more well after the database came back, which is a new incident.
		if r.Level != levels[r.Message] || i == 0 && r.Message != "operation failed" {
			t.Errorf("%s line %d is %s %q", op, i, r.Level, r.Message)
		}
		if r.Message != "operation recovered" {
			failing++
		}
	}
	if failing > limit {
		t.Errorf("%s logged %d failure lines, want at most %d", op, failing, limit)
	}
}

// recovered reports whether each of the operations logged a recovery last.
func recovered(ops map[string][]slog.Record, names ...string) bool {
	for _, op := range names {
		if lines := ops[op]; len(lines) == 0 || lines[len(lines)-1].Message != "operation recovered" {
			return false
		}
	}
	return true
}

// operationLines returns the lines that each operation logged about its
// failures, in order.
func operationLines(records []slog.Record) map[string][]slog.Record {
	ops := map[string][]slog.Record{}
	for _, r := range records {
		if op := attr(r, "operation"); op != "" {
			ops[op] = append(ops[op], r)
		}
	}
	return ops
}

// attr returns the value of a record's attribute, or "" if it has none.
func attr(r slog.Record, key string) string {
	var v string
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			v = a.Value.String()
			return false
		}
		return true
	})
	return v
}

// recordText returns a record's message and attributes, as a line.
func recordText(r slog.Record) string {
	var b strings.Builder
	b.WriteString(r.Message)
	r.Attrs(func(a slog.Attr) bool {
		b.WriteString(" " + a.String())
		return true
	})
	return b.String()
}

// syncBuffer is a buffer that a logger may write to from several goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// proxy forwards connections to a database, and can cut them all, as
// a database that goes away does.
type proxy struct {
	host   string
	port   uint16
	target string

	mu    sync.Mutex
	down  bool
	conns map[net.Conn]bool
}

func newProxy(t *testing.T, url string) *proxy {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	host, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	portNum, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{
		host:   host,
		port:   uint16(portNum),
		target: net.JoinHostPort(cfg.ConnConfig.Host, strconv.Itoa(int(cfg.ConnConfig.Port))),
		conns:  map[net.Conn]bool{},
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.forward(c)
		}
	}()
	// Cleanups run last first: the database stays reachable until the
	// pool closes.
	t.Cleanup(func() {
		_ = ln.Close()
		p.cut(true)
	})
	return p
}

// cut closes every connection and refuses new ones, or accepts them again.
func (p *proxy) cut(down bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = down
	if down {
		for c := range p.conns {
			_ = c.Close()
		}
		clear(p.conns)
	}
}

func (p *proxy) forward(client net.Conn) {
	server, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(context.Background(), "tcp", p.target)
	p.mu.Lock()
	if err != nil || p.down {
		p.mu.Unlock()
		_ = client.Close()
		if server != nil {
			_ = server.Close()
		}
		return
	}
	p.conns[client], p.conns[server] = true, true
	p.mu.Unlock()

	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(server, client)
		_ = server.Close()
		close(done)
	}()
	_, _ = io.Copy(client, server)
	_ = client.Close()
	<-done
	p.mu.Lock()
	delete(p.conns, client)
	delete(p.conns, server)
	p.mu.Unlock()
}
