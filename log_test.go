package taskmill_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

func TestDatabaseOutageLogsAFewLines(t *testing.T) {
	t.Parallel()
	db := newProxy(t)
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

	ran := make(chan struct{}, 1)
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			ran <- struct{}{}
			return nil
		}, nil),
		Queues:        defaultQueue(1),
		PollInterval:  20 * time.Millisecond,
		LeaseDuration: 600 * time.Millisecond,
	})
	timing := testTiming
	timing.FailureReport = time.Second
	taskmill.SetTiming(w, timing)
	startWorker(t, w)
	e.waitListening(t)
	before := e.enqueue(t, sendEmail{}, nil)
	recv(t, ran, "a task before the outage")
	e.waitState(t, before, taskmill.StateCompleted)

	down := time.Now()
	db.cut(true)
	time.Sleep(3 * time.Second)
	db.cut(false)

	// The operations that the worker repeats every moment log their recovery
	// once the database is back. Others, such as retention, recover at their
	// next try.
	var ops map[string]*opLines
	waitFor(t, "the worker's operations to recover", func() bool {
		ops = tallyOperations(logs.all())
		for _, op := range []string{"fetch tasks", "listen for notifications", "record heartbeat", "elect maintainer"} {
			if n := ops[op]; n == nil || n.last != "operation recovered" {
				return false
			}
		}
		return true
	})
	e.enqueue(t, sendEmail{}, nil)
	recv(t, ran, "a task after the outage")

	// An operation logs at most one failure line per FailureReport, however
	// often it fails.
	limit := 1 + int(time.Since(down)/timing.FailureReport)
	for op, n := range ops {
		if n.failed+n.still > limit {
			t.Errorf("%s logged %d failure lines in %s, want at most %d", op, n.failed+n.still, time.Since(down), limit)
		}
	}
	if f := ops["fetch tasks"]; f.failures < 25 || f.still == 0 {
		t.Errorf("fetch tasks failed %d times and logged %d summaries; want at least 25 failures and a summary", f.failures, f.still)
	}
	for _, r := range logs.all() {
		if line := recordText(r); strings.Contains(line, testdb.URL(t)) || strings.Contains(line, "password") {
			t.Errorf("a log line shows the connection string: %s", line)
		}
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

// opLines counts the lines that an operation logged about its failures.
type opLines struct {
	failed, still, recovered int
	failures                 int    // failures that the recovery lines report
	last                     string // the last message
}

// tallyOperations counts the lines of each operation in records.
func tallyOperations(records []slog.Record) map[string]*opLines {
	ops := map[string]*opLines{}
	for _, r := range records {
		op := attr(r, "operation")
		if op == "" {
			continue
		}
		n := ops[op]
		if n == nil {
			n = &opLines{}
			ops[op] = n
		}
		switch n.last = r.Message; r.Message {
		case "operation failed":
			n.failed++
		case "operation still failing":
			n.still++
		case "operation recovered":
			n.recovered++
			f, _ := strconv.Atoi(attr(r, "failures"))
			n.failures += f
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

// proxy forwards connections to the test database, and can cut them all, as
// a database that goes away does.
type proxy struct {
	host   string
	port   uint16
	target string

	mu    sync.Mutex
	down  bool
	conns map[net.Conn]bool
}

func newProxy(t *testing.T) *proxy {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(testdb.URL(t))
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
