package main

import (
	"bufio"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
)

// child configures a child process: a worker or a producer that the fault
// scenarios start, kill, and cut off from the database.
type child struct {
	Role         string        `json:"role"`                    // "worker" or "producer"
	Name         string        `json:"name,omitempty"`          // recorded by the handlers that record who ran them
	Queues       int           `json:"queues,omitempty"`        // serve queues q0 to q<Queues-1>; 0 serves the default queue
	Concurrency  int           `json:"concurrency,omitempty"`   // per queue
	Lease        time.Duration `json:"lease,omitempty"`         // 0 is the default
	NoMaintainer bool          `json:"no_maintainer,omitempty"` // stay out of the maintainer election
	Conns        int32         `json:"conns,omitempty"`         // pool size; 0 is 64
	Keys         int           `json:"keys,omitempty"`          // producer: enqueue every other key below Keys
	Parity       int           `json:"parity,omitempty"`        // producer: starting with this one
}

// proc is a running child process.
type proc struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
}

// spawn starts this program again as a child process with cfg, connected to
// the database at url.
func spawn(ctx context.Context, url string, cfg child) *proc {
	self, err := os.Executable()
	if err != nil {
		fail(err)
	}
	b, _ := json.Marshal(cfg)                                              //nolint:errchkjson // plain fields always encode
	cmd := exec.CommandContext(ctx, self, "-db", url, "-child", string(b)) //nolint:gosec // re-runs this binary
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		fail(err)
	}
	if err := cmd.Start(); err != nil {
		fail(err)
	}
	return &proc{cmd: cmd, stdin: stdin}
}

// kill stops the process at once, as a crash does.
func (p *proc) kill() {
	_ = p.cmd.Process.Kill()
	_ = p.cmd.Wait()
}

// stop stops the process gracefully, and fails if it fails. A process that
// had not yet set up its signal handling ends by the signal, which is fine.
func (p *proc) stop() {
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	err := p.cmd.Wait()
	if exit, ok := errors.AsType[*exec.ExitError](err); ok {
		if ws, ok := exit.Sys().(syscall.WaitStatus); ok && ws.Signaled() && ws.Signal() == syscall.SIGTERM {
			return
		}
	}
	if err != nil {
		fail(fmt.Errorf("child process: %w", err))
	}
}

// wait waits for the process to end, and fails if it fails.
func (p *proc) wait() {
	if err := p.cmd.Wait(); err != nil {
		fail(fmt.Errorf("child process: %w", err))
	}
}

// release lets the child's held handlers of a wave return.
func (p *proc) release(wave int) {
	if _, err := fmt.Fprintln(p.stdin, "release", wave); err != nil {
		fail(err)
	}
}

func runChild(ctx context.Context, url, spec string) {
	var cfg child
	if err := json.Unmarshal([]byte(spec), &cfg); err != nil {
		fail(err)
	}
	// The parent writes to stdin, which ends when the parent does, however
	// it ends: the child stops then too, rather than go on taking tasks.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	waves := [3]chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	go func() {
		defer cancel()
		lines := bufio.NewScanner(os.Stdin)
		for lines.Scan() {
			if n, err := strconv.Atoi(strings.TrimPrefix(lines.Text(), "release ")); err == nil && n > 0 && n < len(waves) {
				close(waves[n])
			}
		}
	}()

	pool := connect(ctx, url, cmp.Or(cfg.Conns, 64))
	defer pool.Close()
	c, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Schema: schema})
	if err != nil {
		if ctx.Err() != nil {
			return // stopped while starting
		}
		fail(err)
	}
	switch cfg.Role {
	case "worker":
		runWorker(ctx, c, pool, cfg, waves)
	case "producer":
		runProducer(ctx, c, pool, cfg)
	default:
		fail(fmt.Errorf("unknown child role %q", cfg.Role))
	}
}

// step is one task of a FIFO group in the order scenario.
type step struct {
	G int `json:"g"`
	S int `json:"s"`
}

func (step) Kind() string { return "bench.step" }

// hold records that it started, then waits until its wave is released,
// ignoring its context, as a handler that does not respect cancellation does,
// and records that it ended.
type hold struct {
	N    int `json:"n"`
	Wave int `json:"wave"`
}

func (hold) Kind() string { return "bench.hold" }

// stepFailures is the percentage of order scenario attempts that fail.
const stepFailures = 5

// runWorker runs a worker with the benchmark's handlers. The handlers of
// hold tasks wait for their wave's channel to close.
func runWorker(ctx context.Context, c *taskmill.Client, pool *pgxpool.Pool, cfg child, waves [3]chan struct{}) {
	insert := func(ctx context.Context, sql string, args ...any) error {
		_, err := pool.Exec(ctx, q(sql), args...)
		return err
	}

	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[tracked](func(ctx context.Context, t *taskmill.Task[tracked]) error {
		if err := insert(ctx, "INSERT INTO {s}.executions VALUES ($1)", t.Args.N); err != nil {
			return err
		}
		time.Sleep(time.Duration(10+rand.IntN(30)) * time.Millisecond) //nolint:gosec // simulated work
		return nil
	}), nil)
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[step](func(ctx context.Context, t *taskmill.Task[step]) error {
		ok := rand.IntN(100) >= stepFailures //nolint:gosec // picks failures, no security role
		if err := insert(ctx, "INSERT INTO {s}.steps (g, s, ok) VALUES ($1, $2, $3)", t.Args.G, t.Args.S, ok); err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("step %d of group %d failed on purpose", t.Args.S, t.Args.G)
		}
		return nil
	}), nil)
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[hold](func(ctx context.Context, t *taskmill.Task[hold]) error {
		ctx = context.WithoutCancel(ctx)
		record := func(ctx context.Context, phase string) error {
			return insert(ctx, "INSERT INTO {s}.holds (n, worker, attempt, phase) VALUES ($1, $2, $3, $4)",
				t.Args.N, cfg.Name, t.Attempt, phase)
		}
		if err := record(ctx, "start"); err != nil {
			return err
		}
		<-waves[t.Args.Wave]
		// A worker cut off from the database returns all the same.
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		_ = record(ctx, "end")
		return nil
	}), nil)
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[noop](func(context.Context, *taskmill.Task[noop]) error { return nil }), nil)

	queues := map[string]taskmill.QueueOptions{}
	if cfg.Queues == 0 {
		queues[taskmill.DefaultQueue] = taskmill.QueueOptions{Concurrency: cfg.Concurrency}
	}
	for i := range cfg.Queues {
		queues[queueName(i)] = taskmill.QueueOptions{Concurrency: cfg.Concurrency}
	}
	w, err := taskmill.NewWorker(c, &taskmill.WorkerConfig{
		Handlers:      &hs,
		Queues:        queues,
		LeaseDuration: cfg.Lease,
		DrainTimeout:  time.Second,
		NoMaintainer:  cfg.NoMaintainer,
	})
	if err != nil {
		fail(err)
	}
	if err := w.Run(ctx); err != nil {
		fail(err)
	}
}

func queueName(i int) string { return "q" + strconv.Itoa(i) }

// keyed is a task of the dedupe scenario.
type keyed struct {
	Key int `json:"key"`
}

func (keyed) Kind() string { return "bench.keyed" }

// runProducer enqueues every other key below cfg.Keys, in batches of 100, and
// records each result. Every fifth key also joins one of 100 FIFO groups.
func runProducer(ctx context.Context, c *taskmill.Client, pool *pgxpool.Pool, cfg child) {
	var results [][]any
	batch := make([]taskmill.EnqueueParams, 0, 100)
	keys := make([]int, 0, cap(batch))
	flush := func() {
		res, err := c.EnqueueMany(ctx, batch)
		if err != nil {
			fail(err)
		}
		for i, r := range res {
			results = append(results, []any{keys[i], r.ID, r.Duplicate})
		}
		batch, keys = batch[:0], keys[:0]
	}
	for k := cfg.Parity; k < cfg.Keys; k += 2 {
		opts := &taskmill.EnqueueOptions{DedupeKey: strconv.Itoa(k)}
		if k%5 == 0 {
			opts.GroupKey = "g" + strconv.Itoa(k/5%100)
		}
		batch = append(batch, taskmill.EnqueueParams{Args: keyed{Key: k}, Options: opts})
		keys = append(keys, k)
		if len(batch) == cap(batch) {
			flush()
		}
	}
	if len(batch) > 0 {
		flush()
	}
	_, err := pool.CopyFrom(ctx, pgx.Identifier{schema, "results"}, []string{"key", "task_id", "duplicate"},
		pgx.CopyFromRows(results))
	if err != nil {
		fail(err)
	}
}
