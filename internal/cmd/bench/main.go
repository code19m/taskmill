// Command bench measures taskmill against the targets in
// docs/design/performance.md, and checks its reliability under faults.
//
// It runs against the database in TASKMILL_TEST_DATABASE_URL, in a schema of
// its own that it recreates, and prints a Markdown report:
//
//	go run ./internal/cmd/bench -scenarios enqueue,throughput,latency,chaos
//
// The chaos scenario starts worker processes and kills them with SIGKILL
// while they work, then checks that every task completed exactly once.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
)

const schema = "taskmill_bench"

type noop struct {
	N    int   `json:"n"`
	Sent int64 `json:"sent,omitempty"` // UnixNano after the enqueue committed
}

func (noop) Kind() string { return "bench.noop" }

// tracked records each execution in a table, so the chaos scenario can count
// them after workers die.
type tracked struct {
	N int `json:"n"`
}

func (tracked) Kind() string { return "bench.tracked" }

func main() {
	var (
		url       = flag.String("db", os.Getenv("TASKMILL_TEST_DATABASE_URL"), "database URL")
		scenarios = flag.String("scenarios", "enqueue,throughput,latency,chaos", "comma-separated scenarios")
		duration  = flag.Duration("duration", 10*time.Second, "duration of timed scenarios")
		tasks     = flag.Int("tasks", 200_000, "tasks for the throughput scenario")
		role      = flag.String("role", "", "internal: run as a chaos worker process")
	)
	flag.Parse()
	if *url == "" {
		fail(errors.New("set -db or TASKMILL_TEST_DATABASE_URL"))
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool := connect(ctx, *url, 64)
	defer pool.Close()

	if *role == "worker" {
		runChaosWorker(ctx, pool)
		return
	}

	for s := range strings.SplitSeq(*scenarios, ",") {
		switch s {
		case "enqueue":
			benchEnqueue(ctx, pool, *duration)
		case "throughput":
			benchThroughput(ctx, pool, *tasks)
		case "latency":
			benchLatency(ctx, pool, *duration)
		case "chaos":
			benchChaos(ctx, pool, *url)
		default:
			fail(fmt.Errorf("unknown scenario %q", s))
		}
	}
}

func connect(ctx context.Context, url string, conns int32) *pgxpool.Pool {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		fail(err)
	}
	cfg.MaxConns = conns
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		fail(err)
	}
	return pool
}

// fresh recreates the benchmark schema and returns a client for it.
func fresh(ctx context.Context, pool *pgxpool.Pool) *taskmill.Client {
	if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
		fail(err)
	}
	return client(ctx, pool, true)
}

func client(ctx context.Context, pool *pgxpool.Pool, migrate bool) *taskmill.Client {
	if migrate {
		if err := taskmill.Migrate(ctx, pool, &taskmill.MigrateConfig{Schema: schema}); err != nil {
			fail(err)
		}
	}
	c, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Schema: schema})
	if err != nil {
		fail(err)
	}
	return c
}

// T1 and T2: enqueue throughput, one task per transaction and in batches.
func benchEnqueue(ctx context.Context, pool *pgxpool.Pool, d time.Duration) {
	c := fresh(ctx, pool)
	fmt.Println("## Enqueue")
	fmt.Println()
	fmt.Println("| ID | Workload | Result |")
	fmt.Println("|---|---|---|")

	one := timed(ctx, 16, d, func(ctx context.Context) int {
		if _, err := c.Enqueue(ctx, noop{}, nil); err != nil && ctx.Err() == nil {
			fail(err)
		}
		return 1
	})
	fmt.Printf("| T1 | Enqueue one task per transaction, 16 producers | %s tasks/s |\n", rate(one, d))

	batch := make([]taskmill.EnqueueParams, 100)
	for i := range batch {
		batch[i] = taskmill.EnqueueParams{Args: noop{N: i}}
	}
	many := timed(ctx, 8, d, func(ctx context.Context) int {
		if _, err := c.EnqueueMany(ctx, batch); err != nil && ctx.Err() == nil {
			fail(err)
		}
		return len(batch)
	})
	fmt.Printf("| T2 | Enqueue in batches of 100, 8 producers | %s tasks/s |\n", rate(many, d))
	fmt.Println()
}

// timed runs fn from n goroutines for d and returns how many units they did.
func timed(ctx context.Context, n int, d time.Duration, fn func(context.Context) int) int64 {
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	var total atomic.Int64
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			for ctx.Err() == nil {
				total.Add(int64(fn(ctx)))
			}
		})
	}
	wg.Wait()
	return total.Load()
}

// T3, R1, and R2: end-to-end processing of a backlog, with history kept.
func benchThroughput(ctx context.Context, pool *pgxpool.Pool, n int) {
	c := fresh(ctx, pool)
	batch := make([]taskmill.EnqueueParams, 1000)
	for i := range batch {
		batch[i] = taskmill.EnqueueParams{Args: noop{N: i}}
	}
	for done := 0; done < n; done += len(batch) {
		if _, err := c.EnqueueMany(ctx, batch); err != nil {
			fail(err)
		}
	}

	var completed atomic.Int64
	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[noop](func(context.Context, *taskmill.Task[noop]) error {
		completed.Add(1)
		return nil
	}), nil)
	w, err := taskmill.NewWorker(c, &taskmill.WorkerConfig{
		Handlers: &hs,
		Queues:   map[string]taskmill.QueueOptions{taskmill.DefaultQueue: {Concurrency: 64}},
	})
	if err != nil {
		fail(err)
	}

	walStart := walLSN(ctx, pool)
	statsStart := statements(ctx, pool)
	wctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- w.Run(wctx) }()
	for completed.Load() < int64(n) && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	elapsed := time.Since(start)
	cancel()
	if err := <-done; err != nil {
		fail(err)
	}
	// Wait for the last completions to be recorded.
	for history(ctx, pool) < n && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	wal := walDiff(ctx, pool, walStart)
	stmts := statements(ctx, pool) - statsStart

	fmt.Println("## Processing")
	fmt.Println()
	fmt.Println("| ID | Workload | Result |")
	fmt.Println("|---|---|---|")
	fmt.Printf("| T3 | %d tasks end to end, 1 worker with concurrency 64, history kept | %s tasks/s |\n",
		n, rate(int64(n), elapsed))
	fmt.Printf("| R2 | WAL written per task, fetch to history | %.2f KiB |\n", float64(wal)/float64(n)/1024)
	if stmts > 0 {
		fmt.Printf("| R1 | SQL statements per task while processing | %.2f |\n", float64(stmts)/float64(n))
	}
	fmt.Println()
}

// L1: pickup latency of an idle worker woken by notifications.
func benchLatency(ctx context.Context, pool *pgxpool.Pool, d time.Duration) {
	c := fresh(ctx, pool)
	var mu sync.Mutex
	var lat []time.Duration
	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[noop](func(_ context.Context, t *taskmill.Task[noop]) error {
		mu.Lock()
		lat = append(lat, time.Since(time.Unix(0, t.Args.Sent)))
		mu.Unlock()
		return nil
	}), nil)
	w, err := taskmill.NewWorker(c, &taskmill.WorkerConfig{
		Handlers:     &hs,
		Queues:       map[string]taskmill.QueueOptions{taskmill.DefaultQueue: {}},
		PollInterval: time.Minute, // only notifications wake the worker
	})
	if err != nil {
		fail(err)
	}
	wctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(wctx) }()
	time.Sleep(time.Second) // let the worker connect

	sent := 0
	end := time.Now().Add(d)
	for time.Now().Before(end) && ctx.Err() == nil {
		// The enqueue's commit is the starting point, so the send time is
		// stamped in a second statement in the same transaction.
		err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			_, err := c.EnqueueTx(ctx, taskmill.Pgx(tx), noop{Sent: time.Now().UnixNano()}, nil)
			return err
		})
		if err != nil {
			fail(err)
		}
		sent++
		time.Sleep(100 * time.Millisecond) // beyond the notification throttle window
	}
	time.Sleep(time.Second)
	cancel()
	if err := <-done; err != nil {
		fail(err)
	}

	mu.Lock()
	defer mu.Unlock()
	slices.Sort(lat)
	fmt.Println("## Latency")
	fmt.Println()
	fmt.Println("| ID | Measure | Result |")
	fmt.Println("|---|---|---|")
	fmt.Printf("| L1 | Pickup latency of an idle worker, %d tasks | p50 %s, p99 %s, max %s |\n",
		len(lat), pct(lat, 50), pct(lat, 99), pct(lat, 100))
	fmt.Println()
}

// F1 to F3: kill worker processes while they work, and check that every task
// completes exactly once and nothing is lost.
func benchChaos(ctx context.Context, pool *pgxpool.Pool, url string) {
	const n, procs, concurrency = 60_000, 4, 16
	c := fresh(ctx, pool)
	if _, err := pool.Exec(ctx, "CREATE TABLE "+schema+".executions (n integer NOT NULL)"); err != nil {
		fail(err)
	}
	batch := make([]taskmill.EnqueueParams, 1000)
	for done := 0; done < n; done += len(batch) {
		for i := range batch {
			batch[i] = taskmill.EnqueueParams{Args: tracked{N: done + i}, Options: &taskmill.EnqueueOptions{MaxAttempts: 100}}
		}
		if _, err := c.EnqueueMany(ctx, batch); err != nil {
			fail(err)
		}
	}

	self, err := os.Executable()
	if err != nil {
		fail(err)
	}
	start := func() *exec.Cmd {
		cmd := exec.CommandContext(ctx, self, "-role", "worker", "-db", url) //nolint:gosec // re-runs this binary
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			fail(err)
		}
		return cmd
	}
	workers := make([]*exec.Cmd, procs)
	for i := range workers {
		workers[i] = start()
	}

	kills := 0
	begin := time.Now()
	for history(ctx, pool) < n && ctx.Err() == nil {
		time.Sleep(time.Second)
		i := rand.IntN(procs) //nolint:gosec // picks a victim, no security role
		_ = workers[i].Process.Kill()
		_ = workers[i].Wait()
		kills++
		workers[i] = start()
		if time.Since(begin) > 10*time.Minute {
			fail(errors.New("chaos run did not finish in 10 minutes"))
		}
	}
	for _, w := range workers {
		_ = w.Process.Signal(syscall.SIGTERM)
		_ = w.Wait()
	}

	reportChaos(ctx, pool, n, procs, concurrency, kills, time.Since(begin))
}

// reportChaos checks and prints the outcome of a chaos run.
func reportChaos(ctx context.Context, pool *pgxpool.Pool, n, procs, concurrency, kills int, took time.Duration) {
	var completed, distinct, executions, leftover int
	q := func(sql string, dst *int) {
		if err := pool.QueryRow(ctx, sql).Scan(dst); err != nil {
			fail(err)
		}
	}
	q("SELECT count(*) FROM "+schema+".task_history", &completed)
	q("SELECT count(DISTINCT payload->>'n') FROM "+schema+".task_history", &distinct)
	q("SELECT count(*) FROM "+schema+".executions", &executions)
	q("SELECT count(*) FROM "+schema+".tasks", &leftover)
	extra := executions - n

	check := func(ok bool) string {
		if ok {
			return "pass"
		}
		return "**FAIL**"
	}
	fmt.Println("## Reliability under faults")
	fmt.Println()
	fmt.Printf("%d tasks, %d worker processes with concurrency %d, one killed with SIGKILL every second: %d kills in %s.\n\n",
		n, procs, concurrency, kills, took.Round(time.Second))
	fmt.Println("| ID | Check | Result | |")
	fmt.Println("|---|---|---|---|")
	fmt.Printf("| F1 | Every task completed, none left behind | %d completed, %d left | %s |\n",
		completed, leftover, check(completed == n && leftover == 0))
	fmt.Printf("| F2 | Every task completed exactly once | %d distinct of %d | %s |\n",
		distinct, completed, check(distinct == n && completed == n))
	fmt.Printf("| F3 | Extra executions at most kills × concurrency | %d extra, bound %d | %s |\n",
		extra, kills*concurrency, check(extra >= 0 && extra <= kills*concurrency))
	fmt.Println()
}

func runChaosWorker(ctx context.Context, pool *pgxpool.Pool) {
	c, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Schema: schema})
	if err != nil {
		if ctx.Err() != nil {
			return // stopped while starting
		}
		fail(err)
	}
	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[tracked](func(ctx context.Context, t *taskmill.Task[tracked]) error {
		if _, err := pool.Exec(ctx, "INSERT INTO "+schema+".executions VALUES ($1)", t.Args.N); err != nil {
			return err
		}
		time.Sleep(time.Duration(10+rand.IntN(30)) * time.Millisecond) //nolint:gosec // simulated work
		return nil
	}), nil)
	w, err := taskmill.NewWorker(c, &taskmill.WorkerConfig{
		Handlers:      &hs,
		Queues:        map[string]taskmill.QueueOptions{taskmill.DefaultQueue: {Concurrency: 16}},
		LeaseDuration: 3 * time.Second,
		DrainTimeout:  time.Second,
	})
	if err != nil {
		fail(err)
	}
	if err := w.Run(ctx); err != nil {
		fail(err)
	}
}

func history(ctx context.Context, pool *pgxpool.Pool) int {
	var n int
	_ = pool.QueryRow(ctx, "SELECT count(*) FROM "+schema+".task_history").Scan(&n)
	return n
}

func walLSN(ctx context.Context, pool *pgxpool.Pool) string {
	var lsn string
	if err := pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn); err != nil {
		fail(err)
	}
	return lsn
}

func walDiff(ctx context.Context, pool *pgxpool.Pool, from string) int64 {
	var n int64
	if err := pool.QueryRow(ctx, "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), $1::pg_lsn)::bigint", from).Scan(&n); err != nil {
		fail(err)
	}
	return n
}

// statements returns the number of statements run, if pg_stat_statements is
// installed, or 0.
func statements(ctx context.Context, pool *pgxpool.Pool) int64 {
	var n int64
	err := pool.QueryRow(ctx, "SELECT coalesce(sum(calls), 0)::bigint FROM pg_stat_statements WHERE query ILIKE '%"+schema+"%'").Scan(&n)
	if err != nil {
		return 0
	}
	return n
}

func rate(n int64, d time.Duration) string {
	return commas(int64(float64(n) / d.Seconds()))
}

func commas(n int64) string {
	s := strconv.FormatInt(n, 10)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

func pct(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := min(len(sorted)-1, (len(sorted)*p+99)/100-1)
	return sorted[max(i, 0)].Round(100 * time.Microsecond)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "bench:", err)
	os.Exit(1)
}
