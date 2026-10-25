// Command bench measures taskmill against the targets in docs/benchmarks.md,
// and checks its reliability under faults.
//
// It runs against the database in TASKMILL_TEST_DATABASE_URL, in a schema of
// its own that it recreates, and prints a Markdown report:
//
//	go run ./internal/cmd/bench -scenarios enqueue,throughput,chaos
//
// The fault scenarios start worker and producer processes, kill them, cut
// them off from the database, and restart the database, then check each
// invariant exactly. A failed check fails the run.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
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
	"github.com/code19m/taskmill/outbox"
)

const schema = "taskmill_bench"

// allScenarios is every scenario that runs by default, in order.
const allScenarios = "enqueue,throughput,e2e,fifo,latency,timing,recovery,scale,queues,resources," +
	"chaos,order,dedupe,restart,partition"

type noop struct {
	N    int   `json:"n"`
	Sent int64 `json:"sent,omitempty"` // UnixNano just before the enqueue's transaction
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
		scenarios = flag.String("scenarios", allScenarios, "comma-separated scenarios; soak runs only when listed")
		duration  = flag.Duration("duration", 10*time.Second, "duration of timed scenarios")
		tasks     = flag.Int("tasks", 200_000, "tasks for the throughput scenarios")
		soak      = flag.Duration("soak", time.Hour, "duration of the soak scenario")
		container = flag.String("container", "", "Docker container of the database, for the restart scenario")
		short     = flag.Bool("short", false, "run the fault scenarios at a tenth of their size, as CI does")
		spec      = flag.String("child", "", "internal: run as a child process with this JSON configuration")
	)
	flag.Parse()
	if *url == "" {
		fail(errors.New("set -db or TASKMILL_TEST_DATABASE_URL"))
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *spec != "" {
		runChild(ctx, *url, *spec)
		return
	}
	pool := connect(ctx, *url, 64)
	defer pool.Close()

	size := 1
	if *short {
		size = 10
	}
	for s := range strings.SplitSeq(*scenarios, ",") {
		switch s {
		case "enqueue":
			benchEnqueue(ctx, pool, *duration)
		case "throughput":
			benchThroughput(ctx, pool, *tasks)
		case "e2e":
			benchEndToEnd(ctx, pool, *duration)
		case "fifo":
			benchFIFO(ctx, pool, *tasks)
		case "latency":
			benchLatency(ctx, pool, *duration)
		case "timing":
			benchTiming(ctx, pool)
		case "recovery":
			benchRecovery(ctx, pool, *url)
		case "scale":
			benchScale(ctx, pool, *duration)
		case "queues":
			benchQueues(ctx, pool, *url, *duration)
		case "resources":
			benchResources(ctx, pool, *url, *duration)
		case "soak":
			benchSoak(ctx, pool, *soak)
		case "chaos":
			benchChaos(ctx, pool, *url, 60_000/size)
		case "order":
			benchOrder(ctx, pool, *url, 10_000/size)
		case "dedupe":
			benchDedupe(ctx, pool, *url, 100_000/size)
		case "restart":
			benchRestart(ctx, pool, *url, *container, 60_000/size)
		case "partition":
			benchPartition(ctx, pool, *url)
		default:
			fail(fmt.Errorf("unknown scenario %q", s))
		}
	}
	if violations > 0 {
		fail(fmt.Errorf("%d checks failed", violations))
	}
}

func connect(ctx context.Context, url string, conns int32) *pgxpool.Pool {
	return connectTraced(ctx, url, conns, &statements)
}

// connectTraced connects a pool whose statements tracer sees.
func connectTraced(ctx context.Context, url string, conns int32, tracer pgx.QueryTracer) *pgxpool.Pool {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		fail(err)
	}
	cfg.MaxConns = conns
	cfg.ConnConfig.Tracer = tracer
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
		if _, err := c.Enqueue(ctx, noop{}, nil); err != nil {
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
		if _, err := c.EnqueueMany(ctx, batch); err != nil {
			fail(err)
		}
		return len(batch)
	})
	fmt.Printf("| T2 | Enqueue in batches of 100, 8 producers | %s tasks/s |\n", rate(many, d))
	fmt.Println()
}

// timed runs fn from n goroutines for d and returns how many units they did.
// The last calls finish rather than being canceled, so that no statement of
// theirs still runs when the next scenario drops the schema.
func timed(ctx context.Context, n int, d time.Duration, fn func(context.Context) int) int64 {
	end := time.Now().Add(d)
	var total atomic.Int64
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			for time.Now().Before(end) && ctx.Err() == nil {
				total.Add(int64(fn(ctx)))
			}
		})
	}
	wg.Wait()
	return total.Load()
}

// T3, E1, and E2: processing a backlog of queued tasks, with history kept.
func benchThroughput(ctx context.Context, pool *pgxpool.Pool, n int) {
	c := fresh(ctx, pool)
	enqueueNoop(ctx, c, n)

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
	statsStart := statements.n.Load()
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
	stmts := statements.n.Load() - statsStart

	fmt.Println("## Processing")
	fmt.Println()
	fmt.Println("| ID | Workload | Result |")
	fmt.Println("|---|---|---|")
	fmt.Printf("| T3 | %s queued tasks fetched, run, and completed into history, 1 worker with concurrency 64 | %s tasks/s |\n",
		commas(int64(n)), rate(int64(n), elapsed))
	fmt.Printf("| E1 | SQL statements per task while processing | %.2f |\n", float64(stmts)/float64(n))
	fmt.Printf("| E2 | WAL written per task, fetch to history | %.2f KiB |\n", float64(wal)/float64(n)/1024)
	fmt.Println()
}

// enqueueNoop enqueues n tasks that do nothing, in batches of 1,000.
func enqueueNoop(ctx context.Context, c *taskmill.Client, n int) {
	batch := make([]taskmill.EnqueueParams, 1000)
	for i := range batch {
		batch[i] = taskmill.EnqueueParams{Args: noop{N: i}}
	}
	for done := 0; done < n; done += len(batch) {
		if _, err := c.EnqueueMany(ctx, batch[:min(len(batch), n-done)]); err != nil {
			fail(err)
		}
	}
}

// T4 and T5: FIFO groups, and the outbox relay with a batch publisher.
func benchFIFO(ctx context.Context, pool *pgxpool.Pool, n int) {
	const groups, keys = 100_000, 10_000

	c := fresh(ctx, pool)
	batch := make([]taskmill.EnqueueParams, 1000)
	for done := 0; done < n; done += len(batch) {
		for i := range batch {
			key := "account-" + strconv.Itoa((done+i)%groups)
			batch[i] = taskmill.EnqueueParams{Args: noop{N: done + i}, Options: &taskmill.EnqueueOptions{GroupKey: key}}
		}
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
	fifo := runUntil(ctx, c, &hs, taskmill.DefaultQueue, 64, func() bool { return completed.Load() >= int64(n) })

	c = fresh(ctx, pool)
	ob := outbox.New(c)
	for done := 0; done < n; done += 1000 {
		msgs := make([]outbox.Message, 1000)
		for i := range msgs {
			msgs[i] = outbox.Message{Topic: "orders", Key: strconv.AppendInt(nil, int64((done+i)%keys), 10), Value: []byte(`{}`)}
		}
		err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error { return ob.Store(ctx, taskmill.Pgx(tx), msgs...) })
		if err != nil {
			fail(err)
		}
	}
	var published atomic.Int64
	hs = taskmill.Handlers{}
	ob.AddRelay(&hs, outbox.PublisherFunc(func(_ context.Context, ds []outbox.Delivery) []error {
		published.Add(int64(len(ds)))
		return make([]error, len(ds))
	}))
	relay := runUntil(ctx, c, &hs, outbox.Queue, 8, func() bool { return published.Load() >= int64(n) })

	fmt.Println("## Ordering")
	fmt.Println()
	fmt.Println("| ID | Workload | Result |")
	fmt.Println("|---|---|---|")
	fmt.Printf("| T4 | %s tasks in %s FIFO groups, 1 worker with concurrency 64 | %s tasks/s |\n",
		commas(int64(n)), commas(groups), rate(int64(n), fifo))
	fmt.Printf("| T5 | %s outbox messages on %s keys, 1 relay worker with concurrency 8 | %s messages/s |\n",
		commas(int64(n)), commas(keys), rate(int64(n), relay))
	fmt.Println()
}

// runUntil runs a worker on queue until done reports true, and returns how
// long that took.
func runUntil(ctx context.Context, c *taskmill.Client, hs *taskmill.Handlers, queue string, concurrency int, done func() bool) time.Duration {
	w, err := taskmill.NewWorker(c, &taskmill.WorkerConfig{
		Handlers: hs,
		Queues:   map[string]taskmill.QueueOptions{queue: {Concurrency: concurrency}},
	})
	if err != nil {
		fail(err)
	}
	stop := run(ctx, w)
	start := time.Now()
	for !done() && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	elapsed := time.Since(start)
	stop()
	return elapsed
}

// run runs w until the returned function is called, which waits for w to
// stop.
func run(ctx context.Context, w *taskmill.Worker) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	return func() {
		cancel()
		if err := <-done; err != nil {
			fail(err)
		}
	}
}

// background runs fn in a goroutine, and returns a channel that is closed
// when fn returns.
func background(fn func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	return done
}

// maintain runs a worker that only maintains the schema, so that scenarios
// whose workers come and go have a maintainer that stays.
func maintain(ctx context.Context, c *taskmill.Client) (stop func()) {
	w, err := taskmill.NewWorker(c, nil)
	if err != nil {
		fail(err)
	}
	return run(ctx, w)
}

// F1 to F3: kill worker processes while they work, and check that every task
// completes exactly once and nothing is lost.
func benchChaos(ctx context.Context, pool *pgxpool.Pool, url string, n int) {
	const procs, concurrency = 4, 16
	c := fresh(ctx, pool)
	mustExec(ctx, pool, "CREATE TABLE {s}.executions (n integer NOT NULL)")
	enqueueTracked(ctx, c, n)

	spec := child{Role: "worker", Concurrency: concurrency, Lease: 3 * time.Second}
	workers := make([]*proc, procs)
	for i := range workers {
		workers[i] = spawn(ctx, url, spec)
	}
	kills := 0
	begin := time.Now()
	for history(ctx, pool) < n && ctx.Err() == nil {
		time.Sleep(time.Second)
		i := rand.IntN(procs) //nolint:gosec // picks a victim, no security role
		workers[i].kill()
		kills++
		workers[i] = spawn(ctx, url, spec)
		if time.Since(begin) > 10*time.Minute {
			fail(errors.New("chaos run did not finish in 10 minutes"))
		}
	}
	for _, w := range workers {
		w.stop()
	}

	fmt.Println("## Reliability under faults")
	fmt.Println()
	fmt.Printf("%s tasks, %d worker processes with concurrency %d, one killed with SIGKILL every second: %d kills in %s.\n\n",
		commas(int64(n)), procs, concurrency, kills, time.Since(begin).Round(time.Second))
	fmt.Println("| ID | Check | Result | |")
	fmt.Println("|---|---|---|---|")
	reportOnce(ctx, pool, "", n, kills*concurrency)
	fmt.Println()
}

// enqueueTracked enqueues n tasks that record each execution.
func enqueueTracked(ctx context.Context, c *taskmill.Client, n int) {
	batch := make([]taskmill.EnqueueParams, 1000)
	for done := 0; done < n; done += len(batch) {
		for i := range batch {
			batch[i] = taskmill.EnqueueParams{Args: tracked{N: done + i}, Options: &taskmill.EnqueueOptions{MaxAttempts: 100}}
		}
		if _, err := c.EnqueueMany(ctx, batch[:min(len(batch), n-done)]); err != nil {
			fail(err)
		}
	}
}

// reportOnce checks that n tracked tasks completed exactly once, with at most
// bound executions beyond one each, and prints the rows of the checks, which
// are F1 to F3, or else all the target id.
func reportOnce(ctx context.Context, pool *pgxpool.Pool, id string, n, bound int) {
	ids := [3]string{"F1", "F2", "F3"}
	if id != "" {
		ids = [3]string{id, id, id}
	}
	completed := scalar(ctx, pool, "SELECT count(*) FROM {s}.task_history")
	distinct := scalar(ctx, pool, "SELECT count(DISTINCT payload->>'n') FROM {s}.task_history")
	executions := scalar(ctx, pool, "SELECT count(*) FROM {s}.executions")
	leftover := scalar(ctx, pool, "SELECT count(*) FROM {s}.tasks")
	extra := executions - int64(n)
	fmt.Printf("| %s | Every task completed, none left behind | %d completed, %d left | %s |\n",
		ids[0], completed, leftover, verdict(completed == int64(n) && leftover == 0))
	fmt.Printf("| %s | Every task completed exactly once | %d distinct of %d | %s |\n",
		ids[1], distinct, completed, verdict(distinct == int64(n) && completed == int64(n)))
	fmt.Printf("| %s | Extra executions at most the attempts that were interrupted | %d extra, bound %d | %s |\n",
		ids[2], extra, bound, verdict(extra >= 0 && extra <= int64(bound)))
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

// statements counts the statements that the benchmark's pools run, for E1.
var statements statementCounter

// statementCounter is a pgx tracer that counts statements, including each
// statement of a batch.
type statementCounter struct {
	n atomic.Int64
}

func (c *statementCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	c.n.Add(1)
	return ctx
}

func (*statementCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (*statementCounter) TraceBatchStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	return ctx
}

// TraceBatchQuery counts each statement of a batch.
func (c *statementCounter) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {
	c.n.Add(1)
}

func (*statementCounter) TraceBatchEnd(context.Context, *pgx.Conn, pgx.TraceBatchEndData) {}

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

// percentiles sorts ds and returns their p50, p99, and maximum.
func percentiles(ds []time.Duration) string {
	slices.Sort(ds)
	return fmt.Sprintf("p50 %s, p99 %s, max %s", pct(ds, 50), pct(ds, 99), pct(ds, 100))
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "bench:", err)
	os.Exit(1)
}

// violations counts the checks that failed.
var violations int

// verdict reports a check's outcome, and counts a failure.
func verdict(ok bool) string {
	if ok {
		return "pass"
	}
	violations++
	return "**FAIL**"
}

// q fills in the benchmark's schema.
func q(sql string) string { return strings.ReplaceAll(sql, "{s}", schema) }

func mustExec(ctx context.Context, pool *pgxpool.Pool, sql string) {
	if _, err := pool.Exec(ctx, q(sql)); err != nil {
		fail(err)
	}
}

// scalar runs a query that returns one number.
func scalar(ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) int64 {
	var n int64
	if err := pool.QueryRow(ctx, q(sql), args...).Scan(&n); err != nil {
		fail(fmt.Errorf("%w: %s", err, sql))
	}
	return n
}

// waitFor polls cond until it holds, and fails after limit. It returns how
// long it waited.
func waitFor(ctx context.Context, limit time.Duration, what string, cond func() bool) time.Duration {
	start := time.Now()
	for !cond() {
		if ctx.Err() != nil {
			fail(ctx.Err())
		}
		if time.Since(start) > limit {
			fail(fmt.Errorf("timed out after %s waiting for %s", limit, what))
		}
		time.Sleep(20 * time.Millisecond)
	}
	return time.Since(start)
}
