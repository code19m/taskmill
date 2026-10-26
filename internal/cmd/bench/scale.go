package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
)

// T3 end to end, and E1 in steady state.
func benchEndToEnd(ctx context.Context, pool *pgxpool.Pool, d time.Duration) {
	tasks, perTask := endToEnd(ctx, fresh(ctx, pool), d)
	fmt.Println("## End to end")
	fmt.Println()
	fmt.Println("| ID | Workload | Result |")
	fmt.Println("|---|---|---|")
	fmt.Printf("| T3 | %d producers enqueueing in batches of 100 while %d workers with concurrency %d run the tasks, "+
		"history kept, for %s after a warm-up | %s tasks/s |\n", e2eProducers, e2eWorkers, e2eConcurrency, d, commas(int64(tasks)))
	fmt.Printf("| E1 | SQL statements per task in that steady state, enqueues included | %.2f |\n", perTask)
	fmt.Println()
}

// The end-to-end workload: producers and workers at once.
const e2eWorkers, e2eConcurrency, e2eProducers = 2, 64, 8

// endToEnd runs producers and workers on c, and returns the tasks a second
// that they sustain together over d, after a warm-up of 30 seconds, and the
// statements per task. Producers hold back while 20,000 tasks wait.
func endToEnd(ctx context.Context, c *taskmill.Client, d time.Duration) (tasks, perTask float64) {
	const backlog = 20_000
	var completed, enqueued atomic.Int64
	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[noop](func(context.Context, *taskmill.Task[noop]) error {
		completed.Add(1)
		return nil
	}), nil)
	var stops []func()
	for range e2eWorkers {
		w, err := taskmill.NewWorker(c, &taskmill.WorkerConfig{
			Handlers: &hs,
			Queues:   map[string]taskmill.QueueOptions{taskmill.DefaultQueue: {Concurrency: e2eConcurrency}},
		})
		if err != nil {
			fail(err)
		}
		stops = append(stops, run(ctx, w))
	}
	batch := make([]taskmill.EnqueueParams, 100)
	for i := range batch {
		batch[i] = taskmill.EnqueueParams{Args: noop{N: i}}
	}
	pctx, stopProducers := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range e2eProducers {
		wg.Go(func() {
			for pctx.Err() == nil {
				if enqueued.Load()-completed.Load() > backlog {
					time.Sleep(time.Millisecond)
					continue
				}
				if _, err := c.EnqueueMany(ctx, batch); err != nil {
					fail(err)
				}
				enqueued.Add(int64(len(batch)))
			}
		})
	}
	time.Sleep(30 * time.Second)
	done, stmts, start := completed.Load(), statements.n.Load(), time.Now()
	time.Sleep(d)
	done, stmts, elapsed := completed.Load()-done, statements.n.Load()-stmts, time.Since(start)
	stopProducers()
	wg.Wait()
	for _, stop := range stops {
		stop()
	}
	return float64(done) / elapsed.Seconds(), float64(stmts) / float64(done)
}

// S1 and S2: T3 end to end while other tasks wait: future and available
// tasks, and FIFO groups, in another queue.
func benchScale(ctx context.Context, pool *pgxpool.Pool, d time.Duration) {
	const scheduled, available, groups, groupSize = 10_000_000, 1_000_000, 100_000, 10
	base, _ := endToEnd(ctx, fresh(ctx, pool), d)
	if base == 0 {
		fail(errors.New("T3 completed no task"))
	}

	c := fresh(ctx, pool)
	later := time.Now().AddDate(1, 0, 0)
	bulk(ctx, c, scheduled, func(i int) taskmill.EnqueueParams {
		return taskmill.EnqueueParams{Args: noop{N: i}, Options: &taskmill.EnqueueOptions{RunAt: later}}
	})
	bulk(ctx, c, available, func(i int) taskmill.EnqueueParams {
		return taskmill.EnqueueParams{Args: noop{N: i}, Options: &taskmill.EnqueueOptions{Queue: "parked"}}
	})
	settle(ctx, pool)
	waiting, _ := endToEnd(ctx, c, d)

	c = fresh(ctx, pool)
	bulk(ctx, c, groups*groupSize, func(i int) taskmill.EnqueueParams {
		opts := &taskmill.EnqueueOptions{Queue: "parked", GroupKey: strconv.Itoa(i / groupSize)}
		return taskmill.EnqueueParams{Args: noop{N: i}, Options: opts}
	})
	settle(ctx, pool)
	grouped, _ := endToEnd(ctx, c, d)

	fmt.Println("## Scalability")
	fmt.Println()
	fmt.Printf("Each result is T3 end to end: %d producers and %d workers with concurrency %d, for %s after a warm-up. "+
		"The waiting tasks were vacuumed, analyzed, and checkpointed first. Alone, T3 ran %s tasks/s.\n\n",
		e2eProducers, e2eWorkers, e2eConcurrency, d, commas(int64(base)))
	fmt.Println("| ID | Workload | Result | |")
	fmt.Println("|---|---|---|---|")
	fmt.Printf("| S1 | T3 with %s scheduled and %s available tasks waiting in another queue | %s tasks/s, %.0f%% of T3 | %s |\n",
		commas(scheduled), commas(available), commas(int64(waiting)), 100*waiting/base, verdict(waiting >= 0.8*base))
	fmt.Printf("| S2 | T3 with %s FIFO groups of %d tasks waiting in another queue | %s tasks/s, %.0f%% of T3 | %s |\n",
		commas(groups), groupSize, commas(int64(grouped)), 100*grouped/base, verdict(grouped >= 0.9*base))
	fmt.Println()
}

// settle makes the waiting tasks what tasks that wait long are: vacuumed,
// analyzed, and written to disk.
func settle(ctx context.Context, pool *pgxpool.Pool) {
	mustExec(ctx, pool, "VACUUM (ANALYZE) {s}.tasks")
	mustExec(ctx, pool, "CHECKPOINT")
}

// bulk enqueues n tasks that task makes, in batches of 1,000 from 8
// producers, each of which enqueues one range of them in order.
func bulk(ctx context.Context, c *taskmill.Client, n int, task func(i int) taskmill.EnqueueParams) {
	const producers = 8
	var wg sync.WaitGroup
	for p := range producers {
		wg.Go(func() {
			end := (p + 1) * n / producers
			for from := p * n / producers; from < end; from += 1000 {
				batch := make([]taskmill.EnqueueParams, 0, 1000)
				for i := from; i < min(from+1000, end); i++ {
					batch = append(batch, task(i))
				}
				if _, err := c.EnqueueMany(ctx, batch); err != nil {
					fail(err)
				}
			}
		})
	}
	wg.Wait()
}

// S3: one maintainer for 100 queues that 50 worker processes serve, while
// tasks arrive in all of them, some of them delayed, and a worker process is
// killed every 10 seconds, leaving leases to rescue. The benchmark is the
// maintainer, and times each of its statements.
func benchQueues(ctx context.Context, pool *pgxpool.Pool, url string, d time.Duration) {
	const queues, procs, perSecond = 100, 50, 100
	d = max(d, time.Minute)
	c := fresh(ctx, pool)
	steps := &stepTracer{}
	mpool := connectTraced(ctx, url, 8, steps)
	defer mpool.Close()
	stopMaintainer := maintain(ctx, client(ctx, mpool, false))

	spec := child{Role: "worker", Queues: queues, Concurrency: 1, Lease: 5 * time.Second, NoMaintainer: true, Conns: 6}
	workers := make([]*proc, procs)
	for i := range workers {
		workers[i] = spawn(ctx, url, spec)
	}
	produced := background(func() {
		paced(ctx, d, perSecond/10, func() {
			batch := make([]taskmill.EnqueueParams, 10)
			for i := range batch {
				opts := &taskmill.EnqueueOptions{Queue: queueName(rand.IntN(queues))} //nolint:gosec // spreads tasks
				if i < 2 {
					opts.RunAt = time.Now().Add(time.Duration(1+rand.IntN(4)) * time.Second) //nolint:gosec // spreads run times
				}
				batch[i] = taskmill.EnqueueParams{Args: noop{}, Options: opts}
			}
			if _, err := c.EnqueueMany(ctx, batch); err != nil {
				fail(err)
			}
		})
	})
	kills := 0
	for end := time.Now().Add(d); time.Now().Before(end); kills++ {
		time.Sleep(10 * time.Second)
		i := rand.IntN(procs) //nolint:gosec // picks a victim, no security role
		workers[i].kill()
		workers[i] = spawn(ctx, url, spec)
	}
	<-produced
	stopMaintainer()
	for _, w := range workers {
		w.stop()
	}

	slowest := steps.slowest()
	fmt.Println("## Many queues")
	fmt.Println()
	fmt.Printf("%d queues served by %d worker processes, each with concurrency 1 per queue; %s tasks a second "+
		"enqueued into random queues, a fifth of them delayed 1 to 4 s; a worker process killed every 10 s "+
		"(%d kills), with 5 s leases. One maintainer ran for %s. Its slowest statements: %s.\n\n",
		queues, procs, commas(perSecond), kills, d, steps.summary(false))
	fmt.Println("| ID | Measure | Result | |")
	fmt.Println("|---|---|---|---|")
	fmt.Printf("| S3 | Maintainer cycle: the slowest iteration of each of its loops | %s | %s |\n",
		steps.summary(true), verdict(slowest < time.Second))
	fmt.Println()
}

// stepTracer times a pool's statements by the step of the worker or the
// maintainer that runs them, which it tells by their text, and the
// iterations of the maintainer's loops. A batch counts as one statement.
type stepTracer struct {
	mu     sync.Mutex
	steps  map[string]*stepTime
	loops  map[string]*stepTime
	partly map[string]time.Duration // the time of each loop's current iteration so far
}

// maintainerLoops lists the steps of each of the maintainer's loops, in
// order: the last one ends an iteration.
var maintainerLoops = []struct {
	name  string
	steps []string
}{
	{"main loop", []string{"election", "rescue"}},
	{"promotion loop", []string{"promotion", "next due"}},
	{"statistics loop", []string{"statistics", "storing statistics", "storing activity"}},
}

type stepTime struct {
	n       int
	longest time.Duration
}

// stepTexts name the steps by a part of their statements' text.
var stepTexts = []struct{ step, text string }{
	{"election", q("INSERT INTO {s}.leader")},
	{"rescue", "'lease_expired'"},
	{"promotion", "WITH due AS"},
	{"next due", "min(run_at) - now()"},
	{"statistics", "WITH RECURSIVE available"},
	{"storing activity", q("INSERT INTO {s}.activity AS a (minute, queue, scheduled")},
	{"storing statistics", q("{s}.queue_stats")},
	{"fetch", "WITH picked AS"},
	{"completion", "WITH done AS"},
	{"transaction", "set_config('enable_sort'"}, // sent with each fetch, in its round trip
	{"lease heartbeat", "WITH attempts AS"},
	{"worker heartbeat", "SET heartbeat_at = now()"},
	{"settings", q("FROM {s}.queues WHERE name = ANY")},
	{"activity", q("INSERT INTO {s}.activity AS a (minute, queue, completed")},
}

// stepOf returns the step that runs sql.
func stepOf(sql string) string {
	switch strings.ToLower(sql) {
	case "begin", "commit", "rollback":
		return "transaction"
	}
	for _, s := range stepTexts {
		if strings.Contains(sql, s.text) {
			return s.step
		}
	}
	return "other"
}

func (t *stepTracer) add(sql string, d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.steps == nil {
		t.steps, t.loops, t.partly = map[string]*stepTime{}, map[string]*stepTime{}, map[string]time.Duration{}
	}
	step := stepOf(sql)
	record(t.steps, step, d)
	for _, l := range maintainerLoops {
		if !slices.Contains(l.steps, step) {
			continue
		}
		t.partly[l.name] += d
		if step == l.steps[len(l.steps)-1] {
			record(t.loops, l.name, t.partly[l.name])
			t.partly[l.name] = 0
		}
	}
}

func record(times map[string]*stepTime, name string, d time.Duration) {
	s := times[name]
	if s == nil {
		s = &stepTime{}
		times[name] = s
	}
	s.n++
	s.longest = max(s.longest, d)
}

func (t *stepTracer) count(step string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	if s := t.steps[step]; s != nil {
		return s.n
	}
	return 0
}

// slowest returns the longest iteration of any of the maintainer's loops.
func (t *stepTracer) slowest() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	var d time.Duration
	for _, s := range t.loops {
		d = max(d, s.longest)
	}
	return d
}

// summary lists the maintainer's loops, or else the steps, with their counts
// and longest times.
func (t *stepTracer) summary(loops bool) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	times, unit := t.steps, "statements"
	if loops {
		times, unit = t.loops, "iterations"
	}
	names := make([]string, 0, len(times))
	for name := range times {
		names = append(names, name)
	}
	slices.Sort(names)
	parts := make([]string, len(names))
	for i, name := range names {
		s := times[name]
		parts[i] = fmt.Sprintf("%s %s (%s %s)", name, s.longest.Round(100*time.Microsecond), commas(int64(s.n)), unit)
	}
	return strings.Join(parts, ", ")
}

type stepStart struct{}

type started struct {
	sql string
	at  time.Time
}

func (*stepTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, stepStart{}, started{sql: d.SQL, at: time.Now()})
}

func (t *stepTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if s, ok := ctx.Value(stepStart{}).(started); ok {
		t.add(s.sql, time.Since(s.at))
	}
}

// batch is a batch in flight: when it started, and the statement that names
// its step.
type batch struct {
	at  time.Time
	sql string
}

func (*stepTracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	return context.WithValue(ctx, stepStart{}, &batch{at: time.Now()})
}

func (*stepTracer) TraceBatchQuery(ctx context.Context, _ *pgx.Conn, d pgx.TraceBatchQueryData) {
	if b, ok := ctx.Value(stepStart{}).(*batch); ok && stepOf(d.SQL) != "transaction" {
		b.sql = d.SQL
	}
}

// TraceBatchEnd times a batch as one statement of its step.
func (t *stepTracer) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchEndData) {
	if b, ok := ctx.Value(stepStart{}).(*batch); ok {
		t.add(b.sql, time.Since(b.at))
	}
}
