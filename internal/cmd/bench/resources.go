package main

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
)

// E4 to E7: the connections, memory, and statements that clients and workers
// use, and the database's work for heartbeats.
func benchResources(ctx context.Context, pool *pgxpool.Pool, url, container string, d time.Duration) {
	const conns, queues, running = 8, 10, 10_000
	clientConns, workerConns, listeners := connections(ctx, pool, url, d, conns)
	idleRSS, busyRSS := memory(ctx, pool, url, running)
	perTask := (busyRSS - idleRSS) * 1024 / running
	steps := idleLoad(ctx, pool, url, d, queues)
	fetches, checkIns := steps.count("fetch"), steps.count("worker check-in")
	// Each queue fetches when Run starts, when its notification connection
	// starts, and every second until the maintainer's first heartbeat
	// arrives, within 5 seconds; then once a minute.
	polls := 7 + int(d/time.Minute)
	others := 0
	for _, step := range []string{"lease renewal", "settings", "activity", "other"} {
		others += steps.count(step)
	}

	fmt.Println("## Resource use")
	fmt.Println()
	fmt.Println("| ID | Measure | Result | |")
	fmt.Println("|---|---|---|---|")
	fmt.Printf("| E4 | Connections of a client with a pool of %d, enqueueing from 16 goroutines | at most %d | %s |\n",
		conns, clientConns, verdict(clientConns <= conns))
	fmt.Printf("| E4 | Connections of a worker with a pool of %d and concurrency 64, while producers enqueue | "+
		"at most %d, %d of them listening | %s |\n", conns, workerConns, listeners, verdict(workerConns <= conns+1 && listeners == 1))
	fmt.Printf("| E5 | Worker process memory, idle | %.1f MiB RSS | %s |\n", float64(idleRSS)/1024, verdict(idleRSS <= 30*1024))
	fmt.Printf("| E5 | Worker process memory per running task, %s tasks | %.1f KiB | %s |\n",
		commas(running), float64(perTask)/1024, verdict(perTask <= 8*1024))
	fmt.Printf("| E6 | An idle worker serving %d queues for %s, the maintainer's heartbeats arriving | %d fetches (%.1f per queue), "+
		"%d worker check-ins, %d other statements | %s |\n", queues, d, fetches,
		float64(fetches)/float64(queues), checkIns, others, verdict(fetches <= queues*polls))
	const measure = "| E7 | The database's CPU for heartbeats, when installations share a database: %d connections listen"
	if container == "" {
		fmt.Printf(measure+" | Not run: set -container to the database's Docker container | |\n", fanOutListeners)
	} else {
		cpus := heartbeatCost(ctx, pool, url, container, []int{0, 20, 100})
		perBeat := time.Duration((cpus[1] - cpus[0]) / (20 * fanOutListeners) * float64(time.Second))
		fmt.Printf(measure+" | %.2f CPUs without heartbeats; %.2f more at 20 a second, which 100 installations send, "+
			"and %.2f more at 100 a second: about %s per heartbeat per listening connection | |\n",
			fanOutListeners, cpus[0], cpus[1]-cpus[0], cpus[2]-cpus[0], perBeat.Round(100*time.Nanosecond))
	}
	fmt.Println()
}

// fanOutListeners is how many connections listen in E7.
const fanOutListeners = 500

// heartbeatCost measures E7. PostgreSQL wakes every connection that listens in
// a database for each notification, whatever its channel, so installations
// that share a database wake each other's listeners with their heartbeats.
// It keeps fanOutListeners connections listening, each on a channel of its
// own, sends heartbeats on another at each rate per second for 30 seconds,
// and returns the CPUs that the database's container used at each rate.
func heartbeatCost(ctx context.Context, pool *pgxpool.Pool, url, container string, rates []int) []float64 {
	const window = 30 * time.Second
	defer listenIdle(ctx, url, fanOutListeners)()
	cpus := make([]float64, len(rates))
	for i, rate := range rates {
		before, began := containerCPU(ctx, container), time.Now()
		if rate == 0 {
			time.Sleep(window)
		} else {
			paced(ctx, window, rate, func() {
				if _, err := pool.Exec(ctx, "SELECT pg_notify('other_installation', 'h|')"); err != nil {
					fail(err)
				}
			})
		}
		cpus[i] = float64(containerCPU(ctx, container)-before) / float64(time.Since(began))
	}
	return cpus
}

// listenIdle opens n connections that each listen on a channel of their own,
// and returns a function that closes them.
func listenIdle(ctx context.Context, url string, n int) (stop func()) {
	lctx, cancel := context.WithCancel(ctx)
	var listeners sync.WaitGroup
	for i := range n {
		conn, err := pgx.Connect(ctx, url)
		if err != nil {
			fail(err)
		}
		if _, err := conn.Exec(ctx, "LISTEN installation_"+strconv.Itoa(i)); err != nil {
			fail(err)
		}
		listeners.Go(func() {
			defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
			for lctx.Err() == nil {
				_, _ = conn.WaitForNotification(lctx)
			}
		})
	}
	return func() {
		cancel()
		listeners.Wait()
	}
}

// containerCPU returns the CPU time that a Docker container used so far, from
// its cgroup.
func containerCPU(ctx context.Context, container string) time.Duration {
	out, err := exec.CommandContext(ctx, "docker", "exec", container, "cat", "/sys/fs/cgroup/cpu.stat").Output() //nolint:gosec // the operator's container
	if err != nil {
		fail(fmt.Errorf("read the database's CPU use: %w", err))
	}
	for line := range strings.Lines(string(out)) {
		if v, ok := strings.CutPrefix(line, "usage_usec "); ok {
			us, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			if err != nil {
				fail(err)
			}
			return time.Duration(us) * time.Microsecond
		}
	}
	fail(errors.New("the database's container shows no usage_usec in cpu.stat"))
	return 0
}

// connections returns the most connections that a client, and then a client
// with a worker, held at once, each with a pool of conns, and the most of the
// worker's connections that listened.
func connections(ctx context.Context, pool *pgxpool.Pool, url string, d time.Duration, conns int32) (client, worker, listening int64) {
	fresh(ctx, pool)
	const app = "taskmill-bench-e4"
	sep := "?"
	if strings.Contains(url, "?") {
		sep = "&"
	}
	own := connect(ctx, url+sep+"application_name="+app, conns)
	defer own.Close()
	c, err := taskmill.NewClient(ctx, own, &taskmill.ClientConfig{Schema: schema})
	if err != nil {
		fail(err)
	}
	sample := func(sql string) func() int64 {
		var most int64
		var mu sync.Mutex
		sctx, stop := context.WithCancel(ctx)
		var wg sync.WaitGroup
		wg.Go(func() {
			for sctx.Err() == nil {
				n := scalar(ctx, pool, sql, app)
				mu.Lock()
				most = max(most, n)
				mu.Unlock()
				time.Sleep(10 * time.Millisecond)
			}
		})
		return func() int64 {
			stop()
			wg.Wait()
			return most
		}
	}
	enqueue := func(ctx context.Context) int {
		if _, err := c.Enqueue(ctx, noop{}, nil); err != nil {
			fail(err)
		}
		return 1
	}
	all := "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1"

	done := sample(all)
	timed(ctx, 16, d, enqueue)
	client = done()

	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[noop](func(context.Context, *taskmill.Task[noop]) error { return nil }), nil)
	w, err := taskmill.NewWorker(c, &taskmill.WorkerConfig{
		Handlers: &hs,
		Queues:   map[string]taskmill.QueueOptions{taskmill.DefaultQueue: {Concurrency: 64}},
	})
	if err != nil {
		fail(err)
	}
	stop := run(ctx, w)
	time.Sleep(time.Second) // let the worker connect
	done = sample(all)
	listeners := sample(all + " AND query LIKE 'LISTEN %'")
	timed(ctx, 16, d, enqueue)
	worker, listening = done(), listeners()
	stop()
	return client, worker, listening
}

// memory starts a worker process with concurrency n, and returns its resident
// memory in KiB idle, and then while it runs n tasks.
func memory(ctx context.Context, pool *pgxpool.Pool, url string, n int) (idle, busy int64) {
	c := fresh(ctx, pool)
	mustExec(ctx, pool, `CREATE TABLE {s}.holds (n integer NOT NULL, worker text NOT NULL, attempt integer NOT NULL,
    phase text NOT NULL, at timestamptz NOT NULL DEFAULT clock_timestamp())`)
	w := spawn(ctx, url, child{Role: "worker", Concurrency: n, Conns: 16})
	defer w.kill()
	rss := func() int64 {
		var most int64
		for range 5 {
			time.Sleep(time.Second)
			out, err := exec.CommandContext(ctx, "ps", "-o", "rss=", "-p", strconv.Itoa(w.cmd.Process.Pid)).Output() //nolint:gosec // the child's ID
			if err != nil {
				fail(err)
			}
			kib, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
			if err != nil {
				fail(err)
			}
			most = max(most, kib)
		}
		return most
	}
	time.Sleep(5 * time.Second) // let the worker start and settle
	idle = rss()
	batch := make([]taskmill.EnqueueParams, 0, 1000)
	for i := range n {
		batch = append(batch, taskmill.EnqueueParams{Args: hold{N: i, Wave: 1}})
		if len(batch) == cap(batch) || i == n-1 {
			if _, err := c.EnqueueMany(ctx, batch); err != nil {
				fail(err)
			}
			batch = batch[:0]
		}
	}
	waitFor(ctx, time.Minute, "every task to start", func() bool {
		return scalar(ctx, pool, "SELECT count(*) FROM {s}.holds") == int64(n)
	})
	time.Sleep(10 * time.Second) // check-ins renew the leases
	return idle, rss()
}

// idleLoad runs a worker that serves queues, none of which has tasks, for d,
// and returns its statements by step. It stays out of the maintainer
// election, whose work is S3's.
func idleLoad(ctx context.Context, pool *pgxpool.Pool, url string, d time.Duration, queues int) *stepTracer {
	fresh(ctx, pool)
	steps := &stepTracer{}
	own := connectTraced(ctx, url, 8, steps)
	defer own.Close()
	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[noop](func(context.Context, *taskmill.Task[noop]) error { return nil }), nil)
	qs := map[string]taskmill.QueueOptions{}
	for i := range queues {
		qs[queueName(i)] = taskmill.QueueOptions{}
	}
	w, err := taskmill.NewWorker(client(ctx, own, false), &taskmill.WorkerConfig{Handlers: &hs, Queues: qs, NoMaintainer: true})
	if err != nil {
		fail(err)
	}
	// The maintainer runs apart, on an untraced pool.
	stopMaintainer := maintain(ctx, client(ctx, pool, false))
	stop := run(ctx, w)
	time.Sleep(d)
	stop()
	stopMaintainer()
	return steps
}

// E3: the size of the tasks table and its indexes while a worker runs tasks
// at half of T3's target for d. Completed tasks are discarded, so the disk
// holds the tasks table only; the table sees the same writes either way.
func benchSoak(ctx context.Context, pool *pgxpool.Pool, d time.Duration) {
	c := fresh(ctx, pool)
	var completed, enqueued atomic.Int64
	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[noop](func(context.Context, *taskmill.Task[noop]) error {
		completed.Add(1)
		return nil
	}), &taskmill.HandlerOptions{DiscardCompleted: true})
	w, err := taskmill.NewWorker(c, &taskmill.WorkerConfig{
		Handlers: &hs,
		Queues:   map[string]taskmill.QueueOptions{taskmill.DefaultQueue: {Concurrency: 64}},
	})
	if err != nil {
		fail(err)
	}
	stop := run(ctx, w)
	batch := make([]taskmill.EnqueueParams, 100)
	for i := range batch {
		batch[i] = taskmill.EnqueueParams{Args: noop{N: i}}
	}
	produced := background(func() {
		paced(ctx, d, loadRate/len(batch), func() {
			if _, err := c.EnqueueMany(ctx, batch); err != nil {
				fail(err)
			}
			enqueued.Add(int64(len(batch)))
		})
	})

	fmt.Println("## Soak")
	fmt.Println()
	fmt.Printf("%s tasks/s enqueued in batches of 100 for %s, 1 worker with concurrency 64, completed tasks discarded.\n\n",
		commas(loadRate), d)
	fmt.Println("| Time | Enqueued | Completed | Waiting | `tasks` heap | `tasks` indexes |")
	fmt.Println("|---|---|---|---|---|---|")
	const samples = 12
	var reference, most int64
	for i := 1; i <= samples; i++ {
		select {
		case <-ctx.Done():
			fail(ctx.Err())
		case <-time.After(d / samples):
		}
		var heap, index int64
		err := pool.QueryRow(ctx, "SELECT pg_table_size($1), pg_indexes_size($1)", schema+".tasks").Scan(&heap, &index)
		if err != nil {
			fail(err)
		}
		if i == samples/4 {
			reference = heap + index
		}
		if i > samples/4 {
			most = max(most, heap+index)
		}
		fmt.Printf("| %s | %s | %s | %s | %s | %s |\n", (d * time.Duration(i) / samples).Round(time.Second),
			commas(enqueued.Load()), commas(completed.Load()), commas(enqueued.Load()-completed.Load()), mib(heap), mib(index))
	}
	<-produced
	stop()
	fmt.Println()
	fmt.Println("| ID | Measure | Result | |")
	fmt.Println("|---|---|---|---|")
	fmt.Printf("| E3 | `tasks` heap and indexes after %s, against their size after a quarter of it | "+
		"at most %s, %.2f× | %s |\n", d, mib(most), float64(most)/float64(reference), verdict(most <= 2*reference))
	fmt.Println()
}

func mib(bytes int64) string { return fmt.Sprintf("%.1f MiB", float64(bytes)/(1<<20)) }
