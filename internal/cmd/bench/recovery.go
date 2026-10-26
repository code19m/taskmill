package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
)

// stall runs for a minute and ignores its context.
type stall struct{}

func (stall) Kind() string { return "bench.stall" }

// L5 and L6: how soon a killed worker's tasks run again, and how soon a
// stopping worker returns when its handlers ignore their contexts.
func benchRecovery(ctx context.Context, pool *pgxpool.Pool, url string) {
	const tasks, lease, drain = 20, 3 * time.Second, 2 * time.Second
	rerun := rerunAfterKill(ctx, pool, url, tasks, lease)
	stopped := stopTime(ctx, pool, drain)

	fmt.Println("## Recovery")
	fmt.Println()
	fmt.Println("| ID | Measure | Result | |")
	fmt.Println("|---|---|---|---|")
	rerunBound, stopBound := lease+2500*time.Millisecond, drain+6*time.Second
	fmt.Printf("| L5 | A killed worker's %d running tasks running again on another worker, %s lease | "+
		"the last after %s, bound %s | %s |\n",
		tasks, lease, rerun.Round(10*time.Millisecond), rerunBound, verdict(rerun <= rerunBound))
	fmt.Printf("| L6 | `Worker.Run` returning after its context is canceled, handlers ignoring theirs, %s drain timeout | "+
		"%s, bound %s | %s |\n",
		drain, stopped.Round(10*time.Millisecond), stopBound, verdict(stopped <= stopBound))
	fmt.Println()
}

// rerunAfterKill kills a worker process that runs n tasks, and returns how
// long after the kill the last of them starts on another worker, by the
// database's clock. The benchmark is the maintainer, so the kill measures
// lease expiry and rescue without a new election.
func rerunAfterKill(ctx context.Context, pool *pgxpool.Pool, url string, n int, lease time.Duration) time.Duration {
	c := fresh(ctx, pool)
	mustExec(ctx, pool, `CREATE TABLE {s}.holds (n integer NOT NULL, worker text NOT NULL, attempt integer NOT NULL,
    phase text NOT NULL, at timestamptz NOT NULL DEFAULT clock_timestamp())`)
	stopMaintainer := maintain(ctx, c)
	defer stopMaintainer()
	spec := child{Role: "worker", Name: "killed", Concurrency: n, Lease: lease, NoMaintainer: true}
	killed := spawn(ctx, url, spec)
	batch := make([]taskmill.EnqueueParams, n)
	for i := range batch {
		batch[i] = taskmill.EnqueueParams{Args: hold{N: i, Wave: 1}}
	}
	if _, err := c.EnqueueMany(ctx, batch); err != nil {
		fail(err)
	}
	started := func(worker string) func() bool {
		return func() bool {
			return scalar(ctx, pool, "SELECT count(*) FROM {s}.holds WHERE worker = $1 AND phase = 'start'", worker) == int64(n)
		}
	}
	waitFor(ctx, 30*time.Second, "the worker to start every task", started("killed"))
	killed.kill()
	var at time.Time
	if err := pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&at); err != nil {
		fail(err)
	}
	spec.Name = "next"
	next := spawn(ctx, url, spec)
	waitFor(ctx, time.Minute, "another worker to run every task", started("next"))
	var last time.Time
	if err := pool.QueryRow(ctx, q("SELECT max(at) FROM {s}.holds WHERE worker = 'next'")).Scan(&last); err != nil {
		fail(err)
	}
	next.release(1)
	waitFor(ctx, 30*time.Second, "the tasks to complete", func() bool { return history(ctx, pool) == n })
	next.stop()
	return last.Sub(at)
}

// stopTime returns how long Run takes to return once its context is canceled,
// while its handlers ignore theirs. The handlers go on in the background, as
// they would in a process that keeps running.
func stopTime(ctx context.Context, pool *pgxpool.Pool, drain time.Duration) time.Duration {
	const n = 10
	c := fresh(ctx, pool)
	started := make(chan struct{}, n)
	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[stall](func(context.Context, *taskmill.Task[stall]) error {
		started <- struct{}{}
		time.Sleep(time.Minute)
		return nil
	}), nil)
	w, err := taskmill.NewWorker(c, &taskmill.WorkerConfig{Handlers: &hs, DrainTimeout: drain})
	if err != nil {
		fail(err)
	}
	batch := make([]taskmill.EnqueueParams, n)
	for i := range batch {
		batch[i] = taskmill.EnqueueParams{Args: stall{}}
	}
	if _, err := c.EnqueueMany(ctx, batch); err != nil {
		fail(err)
	}
	stop := run(ctx, w)
	for range n {
		<-started
	}
	begin := time.Now()
	stop()
	return time.Since(begin)
}
