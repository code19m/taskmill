package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
)

// timedTask records when each of its attempts started, measured against its
// run time, both on the database's clock. It fails its first Fails attempts,
// snoozes once if Snooze is set, and runs for Hold.
type timedTask struct {
	N      int           `json:"n"`
	Fails  int           `json:"fails,omitempty"`
	Snooze bool          `json:"snooze,omitempty"`
	Hold   time.Duration `json:"hold,omitempty"`
}

func (timedTask) Kind() string { return "bench.timed" }

// timingRun is how long the timing scenario runs: long enough for its
// schedule, which runs every minute, to run at least once.
const timingRun = 75 * time.Second

// L4 and F9: tasks start on time by the database's clock: delayed tasks,
// retries, snoozes, and schedule runs. Leases last 3 seconds, and some tasks
// run for longer, renewing theirs. Against a database whose clock is off from
// this machine's by minutes, as docs/benchmarks.md shows how to start, the
// same checks are F9: application servers' clocks out of sync. wantSkew is
// the offset the run expects, if any: a database whose clock is not off by
// it fails F9 rather than skipping it.
func benchTiming(ctx context.Context, pool *pgxpool.Pool, wantSkew time.Duration) {
	c := fresh(ctx, pool)
	mustExec(ctx, pool, `CREATE TABLE {s}.starts (n integer NOT NULL, attempt integer NOT NULL,
    early boolean NOT NULL, late_us bigint NOT NULL)`)
	var dbNow time.Time
	sent := time.Now()
	if err := pool.QueryRow(ctx, "SELECT now()").Scan(&dbNow); err != nil {
		fail(err)
	}
	skew := dbNow.Sub(sent.Add(time.Since(sent) / 2)).Round(time.Second)

	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[timedTask](recordStart(pool)),
		&taskmill.HandlerOptions{Retry: taskmill.ExponentialBackoff{Base: 2 * time.Second, Max: 2 * time.Second}})

	const lease = 3 * time.Second
	schedule := taskmill.Schedule{Name: "every-minute", Cron: "* * * * *", Args: timedTask{N: -1}}
	var stops []func()
	for range 2 {
		w, err := taskmill.NewWorker(c, &taskmill.WorkerConfig{
			Handlers:      &hs,
			Queues:        map[string]taskmill.QueueOptions{taskmill.DefaultQueue: {Concurrency: 128}},
			LeaseDuration: lease,
			Schedules:     []taskmill.Schedule{schedule},
		})
		if err != nil {
			fail(err)
		}
		stops = append(stops, run(ctx, w))
	}

	batch := timingTasks()
	if _, err := c.EnqueueMany(ctx, batch); err != nil {
		fail(err)
	}
	time.Sleep(timingRun)
	var stopping time.Time
	if err := pool.QueryRow(ctx, "SELECT now()").Scan(&stopping); err != nil {
		fail(err)
	}
	for _, stop := range stops {
		stop()
	}

	lates := lateness(ctx, pool)
	early := scalar(ctx, pool, "SELECT count(*) FROM {s}.starts WHERE early")
	runs := scalar(ctx, pool, "SELECT count(*) FROM {s}.task_history WHERE (payload->>'n')::int = -1")
	scheduled := scalar(ctx, pool, "SELECT count(DISTINCT run_at) FROM {s}.task_history WHERE (payload->>'n')::int = -1")
	// A run time that this machine's clock puts in the database's future
	// waits, however soon this machine thinks it is.
	waiting := scalar(ctx, pool, "SELECT count(*) FROM {s}.tasks WHERE state = 'scheduled' AND run_at > now()")
	// A schedule run that came due while the workers stopped waits for the
	// next worker: the maintainer leads until its worker's tasks end, and
	// enqueues it, but the worker no longer fetches.
	dueAtStop := scalar(ctx, pool, "SELECT count(*) FROM {s}.tasks WHERE (payload->>'n')::int = -1 AND created_at >= $1", stopping)
	unfinished := scalar(ctx, pool, "SELECT count(*) FROM {s}.tasks") - waiting - dueAtStop
	completed := scalar(ctx, pool, "SELECT count(*) FROM {s}.task_history WHERE (payload->>'n')::int >= 0")
	lost := scalar(ctx, pool, `
SELECT (SELECT count(*) FROM {s}.task_history WHERE errors @> '[{"code": "lease_expired"}]')
     + (SELECT count(*) FROM {s}.tasks WHERE errors @> '[{"code": "lease_expired"}]')`)
	late := percentiles(lates)
	clock := skew.String() + " ahead of"
	if skew < 0 {
		clock = (-skew).String() + " behind"
	}

	fmt.Println("## Timing")
	fmt.Println()
	fmt.Printf("The database's clock is %s this machine's. 2,100 tasks: 1,000 delayed up to 20 s by this "+
		"machine's clock, 500 failing twice with a 2 s backoff, 500 snoozing once for 2 s, and 100 running for 8 s "+
		"with 3 s leases; and a schedule that runs every minute. 2 workers with concurrency 128 ran for %s.\n\n",
		clock, timingRun)
	fmt.Println("| ID | Check | Result | |")
	fmt.Println("|---|---|---|---|")
	fmt.Printf("| L4 | Start after the run time, %s starts | %s | %s |\n",
		commas(int64(len(lates))), late, verdict(pct(lates, 99) <= 1500*time.Millisecond))
	fmt.Printf("| L4 | Every task completed once, or waits for a run time still ahead; each schedule run enqueued once | "+
		"%s of 2,100 completed, %s waiting, %d left; %d schedule runs at %d times, and %d due while the workers stopped | %s |\n",
		commas(completed), commas(waiting), unfinished, runs, scheduled, dueAtStop,
		verdict(completed+waiting == 2100 && unfinished == 0 && runs > 0 && runs == scheduled))
	switch {
	case wantSkew != 0 && (skew-wantSkew).Abs() > 30*time.Second:
		fmt.Printf("| F9 | Clocks out of sync | The database's clock is %s off, not the expected %s | %s |\n",
			skew, wantSkew, verdict(false))
	case skew.Abs() < time.Minute:
		fmt.Println("| F9 | Clocks out of sync | Not run: the database's clock is within a minute of this machine's | |")
	default:
		fmt.Printf("| F9 | Clocks %s apart: no early start, none late beyond L4, no lease lost while renewed | "+
			"%d early, p99 %s late, %d leases lost | %s |\n", skew.Abs(), early, pct(lates, 99), lost,
			verdict(early == 0 && pct(lates, 99) <= 1500*time.Millisecond && lost == 0))
	}
	fmt.Println()
}

// recordStart returns the handler of timedTask, which records each start.
func recordStart(pool *pgxpool.Pool) func(context.Context, *taskmill.Task[timedTask]) error {
	return func(ctx context.Context, t *taskmill.Task[timedTask]) error {
		// A delayed task enqueued with a run time already past on the
		// database's clock is due from its creation.
		due := t.RunAt
		if t.CreatedAt.After(due) {
			due = t.CreatedAt
		}
		_, err := pool.Exec(ctx, q("INSERT INTO {s}.starts VALUES ($1, $2, $3, $4)"),
			t.Args.N, t.Attempt, t.AttemptedAt.Before(t.RunAt), t.AttemptedAt.Sub(due).Microseconds())
		switch {
		case err != nil:
			return err
		case t.Attempt <= t.Args.Fails:
			return errors.New("failed on purpose")
		case t.Args.Snooze && t.AttemptedAt.Sub(t.CreatedAt) < time.Second:
			return taskmill.Snooze(2 * time.Second)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(t.Args.Hold):
			return nil
		}
	}
}

// timingTasks returns 2,100 tasks: delayed by this machine's clock, failing
// twice, snoozing once, and running for over two leases.
func timingTasks() []taskmill.EnqueueParams {
	batch := make([]taskmill.EnqueueParams, 2100)
	for n := range batch {
		args, opts := timedTask{N: n}, &taskmill.EnqueueOptions{MaxAttempts: 5}
		switch {
		case n < 1000:
			opts.RunAt = time.Now().Add(time.Duration(rand.Int64N(int64(20 * time.Second)))) //nolint:gosec // spreads run times
		case n < 1500:
			args.Fails = 2
		case n < 2000:
			args.Snooze = true
		default:
			args.Hold = 8 * time.Second
		}
		batch[n] = taskmill.EnqueueParams{Args: args, Options: opts}
	}
	return batch
}

// lateness returns how late each recorded start was.
func lateness(ctx context.Context, pool *pgxpool.Pool) []time.Duration {
	r, err := pool.Query(ctx, q("SELECT late_us FROM {s}.starts"))
	if err != nil {
		fail(err)
	}
	lates, err := pgx.CollectRows(r, func(row pgx.CollectableRow) (time.Duration, error) {
		var us int64
		err := row.Scan(&us)
		return time.Duration(us) * time.Microsecond, err
	})
	if err != nil {
		fail(err)
	}
	return lates
}
