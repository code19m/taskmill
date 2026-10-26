package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os/exec"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
)

// F4: FIFO groups keep their order while attempts fail and worker processes
// are killed, and halt at a dead head. Each handler call is recorded on the
// database's clock, and each check compares it with when the tasks of its
// group completed.
func benchOrder(ctx context.Context, pool *pgxpool.Pool, url string, groups int) {
	const size, procs, concurrency = 100, 4, 16
	c := fresh(ctx, pool)
	mustExec(ctx, pool, `CREATE TABLE {s}.steps (g integer NOT NULL, s integer NOT NULL, ok boolean NOT NULL,
    at timestamptz NOT NULL DEFAULT clock_timestamp())`)
	// One transaction at a time, so each group's order is its enqueue order.
	batch := make([]taskmill.EnqueueParams, 0, 1000)
	for s := 1; s <= size; s++ {
		for g := range groups {
			opts := &taskmill.EnqueueOptions{GroupKey: strconv.Itoa(g), MaxAttempts: 3}
			batch = append(batch, taskmill.EnqueueParams{Args: step{G: g, S: s}, Options: opts})
			if len(batch) == cap(batch) || s == size && g == groups-1 {
				if _, err := c.EnqueueMany(ctx, batch); err != nil {
					fail(err)
				}
				batch = batch[:0]
			}
		}
	}

	spec := child{Role: "worker", Concurrency: concurrency, Lease: 3 * time.Second}
	workers := make([]*proc, procs)
	for i := range workers {
		workers[i] = spawn(ctx, url, spec)
	}
	kills := 0
	begin := time.Now()
	// Done when every task completed, or waits behind a dead head.
	for scalar(ctx, pool, "SELECT count(*) FROM {s}.tasks WHERE state NOT IN ('pending', 'dead')") > 0 {
		time.Sleep(2 * time.Second)
		i := rand.IntN(procs) //nolint:gosec // picks a victim, no security role
		workers[i].kill()
		kills++
		workers[i] = spawn(ctx, url, spec)
		if time.Since(begin) > 15*time.Minute {
			fail(errors.New("order run did not finish in 15 minutes"))
		}
	}
	took := time.Since(begin)
	for _, w := range workers {
		w.stop()
	}

	mustExec(ctx, pool, `
CREATE TABLE {s}.done AS SELECT (payload->>'g')::int AS g, (payload->>'s')::int AS s, finalized_at AS at FROM {s}.task_history;
CREATE TABLE {s}.dead AS SELECT (payload->>'g')::int AS g, (payload->>'s')::int AS s, id FROM {s}.tasks WHERE state = 'dead';
CREATE INDEX ON {s}.done (g, s);
CREATE INDEX ON {s}.steps (g, s)`)
	// A call that started before the task ahead of it completed, or after
	// its own task completed.
	early := scalar(ctx, pool, `
SELECT count(*) FROM {s}.steps x LEFT JOIN {s}.done p ON p.g = x.g AND p.s = x.s - 1
WHERE x.s > 1 AND (p.at IS NULL OR x.at <= p.at)`)
	again := scalar(ctx, pool, `SELECT count(*) FROM {s}.steps x JOIN {s}.done d USING (g, s) WHERE x.at >= d.at`)
	pastDead := scalar(ctx, pool, `SELECT count(*) FROM {s}.steps x JOIN {s}.dead d ON d.g = x.g AND x.s > d.s`)
	// Every task, each one checked, completed once, is a dead head that
	// halts its group, or waits behind one.
	done := scalar(ctx, pool, "SELECT count(*) FROM {s}.done")
	twice := done - scalar(ctx, pool, "SELECT count(*) FROM (SELECT DISTINCT g, s FROM {s}.done) d")
	halted := scalar(ctx, pool, "SELECT count(*) FROM {s}.dead d JOIN {s}.groups g ON g.halted_task_id = d.id")
	waiting := scalar(ctx, pool, `
SELECT count(*) FROM {s}.tasks t JOIN {s}.dead d ON d.g = (t.payload->>'g')::int AND d.s < (t.payload->>'s')::int
WHERE t.state = 'pending'`)
	// The distinct tasks that are accounted for, each by its group and
	// place: completed, a dead head that halts its group, or waiting behind
	// one. Any task missing from them is lost.
	lost := int64(groups*size) - scalar(ctx, pool, `
SELECT count(*) FROM (
    SELECT g, s FROM {s}.done
    UNION
    SELECT d.g, d.s FROM {s}.dead d JOIN {s}.groups h ON h.halted_task_id = d.id
    UNION
    SELECT w.g, w.s FROM (SELECT (payload->>'g')::int AS g, (payload->>'s')::int AS s FROM {s}.tasks WHERE state = 'pending') w
    JOIN {s}.dead d ON d.g = w.g AND d.s < w.s
) a WHERE g BETWEEN 0 AND $1 - 1 AND s BETWEEN 1 AND $2`, groups, size)
	broken := brokenGroups(ctx, pool)
	failed := scalar(ctx, pool, "SELECT count(*) FROM {s}.steps WHERE NOT ok")

	fmt.Println("## FIFO order under faults")
	fmt.Println()
	fmt.Printf("%s tasks in %s FIFO groups of %d, at most 3 attempts each, %d worker processes with concurrency %d. "+
		"%d%% of attempts failed (%s), and a process was killed with SIGKILL every 2 seconds (%d kills). "+
		"%s groups halted at a dead head. It took %s.\n\n",
		commas(int64(groups*size)), commas(int64(groups)), size, procs, concurrency, stepFailures, commas(failed),
		kills, commas(halted), took.Round(time.Second))
	fmt.Println("| ID | Check | Result | |")
	fmt.Println("|---|---|---|---|")
	fmt.Printf("| F4 | No handler sees a task before the one ahead of it completes, or after its own | "+
		"%d early, %d after completing | %s |\n", early, again, verdict(early == 0 && again == 0))
	fmt.Printf("| F4 | Nothing after a dead head runs; every task completes once or waits behind one | "+
		"%d runs past a dead head; %s completed, %d twice, %s waiting, %d lost; %d broken groups | %s |\n",
		pastDead, commas(done), twice, commas(waiting), lost, broken,
		verdict(pastDead == 0 && twice == 0 && lost == 0 && broken == 0))
	fmt.Println()
}

// F5: producer processes enqueue the same dedupe keys at the same time, and
// exactly one task is created per key. Every fifth key also joins a FIFO
// group, whose counts must stay exact.
func benchDedupe(ctx context.Context, pool *pgxpool.Pool, url string, keys int) {
	const procs = 20
	fresh(ctx, pool)
	mustExec(ctx, pool, "CREATE TABLE {s}.results (key integer NOT NULL, task_id bigint NOT NULL, duplicate boolean NOT NULL)")
	begin := time.Now()
	producers := make([]*proc, procs)
	for i := range producers {
		producers[i] = spawn(ctx, url, child{Role: "producer", Keys: keys, Parity: i % 2, Conns: 4})
	}
	for _, p := range producers {
		p.wait()
	}
	took := time.Since(begin)

	tasks := scalar(ctx, pool, "SELECT count(*) FROM {s}.tasks")
	distinct := scalar(ctx, pool, "SELECT count(DISTINCT payload->>'key') FROM {s}.tasks")
	enqueues := scalar(ctx, pool, "SELECT count(*) FROM {s}.results")
	admitted := scalar(ctx, pool, "SELECT count(DISTINCT key) FROM {s}.results WHERE NOT duplicate")
	created := scalar(ctx, pool, "SELECT count(*) FROM {s}.results WHERE NOT duplicate")
	// Every result, duplicate or not, names the one task of its key.
	wrong := scalar(ctx, pool, `
SELECT count(*) FROM {s}.results r LEFT JOIN {s}.tasks t ON (t.payload->>'key')::int = r.key
WHERE t.id IS DISTINCT FROM r.task_id`)
	broken := brokenGroups(ctx, pool)

	fmt.Println("## Deduplication under contention")
	fmt.Println()
	fmt.Printf("%d producer processes enqueued %s tasks with %s dedupe keys at once, each key 10 times, "+
		"in batches of 100, in %s.\n\n", procs, commas(enqueues), commas(int64(keys)), took.Round(time.Millisecond))
	fmt.Println("| ID | Check | Result | |")
	fmt.Println("|---|---|---|---|")
	fmt.Printf("| F5 | Exactly one task per key | %s tasks, %s distinct keys | %s |\n",
		commas(tasks), commas(distinct), verdict(tasks == int64(keys) && distinct == int64(keys)))
	fmt.Printf("| F5 | Exactly one enqueue per key reports a new task; every result names the key's task | "+
		"%s new of %s keys, %d wrong results | %s |\n",
		commas(created), commas(admitted), wrong, verdict(created == int64(keys) && admitted == int64(keys) && wrong == 0))
	fmt.Printf("| F5 | FIFO groups count exactly their tasks, with one head each | %d broken groups | %s |\n",
		broken, verdict(broken == 0))
	fmt.Println()
}

// brokenGroups counts the FIFO groups that break the invariants in
// docs/design/task-lifecycle.md, and the grouped tasks without a group.
func brokenGroups(ctx context.Context, pool *pgxpool.Pool) int64 {
	return scalar(ctx, pool, `
WITH members AS (
    SELECT g.tasks, g.halted_task_id,
           count(t.id) FILTER (WHERE t.state IN ('scheduled', 'available', 'running', 'retryable')) AS heads,
           count(t.id) FILTER (WHERE t.state = 'pending') AS pending,
           count(t.id) FILTER (WHERE t.id = g.halted_task_id AND t.state = 'dead') AS halting
    FROM {s}.groups g LEFT JOIN {s}.tasks t ON t.queue = g.queue AND t.group_key = g.group_key
    GROUP BY g.queue, g.group_key, g.tasks, g.halted_task_id
)
SELECT (SELECT count(*) FROM members
        WHERE tasks <> heads + pending + halting OR heads + halting <> 1 OR (halted_task_id IS NULL) <> (halting = 0))
     + (SELECT count(*) FROM {s}.tasks t
        WHERE t.group_key IS NOT NULL AND NOT t.state IN ('dead', 'canceled')
          AND NOT EXISTS (SELECT FROM {s}.groups g WHERE g.queue = t.queue AND g.group_key = t.group_key))`)
}

// F6: the database restarts twice during a run, once shutting down cleanly
// and once crashing, and workers resume each time without help.
func benchRestart(ctx context.Context, pool *pgxpool.Pool, url, container string, n int) {
	fmt.Println("## Database restarts")
	fmt.Println()
	if container == "" {
		fmt.Println("Not run: set -container to the database's Docker container.")
		fmt.Println()
		return
	}
	const procs, concurrency = 4, 16
	c := fresh(ctx, pool)
	mustExec(ctx, pool, "CREATE TABLE {s}.executions (n integer NOT NULL)")
	enqueueTracked(ctx, c, n)
	workers := make([]*proc, procs)
	for i := range workers {
		workers[i] = spawn(ctx, url, child{Role: "worker", Concurrency: concurrency})
	}

	docker := func(args ...string) {
		if out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil { //nolint:gosec // the operator's container
			fail(fmt.Errorf("docker %v: %w: %s", args, err, out))
		}
	}
	// The crashed database stays down for longer than the maintainer's lease.
	restarts := []struct {
		name    string
		restart func()
	}{
		{"shut down cleanly", func() { docker("restart", container) }},
		{"crashed and down for 15 s", func() {
			docker("kill", "--signal", "KILL", container)
			time.Sleep(15 * time.Second)
			docker("start", container)
		}},
	}
	var rows []string
	resumedInTime := true
	for i, r := range restarts {
		waitFor(ctx, 5*time.Minute, "progress", func() bool { return history(ctx, pool) >= n*(i+1)/3 })
		begin := time.Now()
		r.restart()
		waitFor(ctx, time.Minute, "the database", func() bool { return pool.Ping(ctx) == nil })
		down := time.Since(begin)
		// Workers resumed once a task that they fetched after the restart
		// completed; completions of earlier attempts do not count.
		var back time.Time
		if err := pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&back); err != nil {
			fail(err)
		}
		resumed := waitFor(ctx, time.Minute, "workers to resume", func() bool {
			var fetched bool
			err := pool.QueryRow(ctx, q("SELECT EXISTS (SELECT FROM {s}.task_history WHERE attempted_at > $1)"), back).Scan(&fetched)
			return err == nil && fetched
		})
		resumedInTime = resumedInTime && resumed <= 10*time.Second
		rows = append(rows, fmt.Sprintf("%s: back after %s, workers resumed %s later",
			r.name, down.Round(100*time.Millisecond), resumed.Round(10*time.Millisecond)))
	}
	waitFor(ctx, 5*time.Minute, "every task", func() bool { return history(ctx, pool) >= n })
	for _, w := range workers {
		w.stop()
	}

	fmt.Printf("%s tasks, %d worker processes with concurrency %d. PostgreSQL restarted twice: %s; %s.\n\n",
		commas(int64(n)), procs, concurrency, rows[0], rows[1])
	fmt.Println("| ID | Check | Result | |")
	fmt.Println("|---|---|---|---|")
	fmt.Printf("| F6 | Workers resume within 10 s of the database | %s; %s | %s |\n", rows[0], rows[1], verdict(resumedInTime))
	reportOnce(ctx, pool, "F6", n, len(restarts)*procs*concurrency)
	fmt.Println()
}

// F7: a worker is cut off from the database for longer than its lease, while
// its handlers hold tasks. Another worker takes them over, and the cut
// worker's results, sent during the cut, arrive late and must be rejected.
// Half of its handlers return during the cut, so their completions wait in
// the network; the other half return once it heals, after the worker learned
// that it lost their leases.
func benchPartition(ctx context.Context, pool *pgxpool.Pool, dbURL string) {
	const holds, more, lease = 16, 100, 3 * time.Second
	c := fresh(ctx, pool)
	mustExec(ctx, pool, `CREATE TABLE {s}.holds (n integer NOT NULL, worker text NOT NULL, attempt integer NOT NULL,
    phase text NOT NULL, at timestamptz NOT NULL DEFAULT clock_timestamp())`)
	stopMaintainer := maintain(ctx, c) // rescues the cut worker's tasks at once
	u, err := url.Parse(dbURL)
	if err != nil {
		fail(err)
	}
	p := newProxy(ctx, u.Host)
	defer p.close()
	viaProxy := *u
	viaProxy.Host = p.addr()

	spec := child{Role: "worker", Concurrency: holds, Lease: lease, NoMaintainer: true}
	cutSpec, otherSpec := spec, spec
	cutSpec.Name, otherSpec.Name = "cut", "other"
	cut := spawn(ctx, viaProxy.String(), cutSpec)
	batch := make([]taskmill.EnqueueParams, holds)
	for i := range batch {
		batch[i] = taskmill.EnqueueParams{Args: hold{N: i, Wave: 1 + i%2}, Options: &taskmill.EnqueueOptions{MaxAttempts: 5}}
	}
	if _, err := c.EnqueueMany(ctx, batch); err != nil {
		fail(err)
	}
	started := func(worker string) func() bool {
		return func() bool {
			return scalar(ctx, pool, "SELECT count(*) FROM {s}.holds WHERE worker = $1 AND phase = 'start'", worker) == holds
		}
	}
	waitFor(ctx, 30*time.Second, "the worker to start every task", started("cut"))

	p.cut()
	cut.release(1)
	other := spawn(ctx, dbURL, otherSpec)
	takeover := waitFor(ctx, time.Minute, "another worker to take over", started("other"))
	p.heal()
	time.Sleep(2 * lease) // the late results arrive, and the cut worker learns that it lost its leases
	cut.release(2)
	time.Sleep(lease)
	other.release(1)
	other.release(2)
	waitFor(ctx, 30*time.Second, "the tasks to complete", func() bool { return history(ctx, pool) == holds })

	// The cut worker recovers without help: it runs new tasks on its own.
	other.stop()
	enqueueNoop(ctx, c, more)
	recovered := waitFor(ctx, 30*time.Second, "the cut worker to run new tasks",
		func() bool { return history(ctx, pool) == holds+more })
	cut.stop()
	stopMaintainer()

	// Each held task was completed by the other worker's attempt, after its
	// handler returned.
	wrong := scalar(ctx, pool, `
SELECT count(*) FROM {s}.task_history h WHERE h.kind = 'bench.hold' AND NOT EXISTS (
    SELECT FROM {s}.holds x WHERE x.n = (h.payload->>'n')::int AND x.worker = 'other' AND x.phase = 'end'
      AND x.attempt = h.attempt AND x.at < h.finalized_at)`)
	completed := scalar(ctx, pool, "SELECT count(*) FROM {s}.task_history")
	left := scalar(ctx, pool, "SELECT count(*) FROM {s}.tasks")

	fmt.Println("## Network partition")
	fmt.Println()
	fmt.Printf("A worker with %d running tasks and a %s lease was cut off from the database by a proxy that held "+
		"all traffic. Another worker took the tasks over %s after the cut. The cut lasted until then; "+
		"%s bytes that the cut worker sent during it, its results and heartbeats, reached the database after it.\n\n",
		holds, lease, takeover.Round(100*time.Millisecond), commas(p.late.Load()))
	fmt.Println("| ID | Check | Result | |")
	fmt.Println("|---|---|---|---|")
	fmt.Printf("| F7 | The cut worker's late results are rejected | %d of %d tasks completed by a stale attempt | %s |\n",
		wrong, holds, verdict(wrong == 0 && p.late.Load() > 0))
	fmt.Printf("| F7 | Every task completes once; the cut worker recovers on its own | %d completed, %d left; "+
		"it ran %d new tasks in %s | %s |\n", completed, left, more, recovered.Round(10*time.Millisecond),
		verdict(completed == holds+more && left == 0 && recovered <= 10*time.Second))
	fmt.Println()
}
