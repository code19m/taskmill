package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
)

// loadRate is half of T3's target, the load at which L2 is measured.
const loadRate = 10_000

// L1 to L3: how long an enqueued task waits for a worker, idle and under
// load, and how long an enqueue takes.
func benchLatency(ctx context.Context, pool *pgxpool.Pool, d time.Duration) {
	idle := pickup(ctx, pool, d, 0)
	busy := pickup(ctx, pool, d, loadRate)
	calls := enqueueCalls(ctx, pool, d)

	fmt.Println("## Latency")
	fmt.Println()
	fmt.Println("| ID | Measure | Result |")
	fmt.Println("|---|---|---|")
	fmt.Printf("| L1 | Pickup latency of an idle worker, %s tasks | %s |\n", commas(int64(len(idle))), percentiles(idle))
	fmt.Printf("| L2 | Pickup latency at %s tasks/s, 1 worker with concurrency 64, %s tasks | %s |\n",
		commas(loadRate), commas(int64(len(busy))), percentiles(busy))
	fmt.Printf("| L3 | `Enqueue` call latency, one task, no load, %s calls | %s |\n", commas(int64(len(calls))), percentiles(calls))
	fmt.Println()
}

// pickup measures the time from just before an enqueue's transaction to the
// handler's start. With a rate of 0, it enqueues one task at a time, spaced
// beyond the notification throttle window, so only notifications wake the
// worker. Otherwise it enqueues rate tasks a second, in transactions of 10.
func pickup(ctx context.Context, pool *pgxpool.Pool, d time.Duration, rate int) []time.Duration {
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
	cfg := &taskmill.WorkerConfig{
		Handlers: &hs,
		Queues:   map[string]taskmill.QueueOptions{taskmill.DefaultQueue: {Concurrency: 64}},
	}
	if rate == 0 {
		cfg.Queues = nil
		cfg.PollInterval = time.Minute // only notifications wake the worker
	}
	w, err := taskmill.NewWorker(c, cfg)
	if err != nil {
		fail(err)
	}
	stop := run(ctx, w)
	time.Sleep(time.Second) // let the worker connect

	if rate == 0 {
		end := time.Now().Add(d)
		for time.Now().Before(end) && ctx.Err() == nil {
			enqueueStamped(ctx, c, pool, 1)
			time.Sleep(100 * time.Millisecond) // beyond the notification throttle window
		}
	} else {
		paced(ctx, d, rate/10, func() { enqueueStamped(ctx, c, pool, 10) })
	}
	time.Sleep(time.Second)
	stop()
	mu.Lock()
	defer mu.Unlock()
	return lat
}

// enqueueStamped enqueues n tasks in one transaction, stamped with the time
// just before it, so latencies include the enqueue's and the commit's round
// trips.
func enqueueStamped(ctx context.Context, c *taskmill.Client, pool *pgxpool.Pool, n int) {
	sent := time.Now().UnixNano()
	batch := make([]taskmill.EnqueueParams, n)
	for i := range batch {
		batch[i] = taskmill.EnqueueParams{Args: noop{Sent: sent}}
	}
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		_, err := c.EnqueueManyTx(ctx, taskmill.Pgx(tx), batch)
		return err
	})
	if err != nil {
		fail(err)
	}
}

// paced calls fn perSecond times a second for d, from 8 goroutines. Calls
// that would start late are skipped, so a slow fn lowers the rate rather than
// bunching calls. The last calls finish after d.
func paced(ctx context.Context, d time.Duration, perSecond int, fn func()) {
	const goroutines = 8
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			tick := time.NewTicker(time.Second * goroutines / time.Duration(perSecond))
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					fn()
				}
			}
		})
	}
	wg.Wait()
}

// enqueueCalls measures single enqueues, one at a time, for d.
func enqueueCalls(ctx context.Context, pool *pgxpool.Pool, d time.Duration) []time.Duration {
	c := fresh(ctx, pool)
	var lat []time.Duration
	for end := time.Now().Add(d); time.Now().Before(end) && ctx.Err() == nil; {
		start := time.Now()
		if _, err := c.Enqueue(ctx, noop{}, nil); err != nil {
			fail(err)
		}
		lat = append(lat, time.Since(start))
	}
	return lat
}
