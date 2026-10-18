// Package taskmilltest helps test code that uses taskmill.
//
// The helpers work against a real PostgreSQL database, so tests see the same
// transactions, FIFO groups, deduplication, and retries as production. Each
// test gets its own schema from [NewClient], so tests can run in parallel.
// Every helper fails the test on errors in its own setup.
//
// A typical test checks two things: that the code under test enqueues the
// right tasks, with [Enqueued], and that a handler does the right work, with
// [Run]. An integration test can also process everything enqueued, with
// [Work].
package taskmilltest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/argkind"
)

// workLimit bounds how long Work waits for the queues to empty.
const workLimit = time.Minute

// NewClient migrates a new schema with a random name in the database that
// pool connects to, and returns a client for it. The schema is dropped when
// the test ends. cfg may be nil; its Schema must be empty.
func NewClient(tb testing.TB, pool *pgxpool.Pool, cfg *taskmill.ClientConfig) *taskmill.Client {
	tb.Helper()
	var c taskmill.ClientConfig
	if cfg != nil {
		c = *cfg
	}
	if c.Schema != "" {
		tb.Fatal("taskmilltest: NewClient picks the schema; leave ClientConfig.Schema empty")
	}
	var b [6]byte
	_, _ = rand.Read(b[:]) // never returns an error
	c.Schema = "taskmilltest_" + hex.EncodeToString(b[:])

	ctx := tb.Context()
	tb.Cleanup(func() {
		// The test's context is canceled before cleanups run.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+c.Schema+" CASCADE"); err != nil {
			tb.Errorf("taskmilltest: drop schema %s: %v", c.Schema, err)
		}
	})
	if err := taskmill.Migrate(ctx, pool, &taskmill.MigrateConfig{Schema: c.Schema}); err != nil {
		tb.Fatalf("taskmilltest: %v", err)
	}
	client, err := taskmill.NewClient(ctx, pool, &c)
	if err != nil {
		tb.Fatalf("taskmilltest: %v", err)
	}
	return client
}

// Enqueued returns every committed task of A's kind, oldest first, with its
// arguments decoded: unfinished tasks, finished ones, and those in history.
// Use it to check that the code under test enqueued a task when its
// transaction committed, and no task when it rolled back.
//
// The returned tasks cannot complete themselves with [taskmill.Task.CompleteTx];
// use [Run] to run a handler.
func Enqueued[A taskmill.Args](tb testing.TB, client *taskmill.Client) []*taskmill.Task[A] {
	tb.Helper()
	kind, err := argkind.Of[A]()
	if err != nil {
		tb.Fatalf("taskmilltest: %v", err)
	}
	var out []*taskmill.Task[A]
	params := &taskmill.ListParams{Kind: kind, Limit: 1000}
	for {
		page, err := client.List(tb.Context(), params)
		if err != nil {
			tb.Fatalf("taskmilltest: %v", err)
		}
		for _, info := range page.Tasks {
			var args A
			if err := json.Unmarshal(info.Payload, &args); err != nil {
				tb.Fatalf("taskmilltest: decode task %d: %v", info.ID, err)
			}
			out = append(out, &taskmill.Task[A]{TaskInfo: info, Args: args})
		}
		if page.Next == "" {
			break
		}
		params.Cursor = page.Next
	}
	slices.Reverse(out) // List returns the newest first
	return out
}

// RunOptions configure [Run]. The zero value is valid.
type RunOptions struct {
	// Enqueue are the options the task is enqueued with.
	Enqueue *taskmill.EnqueueOptions

	// Handler are the options the handler runs with, such as its timeout.
	Handler *taskmill.HandlerOptions
}

// Run enqueues a task with args, runs h on it once, the way a worker does,
// and records the outcome. The attempt holds a real lease, so h may complete
// the task with [taskmill.Task.CompleteTx]. Run does not retry: a failed task
// stays retryable or dead. It returns the task afterwards, and the error h
// returned, or an error describing its panic. opts may be nil.
func Run[A taskmill.Args](tb testing.TB, client *taskmill.Client, h taskmill.Handler[A], args A, opts *RunOptions) (*taskmill.TaskInfo, error) {
	tb.Helper()
	var o RunOptions
	if opts != nil {
		o = *opts
	}
	res, err := client.Enqueue(tb.Context(), args, o.Enqueue)
	if err != nil {
		tb.Fatalf("taskmilltest: %v", err)
	}

	var handlerErr error
	var hs taskmill.Handlers
	if h != nil {
		taskmill.AddHandler(&hs, taskmill.HandlerFunc[A](func(ctx context.Context, task *taskmill.Task[A]) error {
			defer func() {
				if r := recover(); r != nil {
					handlerErr = fmt.Errorf("handler panicked: %v", r)
					panic(r) // for the worker to record, as it would in production
				}
			}()
			handlerErr = h.Handle(ctx, task)
			return handlerErr
		}), o.Handler)
	} else {
		taskmill.AddHandler[A](&hs, nil, o.Handler) // reported by NewWorker
	}
	w, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &hs, NoMaintainer: true})
	if err != nil {
		tb.Fatalf("taskmilltest: %v", err)
	}
	info, err := w.RunTask(tb.Context(), res.ID)
	if err != nil {
		tb.Fatalf("taskmilltest: %v", err)
	}
	return info, handlerErr
}

// Work runs a worker with cfg until no task of its queues is available or
// running, then stops it. Tasks that wait for a later run time, such as
// retries, stay waiting. If cfg.PollInterval is zero, the worker polls every
// 10 milliseconds. Work fails the test if the worker fails, or if the queues
// are not empty within a minute.
func Work(tb testing.TB, client *taskmill.Client, cfg *taskmill.WorkerConfig) {
	tb.Helper()
	if cfg == nil {
		tb.Fatal("taskmilltest: Work needs a WorkerConfig with handlers and queues")
	}
	c := *cfg
	if c.PollInterval == 0 {
		c.PollInterval = 10 * time.Millisecond
	}
	w, err := taskmill.NewWorker(client, &c)
	if err != nil {
		tb.Fatalf("taskmilltest: %v", err)
	}

	ctx, cancel := context.WithCancel(tb.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	limit := time.After(workLimit)
	for {
		busy, err := busy(ctx, client, c.Queues)
		if err != nil {
			tb.Fatalf("taskmilltest: %v", err)
		}
		if !busy {
			break
		}
		select {
		case err := <-done:
			tb.Fatalf("taskmilltest: worker stopped: %v", err)
		case <-limit:
			tb.Fatalf("taskmilltest: tasks were still available or running after %s", workLimit)
		case <-time.After(c.PollInterval):
		}
	}
	cancel()
	if err := <-done; err != nil {
		tb.Fatalf("taskmilltest: worker: %v", err)
	}
}

// busy reports whether a task of one of queues is available or running. A
// task stays running until its result is recorded, in the same transaction
// that promotes the next task of its FIFO group, so no check falls between a
// task's end and its successor's start.
func busy(ctx context.Context, client *taskmill.Client, queues map[string]taskmill.QueueOptions) (bool, error) {
	params := &taskmill.ListParams{States: []taskmill.State{taskmill.StateAvailable, taskmill.StateRunning}, Limit: 1000}
	for {
		page, err := client.List(ctx, params)
		if err != nil {
			return false, err
		}
		for _, t := range page.Tasks {
			if _, ok := queues[t.Queue]; ok {
				return true, nil
			}
		}
		if page.Next == "" {
			return false, nil
		}
		params.Cursor = page.Next
	}
}
