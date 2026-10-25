// Package compat_test checks the promise that makes rolling deploys safe: the
// latest release's client and worker run on a schema that the current code
// migrated. It is a module of its own, which requires that release, so that
// the current code is not part of its build.
package compat_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/outbox"
)

// job is the task this check enqueues. What names the feature it exercises.
type job struct {
	What string `json:"what"`
	Seq  int    `json:"seq"`
}

func (job) Kind() string { return "compat.job" }

func TestReleaseOnCurrentSchema(t *testing.T) {
	url := os.Getenv("TASKMILL_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TASKMILL_TEST_DATABASE_URL is not set; run `make db` and `make compat`")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	schema := migrateCurrent(t, pool)

	// A release restarts with Migrate, which leaves a newer schema as it is.
	version := schemaVersion(t, pool, schema)
	if err := taskmill.Migrate(ctx, pool, &taskmill.MigrateConfig{Schema: schema}); err != nil {
		t.Fatalf("the release's Migrate: %v", err)
	}
	if v := schemaVersion(t, pool, schema); v != version {
		t.Fatalf("the release's Migrate changed the schema from version %d to %d", version, v)
	}
	client, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Schema: schema})
	if err != nil {
		t.Fatalf("the release's NewClient: %v", err)
	}

	ran := make(chan job, 16)
	var handlers taskmill.Handlers
	taskmill.AddHandler(&handlers, taskmill.HandlerFunc[job](func(ctx context.Context, task *taskmill.Task[job]) error {
		switch task.Args.What {
		case "retry":
			if task.Attempt == 1 {
				return errors.New("the first attempt fails")
			}
		case "complete in a transaction":
			if err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
				return task.CompleteTx(ctx, taskmill.Pgx(tx))
			}); err != nil {
				return err
			}
		}
		ran <- task.Args
		return nil
	}), &taskmill.HandlerOptions{Retry: taskmill.ExponentialBackoff{Base: time.Millisecond, Max: time.Millisecond}})
	box := outbox.New(client)
	published := make(chan string, 1)
	box.AddRelay(&handlers, outbox.PublisherFunc(func(_ context.Context, ds []outbox.Delivery) []error {
		for _, d := range ds {
			published <- string(d.Value)
		}
		return make([]error, len(ds))
	}))

	// Everything is enqueued before the worker starts, so that the FIFO group
	// has tasks waiting behind its head.
	if err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		_, err := client.EnqueueTx(ctx, taskmill.Pgx(tx), job{What: "rolled back"}, nil)
		return errors.Join(err, errors.New("roll back"))
	}); err == nil {
		t.Fatal("the transaction committed")
	}
	if err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		for _, j := range []job{{What: "enqueue in a transaction"}, {What: "retry"}, {What: "complete in a transaction"}} {
			if _, err := client.EnqueueTx(ctx, taskmill.Pgx(tx), j, nil); err != nil {
				return err
			}
		}
		for seq := 1; seq <= 3; seq++ {
			if _, err := client.EnqueueTx(ctx, taskmill.Pgx(tx), job{What: "fifo", Seq: seq},
				&taskmill.EnqueueOptions{GroupKey: "group"}); err != nil {
				return err
			}
		}
		return box.Store(ctx, taskmill.Pgx(tx), outbox.Message{Topic: "compat", Value: []byte("message")})
	}); err != nil {
		t.Fatalf("the release's EnqueueTx: %v", err)
	}

	worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
		Handlers:     &handlers,
		PollInterval: 50 * time.Millisecond,
		Schedules:    []taskmill.Schedule{{Name: "compat", Cron: "@yearly", Args: job{What: "schedule"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- worker.Run(workerCtx) }()
	defer func() {
		stop()
		if err := <-done; err != nil {
			t.Errorf("the release's worker: %v", err)
		}
	}()

	// The maintainer stores the schedule when it starts; then it can run on
	// demand.
	for err := client.RunSchedule(ctx, "compat"); err != nil; err = client.RunSchedule(ctx, "compat") {
		if !errors.Is(err, taskmill.ErrNotFound) {
			t.Fatalf("the release's RunSchedule: %v", err)
		}
		sleep(ctx, 50*time.Millisecond)
	}

	want := []string{"enqueue in a transaction", "retry", "complete in a transaction", "fifo", "fifo", "fifo", "schedule"}
	var got []string
	var fifo []int
	for len(got) < len(want) {
		select {
		case j := <-ran:
			got = append(got, j.What)
			if j.What == "fifo" {
				fifo = append(fifo, j.Seq)
			}
		case <-ctx.Done():
			t.Fatalf("the release's worker ran %q, want each of %q", got, want)
		}
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("the release's worker ran %q, want %q", got, want)
	}
	if !slices.Equal(fifo, []int{1, 2, 3}) {
		t.Fatalf("the FIFO group ran in the order %v", fifo)
	}
	select {
	case v := <-published:
		if v != "message" {
			t.Fatalf("the release's relay published %q", v)
		}
	case <-ctx.Done():
		t.Fatal("the release's relay published nothing")
	}

	// The release's dashboard reads these.
	if _, err := client.List(ctx, &taskmill.ListParams{States: []taskmill.State{taskmill.StateCompleted}}); err != nil {
		t.Errorf("the release's List: %v", err)
	}
	if _, err := client.Queues(ctx); err != nil {
		t.Errorf("the release's Queues: %v", err)
	}
	if _, err := client.Workers(ctx); err != nil {
		t.Errorf("the release's Workers: %v", err)
	}
	if _, err := client.Schedules(ctx); err != nil {
		t.Errorf("the release's Schedules: %v", err)
	}
	if _, err := client.Activity(ctx, nil); err != nil {
		t.Errorf("the release's Activity: %v", err)
	}
}

// migrateCurrent migrates a fresh schema with the current code, and drops it
// when the test ends.
func migrateCurrent(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var b [6]byte
	_, _ = rand.Read(b[:]) // never returns an error
	schema := "compat_" + hex.EncodeToString(b[:])
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			t.Errorf("drop schema %s: %v", schema, err)
		}
	})
	cmd := exec.CommandContext(t.Context(), "go", "run", "./internal/cmd/migrate", schema)
	cmd.Dir = "../.." // the current code's module
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("migrate with the current code: %v\n%s", err, out)
	}
	return schema
}

func schemaVersion(t *testing.T, pool *pgxpool.Pool, schema string) int {
	t.Helper()
	var v int
	if err := pool.QueryRow(t.Context(), "SELECT max(version) FROM "+schema+".migrations").Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}
