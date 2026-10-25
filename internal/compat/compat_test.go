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

// wait bounds each wait for the release's worker.
const wait = time.Minute

// job is the task this check enqueues. What names the feature it exercises.
type job struct {
	What string `json:"what"`
	Seq  int    `json:"seq"`
}

func (job) Kind() string { return "compat.job" }

func TestReleaseOnCurrentSchema(t *testing.T) {
	r := newRelease(t)
	r.enqueue(t)
	r.work(t)
	r.runSchedule(t)
	r.checkRuns(t)
	r.checkReads(t)
}

// release is the latest release's client on a schema that the current code
// migrated, and what its handlers did.
type release struct {
	pool      *pgxpool.Pool
	client    *taskmill.Client
	box       *outbox.Outbox
	ran       chan job
	published chan string
}

// newRelease migrates a fresh schema with the current code, and opens it
// with the release, which restarts with Migrate as it would during a rolling
// deploy.
func newRelease(t *testing.T) *release {
	t.Helper()
	url := os.Getenv("TASKMILL_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TASKMILL_TEST_DATABASE_URL is not set; run `make db` and `make compat`")
	}
	pool, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	schema := migrateCurrent(t, pool)

	version := schemaVersion(t, pool, schema)
	if err := taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: schema}); err != nil {
		t.Fatalf("the release's Migrate: %v", err)
	}
	if v := schemaVersion(t, pool, schema); v != version {
		t.Fatalf("the release's Migrate changed the schema from version %d to %d", version, v)
	}
	client, err := taskmill.NewClient(t.Context(), pool, &taskmill.ClientConfig{Schema: schema})
	if err != nil {
		t.Fatalf("the release's NewClient: %v", err)
	}
	return &release{pool: pool, client: client, box: outbox.New(client), ran: make(chan job, 16), published: make(chan string, 1)}
}

// enqueue enqueues every task before the worker starts, so that the FIFO
// group has tasks waiting behind its head.
func (r *release) enqueue(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	if err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		_, err := r.client.EnqueueTx(ctx, taskmill.Pgx(tx), job{What: "rolled back"}, nil)
		return errors.Join(err, errors.New("roll back"))
	}); err == nil {
		t.Fatal("the transaction committed")
	}
	if err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		for _, j := range []job{{What: "enqueue in a transaction"}, {What: "retry"}, {What: "complete in a transaction"}} {
			if _, err := r.client.EnqueueTx(ctx, taskmill.Pgx(tx), j, nil); err != nil {
				return err
			}
		}
		for seq := 1; seq <= 3; seq++ {
			if _, err := r.client.EnqueueTx(ctx, taskmill.Pgx(tx), job{What: "fifo", Seq: seq},
				&taskmill.EnqueueOptions{GroupKey: "group"}); err != nil {
				return err
			}
		}
		return r.box.Store(ctx, taskmill.Pgx(tx), outbox.Message{Topic: "compat", Value: []byte("message")})
	}); err != nil {
		t.Fatalf("the release's EnqueueTx: %v", err)
	}
}

// work runs the release's worker until the test ends.
func (r *release) work(t *testing.T) {
	t.Helper()
	var handlers taskmill.Handlers
	taskmill.AddHandler(&handlers, taskmill.HandlerFunc[job](r.handle),
		&taskmill.HandlerOptions{Retry: taskmill.ExponentialBackoff{Base: time.Millisecond, Max: time.Millisecond}})
	r.box.AddRelay(&handlers, outbox.PublisherFunc(func(_ context.Context, ds []outbox.Delivery) []error {
		for _, d := range ds {
			r.published <- string(d.Value)
		}
		return make([]error, len(ds))
	}))
	worker, err := taskmill.NewWorker(r.client, &taskmill.WorkerConfig{
		Handlers:     &handlers,
		PollInterval: 50 * time.Millisecond,
		Schedules:    []taskmill.Schedule{{Name: "compat", Cron: "@yearly", Args: job{What: "schedule"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	t.Cleanup(func() {
		stop()
		if err := <-done; err != nil {
			t.Errorf("the release's worker: %v", err)
		}
	})
}

func (r *release) handle(ctx context.Context, task *taskmill.Task[job]) error {
	switch task.Args.What {
	case "retry":
		if task.Attempt == 1 {
			return errors.New("the first attempt fails")
		}
	case "complete in a transaction":
		if err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
			return task.CompleteTx(ctx, taskmill.Pgx(tx))
		}); err != nil {
			return err
		}
	}
	r.ran <- task.Args
	return nil
}

// runSchedule runs the schedule on demand, once the maintainer has stored it.
func (r *release) runSchedule(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for {
		err := r.client.RunSchedule(t.Context(), "compat")
		if err == nil {
			return
		}
		if !errors.Is(err, taskmill.ErrNotFound) || time.Now().After(deadline) {
			t.Fatalf("the release's RunSchedule: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// checkRuns waits for every task to run once, and the message to be
// published.
func (r *release) checkRuns(t *testing.T) {
	t.Helper()
	want := []string{"complete in a transaction", "enqueue in a transaction", "fifo", "fifo", "fifo", "retry", "schedule"}
	var got []string
	var fifo []int
	timeout := time.After(wait)
	for len(got) < len(want) {
		select {
		case j := <-r.ran:
			got = append(got, j.What)
			if j.What == "fifo" {
				fifo = append(fifo, j.Seq)
			}
		case <-timeout:
			t.Fatalf("the release's worker ran %q, want each of %q", got, want)
		}
	}
	if slices.Sort(got); !slices.Equal(got, want) {
		t.Fatalf("the release's worker ran %q, want %q", got, want)
	}
	if !slices.Equal(fifo, []int{1, 2, 3}) {
		t.Fatalf("the FIFO group ran in the order %v", fifo)
	}
	select {
	case v := <-r.published:
		if v != "message" {
			t.Fatalf("the release's relay published %q", v)
		}
	case <-time.After(wait):
		t.Fatal("the release's relay published nothing")
	}
}

// checkReads makes the reads of the release's dashboard.
func (r *release) checkReads(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	_, err := r.client.List(ctx, &taskmill.ListParams{States: []taskmill.State{taskmill.StateCompleted}})
	_, err2 := r.client.Queues(ctx)
	_, err3 := r.client.Workers(ctx)
	_, err4 := r.client.Schedules(ctx)
	_, err5 := r.client.Activity(ctx, nil)
	if err := errors.Join(err, err2, err3, err4, err5); err != nil {
		t.Fatalf("the release's dashboard reads: %v", err)
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
	cmd := exec.CommandContext(t.Context(), "go", "run", "./internal/cmd/migrate", schema) //nolint:gosec // the schema name is made above
	cmd.Dir = "../.."                                                                      // the current code's module
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
