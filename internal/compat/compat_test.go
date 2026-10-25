// Package compat_test checks the promise that makes rolling deploys safe: the
// latest release's client, worker, and dashboard run on a schema that the
// current code migrated, beside tasks that the current code enqueued. It is a
// module of its own, which requires that release, so that the current code is
// not part of its build; ../cmd/compat is the current code's half.
package compat_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/outbox"
	"github.com/code19m/taskmill/ui"
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
	r.checkDashboard(t)
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

// newRelease prepares a fresh schema with the current code, and opens it with
// the release, which restarts with Migrate as it would during a rolling
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
	schema := runCurrent(t, pool)

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
	return &release{pool: pool, client: client, box: outbox.New(client), ran: make(chan job, 32), published: make(chan string, 1)}
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

// checkRuns waits for every task, the release's and the current code's, to
// run once, and the message to be published.
func (r *release) checkRuns(t *testing.T) {
	t.Helper()
	want := []string{
		"complete in a transaction", "current: deduplicated", "current: delayed",
		"current: fifo", "current: fifo", "current: fifo",
		"enqueue in a transaction", "fifo", "fifo", "fifo", "retry", "schedule",
	}
	var got []string
	groups := make(map[string][]int) // the order each FIFO group ran in
	timeout := time.After(wait)
	for len(got) < len(want) {
		select {
		case j := <-r.ran:
			got = append(got, j.What)
			if j.Seq != 0 {
				groups[j.What] = append(groups[j.What], j.Seq)
			}
		case <-timeout:
			t.Fatalf("the release's worker ran %q, want each of %q", got, want)
		}
	}
	if slices.Sort(got); !slices.Equal(got, want) {
		t.Fatalf("the release's worker ran %q, want %q", got, want)
	}
	for what, order := range groups {
		if !slices.Equal(order, []int{1, 2, 3}) {
			t.Fatalf("the FIFO group of %q ran in the order %v", what, order)
		}
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

// checkDashboard requests each page of the release's dashboard, and takes
// each action, with a task that it then retries.
func (r *release) checkDashboard(t *testing.T) {
	t.Helper()
	later := &taskmill.EnqueueOptions{RunAt: time.Now().Add(time.Hour)}
	retried, err := r.client.Enqueue(t.Context(), job{What: "retried from the dashboard"}, later)
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := r.client.Enqueue(t.Context(), job{What: "deleted from the dashboard"}, later)
	if err != nil {
		t.Fatal(err)
	}
	h := ui.Handler(r.client, nil)
	for _, page := range []string{
		"/", "/tasks", "/tasks?state=completed", fmt.Sprintf("/tasks/%d", retried.ID),
		"/choices/groups?search=current", "/schedules", "/workers",
	} {
		if code := serve(t, h, http.MethodGet, page); code != http.StatusOK {
			t.Errorf("the release's dashboard: GET %s: status %d", page, code)
		}
	}
	for _, action := range []string{
		fmt.Sprintf("/tasks/%d/cancel", retried.ID), fmt.Sprintf("/tasks/%d/retry", retried.ID),
		fmt.Sprintf("/tasks/%d/delete", deleted.ID), "/queues/default/pause", "/queues/default/resume",
		"/schedules/compat/pause", "/schedules/compat/resume", "/schedules/compat/run",
	} {
		if code := serve(t, h, http.MethodPost, action); code != http.StatusSeeOther {
			t.Errorf("the release's dashboard: POST %s: status %d", action, code)
		}
	}
	timeout := time.After(wait)
	for j := (job{}); j.What != "retried from the dashboard"; {
		select {
		case j = <-r.ran:
		case <-timeout:
			t.Fatal("the task retried from the dashboard did not run")
		}
	}
}

func serve(t *testing.T, h http.Handler, method, target string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), method, target, nil))
	return rec.Code
}

// runCurrent migrates a fresh schema with the current code, which then
// enqueues its tasks there, and drops the schema when the test ends.
func runCurrent(t *testing.T, pool *pgxpool.Pool) string {
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
	cmd := exec.CommandContext(t.Context(), "go", "run", "./internal/cmd/compat", schema) //nolint:gosec // the schema name is made above
	cmd.Dir = "../.."                                                                     // the current code's module
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the current code: %v\n%s", err, out)
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
