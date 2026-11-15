package taskmill_test

import (
	"testing"
	"time"

	"github.com/code19m/taskmill"
)

// A worker checks in every third of its LeaseDuration: with a lease of 10
// minutes, it is silent for more than 3 minutes while it is healthy. So how
// long a worker may be silent before it counts as stale, or as gone, follows
// the lease that it stored when it registered.

// longLease starts a worker with a lease of 10 minutes, which makes no
// check-in while the test runs, and returns its ID once it is registered.
func longLease(t *testing.T, e *env) string {
	t.Helper()
	runWorker(t, e, &taskmill.WorkerConfig{LeaseDuration: 10 * time.Minute, NoMaintainer: true})
	var id string
	waitFor(t, "the worker to register", func() bool {
		workers, err := e.client.Workers(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(workers) == 1 {
			id = workers[0].ID
		}
		return id != ""
	})
	return id
}

// silent sets the last check-in of worker id to ago before now, as if it had
// made none since.
func (e *env) silent(t *testing.T, id, ago string) {
	t.Helper()
	e.exec(t, "UPDATE {s}.workers SET heartbeat_at = now() - interval '"+ago+"' WHERE id = $1", id)
}

// oldWorker registers a worker as the previous release does, which stores no
// lease.
func (e *env) oldWorker(t *testing.T) string {
	t.Helper()
	e.exec(t, "INSERT INTO {s}.workers (id, queues, hostname, version) VALUES ('old', '{}', 'host', 'v0.6.0')")
	return "old"
}

func TestWorkerIsStaleAfterItsOwnLease(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	long, old := longLease(t, e), e.oldWorker(t)
	for _, tt := range []struct {
		id, ago string
		stale   bool
	}{
		{long, "5 minutes", false}, // one check-in was late: its tasks are still leased
		{long, "11 minutes", true},
		{old, "20 seconds", false}, // a release that stores no lease: 30 seconds, as before
		{old, "40 seconds", true},
	} {
		e.silent(t, tt.id, tt.ago)
		workers, err := e.client.Workers(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		for _, w := range workers {
			if w.ID == tt.id && w.Stale != tt.stale {
				t.Errorf("the worker %s, silent for %s: stale is %v, want %v", tt.id, tt.ago, w.Stale, tt.stale)
			}
		}
	}
}

func TestMaintainerRemovesAWorkerByItsOwnLease(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	long, old := longLease(t, e), e.oldWorker(t)
	maintainer := newWorker(t, e, &taskmill.WorkerConfig{})
	listed := func(id string) bool {
		return e.count(t, "SELECT count(*) FROM {s}.workers WHERE id = $1", id) == 1
	}

	// Two check-ins of the long lease are missing, and its tasks are still
	// leased. The other worker has been silent for 5 minutes, as before.
	e.silent(t, long, "7 minutes")
	e.silent(t, old, "7 minutes")
	taskmill.RunRetention(t.Context(), maintainer)
	if !listed(long) {
		t.Fatal("the maintainer removed a worker that was silent for less than its lease")
	}
	if listed(old) {
		t.Fatal("the maintainer kept a worker of the previous release that was silent for 7 minutes")
	}

	e.silent(t, long, "21 minutes")
	taskmill.RunRetention(t.Context(), maintainer)
	if listed(long) {
		t.Fatal("the maintainer kept a worker that was silent for more than twice its lease")
	}
}

// The workers table accepts a lease of up to 300 years, which is more than
// any worker has. The statements that move a time back by a lease, or by two,
// must work with the longest one: a row that fails them fails the list of
// the workers, and the removal of the others.
func TestWorkerWithTheLongestLeaseDoesNotFailTheOthers(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	old := e.oldWorker(t)
	e.exec(t, `INSERT INTO {s}.workers (id, queues, hostname, version, lease_duration)
	           VALUES ('longest', '{}', 'host', 'v', interval '300 years')`)

	e.exec(t, "UPDATE {s}.workers SET heartbeat_at = now() - interval '7 minutes'")
	workers, err := e.client.Workers(t.Context())
	if err != nil || len(workers) != 2 {
		t.Fatalf("list the workers: %d of 2, %v", len(workers), err)
	}
	for _, w := range workers {
		if w.Stale != (w.ID == old) {
			t.Errorf("the worker %s, silent for 7 minutes: stale is %v", w.ID, w.Stale)
		}
	}

	taskmill.RunRetention(t.Context(), newWorker(t, e, &taskmill.WorkerConfig{}))
	if n := e.count(t, "SELECT count(*) FROM {s}.workers WHERE id = $1", old); n != 0 {
		t.Errorf("the maintainer kept the worker %q, silent for 7 minutes, beside one with the longest lease", old)
	}
}
