package taskmill

import (
	"context"
	"fmt"
	"time"
)

// This file holds the list of registered workers that Client.Workers reads.

// WorkerInfo describes a worker process.
type WorkerInfo struct {
	// ID names the worker: its host name, its process ID, and a random
	// suffix, joined by dashes.
	ID       string
	Hostname string
	Version  string // taskmill's version in the worker's binary
	Queues   []string
	Kinds    []string // the kinds it has handlers for

	// Schedules are the names of the worker's schedules, sorted, if it is a
	// Candidate: see [WorkerConfig.Schedules]. It is empty for any other
	// worker.
	Schedules []string

	// StartedAt is when the worker last registered: when its Run started,
	// or when it registered again after the maintainer removed it as stale.
	StartedAt   time.Time
	CheckedInAt time.Time // its last check-in, which it makes every third of its LeaseDuration
	Maintainer  bool      // the worker is the elected maintainer

	// Candidate reports that the worker takes part in the maintainer
	// election, so its Schedules count: the maintainer logs an error when
	// the schedules of the candidates keep differing, see [Schedule]. It is
	// false for a worker with [WorkerConfig.NoMaintainer], for one that is
	// stopping, which no longer takes part, and for a worker of a release
	// before v0.13.0, which does not say.
	Candidate bool

	// Stale reports that the worker made no check-in for its whole
	// [WorkerConfig.LeaseDuration], by the database's clock. It stopped
	// without unregistering, or it cannot reach the database: the leases of
	// its tasks have run out, and its [Worker.Health] fails. A worker of a
	// release that does not store its LeaseDuration is stale after 30
	// seconds without a check-in.
	Stale bool
}

// staleWithoutLease is how long a worker that stored no lease may make no
// check-in before it is stale: the workers of releases without the column.
const staleWithoutLease = 30 * time.Second

// Workers returns the registered worker processes, ordered by ID. A worker
// that stopped without unregistering is stale once it made no check-in for
// its LeaseDuration: see [WorkerInfo.Stale]. It disappears once it made
// none for 5 minutes, or for twice its LeaseDuration if that is longer: the
// maintainer removes it.
func (c *Client) Workers(ctx context.Context) ([]WorkerInfo, error) {
	fail := func(err error) error { return fmt.Errorf("taskmill: list workers: %w", err) }
	r, err := c.pool.Query(ctx, c.sql.listWorkers, staleWithoutLease.Microseconds())
	if err != nil {
		return nil, fail(err)
	}
	defer r.Close()

	var out []WorkerInfo
	for r.Next() {
		var w WorkerInfo
		err := r.Scan(&w.ID, &w.Hostname, &w.Version, &w.Queues, &w.Kinds, &w.Schedules, &w.StartedAt, &w.CheckedInAt, &w.Maintainer, &w.Candidate, &w.Stale)
		if err != nil {
			return nil, fail(err)
		}
		out = append(out, w)
	}
	if err := r.Err(); err != nil {
		return nil, fail(err)
	}
	return out, nil
}
