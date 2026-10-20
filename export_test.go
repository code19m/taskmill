package taskmill

import (
	"context"
	"strings"
	"time"
)

// Timing shortens a worker's fixed intervals in tests. Zero fields keep
// their defaults.
type Timing struct {
	Maintainer    time.Duration
	Election      time.Duration
	LeaderLease   time.Duration
	Stats         time.Duration
	ShutdownGrace time.Duration
	Reconnect     time.Duration
	NoHandlerWait time.Duration
}

// SetTiming applies t to w before it runs.
func SetTiming(w *Worker, t Timing) {
	set := func(dst *time.Duration, d time.Duration) {
		if d > 0 {
			*dst = d
		}
	}
	set(&w.timing.maintainer, t.Maintainer)
	set(&w.timing.election, t.Election)
	set(&w.timing.leaderLease, t.LeaderLease)
	set(&w.timing.stats, t.Stats)
	set(&w.timing.shutdownGrace, t.ShutdownGrace)
	set(&w.timing.reconnectMin, t.Reconnect)
	set(&w.timing.reconnectMax, t.Reconnect)
	set(&w.timing.noHandlerWait, t.NoHandlerWait)
}

// RunSchedules runs one pass over w's schedules, as its maintainer does each
// cycle.
func RunSchedules(ctx context.Context, w *Worker) {
	newMaintainer(w).runSchedules(ctx)
}

// PruneSchedules deletes the stored schedules that w does not define, as its
// maintainer does once a minute.
func PruneSchedules(ctx context.Context, w *Worker) {
	newMaintainer(w).pruneSchedules(ctx)
}

// ExplainFetch returns the plan of a fetch from queue, as a worker runs it.
func ExplainFetch(ctx context.Context, c *Client, queue string, limit int) (string, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, planSettings); err != nil {
		return "", err
	}
	r, err := tx.Query(ctx, "EXPLAIN "+c.sql.fetch, queue, limit, int64(1), "w")
	if err != nil {
		return "", err
	}
	defer r.Close()
	var plan []string
	for r.Next() {
		var line string
		if err := r.Scan(&line); err != nil {
			return "", err
		}
		plan = append(plan, line)
	}
	return strings.Join(plan, "\n"), r.Err()
}

// RunRetention applies retention once, as w's maintainer does every minute.
func RunRetention(ctx context.Context, w *Worker) {
	newMaintainer(w).retain(ctx)
}
