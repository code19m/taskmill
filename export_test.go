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
	FailureReport time.Duration
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
	set(&w.timing.failureReport, t.FailureReport)
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

// BulkRowsRead runs the next batch's pick of a bulk action (retry, cancel,
// delete, or history) on filter after task after, under EXPLAIN ANALYZE in a
// transaction that it rolls back, and returns how many rows the scans of
// the task tables read.
func BulkRowsRead(ctx context.Context, c *Client, action string, filter *TaskFilter, after int64) (int64, error) {
	sql, states := map[string]string{
		"retry": c.sql.pickRetry, "cancel": c.sql.pickCancel, "delete": c.sql.pickDelete, "history": c.sql.pickCompleted,
	}[action], map[string][]State{"retry": retryStates, "cancel": cancelStates}[action]
	if states == nil {
		states = deleteStates
	}
	m, err := c.bulk(ctx, filter, states)
	if err != nil {
		return 0, err
	}
	args := []any{m.queues, m.kinds, m.groups, m.states, after, m.last, m.left.queues, m.left.groups}
	if action == "history" {
		args = []any{m.queues, m.kinds, m.groups, after, m.last}
	}
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, bulkSettings); err != nil {
		return 0, err
	}
	var plan []map[string]any
	if err := tx.QueryRow(ctx, "EXPLAIN (ANALYZE, FORMAT JSON) "+sql, args...).Scan(&plan); err != nil {
		return 0, err
	}
	var read func(node map[string]any) int64
	read = func(node map[string]any) int64 {
		var n int64
		if rel, _ := node["Relation Name"].(string); rel == "tasks" || strings.HasPrefix(rel, "task_history") {
			for _, key := range []string{"Actual Rows", "Rows Removed by Filter", "Rows Removed by Index Recheck"} {
				v, _ := node[key].(float64)
				loops, _ := node["Actual Loops"].(float64)
				n += int64(v * loops)
			}
		}
		children, _ := node["Plans"].([]any)
		for _, child := range children {
			if c, ok := child.(map[string]any); ok {
				n += read(c)
			}
		}
		return n
	}
	root, _ := plan[0]["Plan"].(map[string]any)
	return read(root), nil
}
