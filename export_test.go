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
	Heartbeat     time.Duration
	Election      time.Duration
	LeaderLease   time.Duration
	Stats         time.Duration
	ShutdownGrace time.Duration
	Reconnect     time.Duration
	NoHandlerWait time.Duration
	FailureReport time.Duration
	IdlePoll      time.Duration
	Listen        time.Duration
}

// SetTiming applies t to w before it runs.
func SetTiming(w *Worker, t Timing) {
	set := func(dst *time.Duration, d time.Duration) {
		if d > 0 {
			*dst = d
		}
	}
	set(&w.timing.maintainer, t.Maintainer)
	set(&w.timing.heartbeat, t.Heartbeat)
	set(&w.timing.election, t.Election)
	set(&w.timing.leaderLease, t.LeaderLease)
	set(&w.timing.stats, t.Stats)
	set(&w.timing.shutdownGrace, t.ShutdownGrace)
	set(&w.timing.reconnectMin, t.Reconnect)
	set(&w.timing.reconnectMax, t.Reconnect)
	set(&w.timing.noHandlerWait, t.NoHandlerWait)
	set(&w.timing.failureReport, t.FailureReport)
	set(&w.timing.idlePoll, t.IdlePoll)
	set(&w.timing.listen, t.Listen)
}

// Notifying reports whether the maintainer's heartbeats reach w.
func Notifying(w *Worker) bool {
	return w.notifying.Load()
}

// RunSchedules runs one pass over w's schedules, as its maintainer does each
// cycle.
func RunSchedules(ctx context.Context, w *Worker) {
	newMaintainer(w).runSchedules(ctx)
}

// WakeQueues notifies the queues whose tasks wait, and sends the heartbeat,
// as w's maintainer does.
func WakeQueues(ctx context.Context, w *Worker) {
	newMaintainer(w).wake(ctx, true)
}

// PruneSchedules deletes the stored schedules that w does not define, as its
// maintainer does once a minute.
func PruneSchedules(ctx context.Context, w *Worker) {
	newMaintainer(w).pruneSchedules(ctx)
}

// ExplainFetch returns the plan of a fetch from queue, as a worker runs it.
func ExplainFetch(ctx context.Context, c *Client, queue string, limit int) (string, error) {
	return explain(ctx, c, planSettings, c.sql.fetch, queue, limit, int64(1), "w")
}

// ExplainPromote returns the plan of the statement that promotes due tasks.
func ExplainPromote(ctx context.Context, c *Client) (string, error) {
	return explain(ctx, c, "", c.sql.promoteDue, c.schema)
}

// ExplainLeased returns the plans of the statements that find tasks by their
// attempts: the completion, the heartbeat, and the lock of a grouped
// completion.
func ExplainLeased(ctx context.Context, c *Client) (string, error) {
	ids, tokens := []int64{1, 2, 3}, []int32{1, 1, 1}
	var plans []string
	for _, s := range []struct {
		sql  string
		args []any
	}{
		{c.sql.complete, []any{ids, tokens, []int64{}}},
		{c.sql.heartbeat, []any{ids, tokens, int64(1)}},
		{c.sql.lockLeased, []any{ids, tokens}},
		{c.sql.fail, []any{int64(1), int32(1), "retryable", int64(1), `{"message": "m"}`}},
		{c.sql.snooze, []any{int64(1), int32(1), int64(1), nil}},
		{c.sql.release, []any{int64(1), int32(1)}},
		{c.sql.cancelRunning, []any{int64(1), int32(1)}},
	} {
		plan, err := explain(ctx, c, "", s.sql, s.args...)
		if err != nil {
			return "", err
		}
		plans = append(plans, plan)
	}
	return strings.Join(plans, "\n"), nil
}

// explain returns the plan of sql, after settings if any, in a transaction
// that it rolls back.
func explain(ctx context.Context, c *Client, settings, sql string, args ...any) (string, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if settings != "" {
		if _, err := tx.Exec(ctx, settings); err != nil {
			return "", err
		}
	}
	r, err := tx.Query(ctx, "EXPLAIN "+sql, args...)
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
	m, err := c.bulk(ctx, &BulkParams{Filter: *filter}, states)
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

// StoredSize returns at most the size of JSON text as PostgreSQL writes it
// out of jsonb, as the payload limit counts it.
func StoredSize(text []byte) int {
	size, _ := storedSize(text)
	return size
}
