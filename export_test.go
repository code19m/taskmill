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

// StoredSize returns at most the size of JSON text as PostgreSQL writes it
// out of jsonb, as the payload limit counts it.
func StoredSize(text []byte) int {
	size, _ := storedSize(text)
	return size
}
