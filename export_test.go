package taskmill

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// BatchTimeout is the time that a batch of a bulk action has.
const BatchTimeout = batchTimeout

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

// Elect runs one try of worker id to take or renew the maintainer's lease,
// as its maintainer does, in a transaction of its own. It reports whether
// the worker leads, and whether the try wrote: a transaction that writes
// gets a transaction ID.
func Elect(ctx context.Context, c *Client, id string) (leads, wrote bool, err error) {
	err = pgx.BeginFunc(ctx, c.pool, func(tx pgx.Tx) error {
		var leader string
		err := tx.QueryRow(ctx, c.sql.electLeader, id, time.Minute.Microseconds()).Scan(&leader)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		leads = err == nil
		return tx.QueryRow(ctx, "SELECT pg_current_xact_id_if_assigned() IS NOT NULL").Scan(&wrote)
	})
	return leads, wrote, err
}

// ExplainFetch returns the plan of a fetch from queue, as a worker runs it.
func ExplainFetch(ctx context.Context, c *Client, queue string, limit int) (string, error) {
	return explain(ctx, c, planSettings, c.sql.fetch, queue, limit, int64(1), "w")
}

// FillRowsRead runs the statement that fills a batch with up to limit more
// tasks of kind from queue, as a worker runs it, under EXPLAIN ANALYZE in a
// transaction that it rolls back, and returns how many rows its scans of
// tasks read.
func FillRowsRead(ctx context.Context, c *Client, queue, kind string, limit int) (int64, error) {
	return rowsRead(ctx, c, planSettings, c.sql.fetchKind, []any{queue, limit, int64(1), "w", kind, maxFetch},
		func(rel string) bool { return rel == "tasks" })
}

// ExplainPromote returns the plan of the statement that promotes due tasks.
func ExplainPromote(ctx context.Context, c *Client) (string, error) {
	return explain(ctx, c, "", c.sql.promoteDue, c.schema)
}

// ExplainLeased returns the plans of the statements that find tasks by their
// attempts: those of a batch of attempts, and those of one.
func ExplainLeased(ctx context.Context, c *Client) (string, error) {
	var plans []string
	for _, s := range append(leased(c, []int64{1, 2, 3}, []int32{1, 1, 1}), []statement{
		{c.sql.fail, []any{int64(1), int32(1), "retryable", int64(1), `{"message": "m"}`}},
		{c.sql.snooze, []any{int64(1), int32(1), int64(1), nil, true}},
		{c.sql.release, []any{int64(1), int32(1), true}},
	}...) {
		plan, err := explain(ctx, c, "", s.sql, s.args...)
		if err != nil {
			return "", err
		}
		plans = append(plans, plan)
	}
	return strings.Join(plans, "\n"), nil
}

// statement is a statement with its arguments.
type statement struct {
	sql  string
	args []any
}

// leased returns the statements that find a batch of tasks by their attempts
// (ids, tokens): the lock of a grouped completion, the lease renewal, the
// read of cancel requests, and the completion.
func leased(c *Client, ids []int64, tokens []int32) []statement {
	return []statement{
		{c.sql.lockLeased, []any{ids, tokens}},
		{c.sql.renewLeases, []any{ids, tokens, time.Hour.Microseconds()}},
		{c.sql.cancelRequest, []any{ids, tokens}},
		{c.sql.complete, []any{ids, tokens, []int64{}}},
	}
}

// LeasedRowsRead runs the statements that find a batch of tasks by their
// attempts (ids, tokens), with their generic plans, under EXPLAIN ANALYZE,
// each in a transaction that it rolls back. It returns how many rows of
// tasks they read, plus the pairs of a task and an attempt that their joins
// compared and rejected.
func LeasedRowsRead(ctx context.Context, c *Client, ids []int64, tokens []int32) (int64, error) {
	tasks := scanned(func(rel string) bool { return rel == "tasks" })
	var n int64
	for _, s := range leased(c, ids, tokens) {
		plan, err := analyze(ctx, c, true, "", s.sql, s.args...)
		if err != nil {
			return 0, err
		}
		n += planSum(plan, func(node map[string]any) float64 {
			rejected, _ := node["Rows Removed by Join Filter"].(float64)
			return tasks(node) + rejected
		})
	}
	return n, nil
}

// LeasedFound runs the same statements in one transaction, in that order,
// the completion last. It returns the IDs of the rows that each one
// returned, sorted.
func LeasedFound(ctx context.Context, c *Client, ids []int64, tokens []int32) (found [4][]int64, err error) {
	err = pgx.BeginFunc(ctx, c.pool, func(tx pgx.Tx) error {
		for i, s := range leased(c, ids, tokens) {
			r, err := tx.Query(ctx, s.sql, s.args...)
			if err != nil {
				return err
			}
			found[i], err = pgx.CollectRows(r, func(row pgx.CollectableRow) (int64, error) {
				values, err := row.Values()
				if err != nil {
					return 0, err
				}
				id, _ := values[0].(int64)
				return id, nil
			})
			if err != nil {
				return err
			}
			slices.Sort(found[i])
		}
		return nil
	})
	return found, err
}

// Complete completes the attempts with the IDs ids, each with the lease
// token 1, given in the order in which they ended, as a worker's completer
// does. It does so in a transaction that starts with settings, and that it
// rolls back. It returns how many tasks it completed.
func Complete(ctx context.Context, c *Client, settings string, ids []int64) (int, error) {
	cs := make([]completion, len(ids))
	for i, id := range ids {
		cs[i] = completion{id: id, token: 1}
	}
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, settings); err != nil {
		return 0, err
	}
	done, err := c.complete(ctx, c.logger, pgxQuerier{q: tx}, cs)
	return len(done), err
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
// the task tables read. With window, the batch reads in the window that the
// action finds there, as it does once a batch could not end in time.
func BulkRowsRead(ctx context.Context, c *Client, action string, filter *TaskFilter, after int64, window bool) (int64, error) {
	sql, states := map[string]string{
		"retry": c.sql.pickRetry, "cancel": c.sql.pickCancel, "delete": c.sql.pickDelete, "history": c.sql.pickCompleted,
	}[action], map[string][]State{"retry": retryStates, "cancel": cancelStates}[action]
	if states == nil {
		states = deleteStates
	}
	m, err := checkBulk(&BulkParams{Filter: *filter}, states)
	if err != nil {
		return 0, err
	}
	m.after = after
	if err := c.pool.QueryRow(ctx, c.sql.lastTask).Scan(&m.last); err != nil {
		return 0, err
	}
	m.until = m.last
	if window {
		if err := c.nextWindow(ctx, m); err != nil {
			return 0, err
		}
	}
	args := []any{m.queues, m.kinds, m.groups, m.states, m.after, m.until, m.left.queues, m.left.groups}
	if action == "history" {
		args = []any{m.queues, m.kinds, m.groups, m.after, m.until}
	}
	return rowsRead(ctx, c, bulkSettings, sql, args, taskTables)
}

// CountRowsRead runs the count of the tasks that match filter, as Count runs
// it, under EXPLAIN ANALYZE, and returns how many rows its scans of the task
// tables read.
func CountRowsRead(ctx context.Context, c *Client, filter *TaskFilter) (int64, error) {
	m, err := match(filter, allStates)
	if err != nil {
		return 0, err
	}
	states := m.states
	if m.history {
		states = append(states, string(StateCompleted))
	}
	return rowsRead(ctx, c, countSettings, c.sql.countTasks, []any{m.queues, m.kinds, m.groups, states}, taskTables)
}

// WindowRowsRead runs the statement that finds the window of a bulk action
// on every task, after task after, as the action runs it, under EXPLAIN
// ANALYZE, and returns how many rows the scans of the task tables read. With
// generic, the statement runs with its generic plan.
func WindowRowsRead(ctx context.Context, c *Client, after int64, generic bool) (int64, error) {
	var last int64
	if err := c.pool.QueryRow(ctx, c.sql.lastTask).Scan(&last); err != nil {
		return 0, err
	}
	plan, err := analyze(ctx, c, generic, windowSettings, c.sql.windowEnd, after, last, true, true)
	return planSum(plan, scanned(taskTables)), err
}

// taskTables matches the tables of tasks: the unfinished ones, and the
// partitions of the history.
func taskTables(rel string) bool {
	return rel == "tasks" || strings.HasPrefix(rel, "task_history")
}

// ListRowsRead runs the read of a page of tasks on params, as List runs it,
// under EXPLAIN ANALYZE, and returns how many rows its scans of the task
// tables read.
func ListRowsRead(ctx context.Context, c *Client, params *ListParams) (int64, error) {
	limit, before, err := listBounds(params)
	if err != nil {
		return 0, err
	}
	m, err := match(&params.Filter, allStates)
	if err != nil {
		return 0, err
	}
	args := []any{m.queues, m.states, m.kinds, before, limit + 1, len(m.states) > 0, m.history, m.groups}
	return rowsRead(ctx, c, "SET LOCAL plan_cache_mode = force_custom_plan", c.sql.listTasks, args, taskTables)
}

// GroupRowsRead runs the read of a page of groups on params, as Groups runs
// it, under EXPLAIN ANALYZE, and returns how many rows its scans of groups
// read.
func GroupRowsRead(ctx context.Context, c *Client, params *GroupParams) (int64, error) {
	sql, args, err := c.groupsQuery(params)
	if err != nil {
		return 0, err
	}
	return rowsRead(ctx, c, groupsSettings, sql, args, func(rel string) bool { return rel == "groups" })
}

// RetentionRowsRead runs the statement that deletes a batch of the dead and
// canceled tasks older than the dead retention, under EXPLAIN ANALYZE in a
// transaction that it rolls back, and returns how many rows its scans of
// tasks read.
func RetentionRowsRead(ctx context.Context, c *Client) (int64, error) {
	return rowsRead(ctx, c, "", c.sql.deleteFinished, nil, func(rel string) bool { return rel == "tasks" })
}

// rowsRead runs sql after settings under EXPLAIN ANALYZE, in a transaction
// that it rolls back, and returns how many rows the scans of the relations
// that match read.
func rowsRead(ctx context.Context, c *Client, settings, sql string, args []any, match func(rel string) bool) (int64, error) {
	plan, err := analyze(ctx, c, false, settings, sql, args...)
	return planSum(plan, scanned(match)), err
}

// analyze runs sql after settings, if any, under EXPLAIN ANALYZE, in a
// transaction that it rolls back, and returns its plan. With generic, sql
// runs with its generic plan: the plan that a connection keeps for a
// statement once it stops planning it for each call's arguments, which it
// does after five calls if that plan looks no costlier.
func analyze(ctx context.Context, c *Client, generic bool, settings, sql string, args ...any) (map[string]any, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Only a prepared statement has a generic plan, and EXECUTE takes its
	// arguments as text, so pgx writes them into the statement. A prepared
	// statement outlives its transaction, on a connection that others use
	// next: it gets the schema's name, which is unique, and is dropped.
	setup, query, name := []string{settings}, "EXPLAIN (ANALYZE, FORMAT JSON) "+sql, c.schema+"_generic"
	if generic {
		params := make([]string, len(args))
		for i := range args {
			params[i] = "$" + strconv.Itoa(i+1)
		}
		setup = append(setup, "SET LOCAL plan_cache_mode = force_generic_plan", "PREPARE "+name+" AS "+sql)
		query = "EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE " + name + "(" + strings.Join(params, ", ") + ")"
		args = append([]any{pgx.QueryExecModeSimpleProtocol}, args...)
	}
	for _, s := range setup {
		if s == "" {
			continue
		}
		if _, err := tx.Exec(ctx, s); err != nil {
			return nil, err
		}
	}
	var plan []map[string]any
	if err := tx.QueryRow(ctx, query, args...).Scan(&plan); err != nil {
		return nil, err
	}
	if generic {
		if _, err := tx.Exec(ctx, "DEALLOCATE "+name); err != nil {
			return nil, err
		}
	}
	root, _ := plan[0]["Plan"].(map[string]any)
	return root, nil
}

// scanned returns a count for planSum: the rows that a node read, if it
// scans a relation that matches.
func scanned(match func(rel string) bool) func(node map[string]any) float64 {
	return func(node map[string]any) float64 {
		var n float64
		if rel, _ := node["Relation Name"].(string); match(rel) {
			for _, key := range []string{"Actual Rows", "Rows Removed by Filter", "Rows Removed by Index Recheck"} {
				v, _ := node[key].(float64)
				n += v
			}
		}
		return n
	}
}

// planSum adds up count, times the node's loops, over the nodes of plan.
func planSum(plan map[string]any, count func(node map[string]any) float64) int64 {
	loops, _ := plan["Actual Loops"].(float64)
	n := int64(count(plan) * loops)
	children, _ := plan["Plans"].([]any)
	for _, child := range children {
		if c, ok := child.(map[string]any); ok {
			n += planSum(c, count)
		}
	}
	return n
}

// StoredSize returns at most the size of JSON text as PostgreSQL writes it
// out of jsonb, as the payload limit counts it.
func StoredSize(text []byte) int {
	size, _ := storedSize(text)
	return size
}
