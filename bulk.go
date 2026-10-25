package taskmill

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
)

// countCap is the most tasks that [Client.Count] counts in one state.
const countCap = 10_000

// TaskFilter selects tasks for [Client.Count] and the bulk actions, by the
// same filters as [ListParams]: a task matches if it matches every filter
// that is set.
type TaskFilter struct {
	// Queues limits the tasks to these queues. Empty means all queues.
	Queues []string

	// States limits the tasks to these states. Empty means all states.
	States []State

	// Kinds limits the tasks to these kinds. Empty means all kinds.
	Kinds []string

	// GroupKeys limits the tasks to those of these FIFO groups. Empty means
	// all tasks.
	GroupKeys []string
}

// Every state, and the states that each bulk action applies to: those that
// the action on a single task allows.
var (
	allStates = []State{
		StateScheduled, StatePending, StateAvailable, StateRunning,
		StateRetryable, StateCompleted, StateDead, StateCanceled,
	}
	retryStates  = []State{StateDead, StateCanceled}
	cancelStates = []State{StateScheduled, StatePending, StateAvailable, StateRunning, StateRetryable}
	deleteStates = slices.DeleteFunc(slices.Clone(allStates), func(s State) bool { return s == StateRunning })
)

// matching is a checked filter, as the arguments of the statements.
type matching struct {
	queues, kinds, groups []string
	states                []string  // the selected states, except completed
	history               bool      // completed tasks are selected
	start                 time.Time // when the bulk action started
}

// match checks filter, and selects those of states that it allows. A nil
// filter matches every task.
func match(filter *TaskFilter, states []State) (*matching, error) {
	if filter == nil {
		filter = &TaskFilter{}
	}
	var m matching
	var err error
	if m.queues, err = nonEmpty("queue name", filter.Queues); err != nil {
		return nil, err
	}
	if m.kinds, err = nonEmpty("kind", filter.Kinds); err != nil {
		return nil, err
	}
	if m.groups, err = nonEmpty("group key", filter.GroupKeys); err != nil {
		return nil, err
	}
	for _, s := range filter.States {
		if !s.valid() {
			return nil, invalidArgument("unknown state %q", s)
		}
	}
	m.states = []string{}
	for _, s := range states {
		switch {
		case len(filter.States) > 0 && !slices.Contains(filter.States, s):
		case s == StateCompleted:
			m.history = true
		default:
			m.states = append(m.states, string(s))
		}
	}
	return &m, nil
}

// Count returns how many tasks match filter in each of its states, or in
// every state if it selects none. filter may be nil. Counting stops at
// 10,000 tasks per state, so 10,000 means 10,000 or more. Like
// [Client.List], the query is canceled after 5 seconds.
func (c *Client) Count(ctx context.Context, filter *TaskFilter) (map[State]int64, error) {
	m, err := match(filter, allStates)
	if err != nil {
		return nil, err
	}
	states := m.states
	if m.history {
		states = append(states, string(StateCompleted))
	}
	out := make(map[State]int64, len(states))
	err = pgx.BeginFunc(ctx, c.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '"+listTimeout+"'"); err != nil {
			return err
		}
		// Custom plans leave out the branches of states not counted.
		if _, err := tx.Exec(ctx, "SET LOCAL plan_cache_mode = force_custom_plan"); err != nil {
			return err
		}
		r, err := tx.Query(ctx, c.sql.countTasks, m.queues, m.kinds, m.groups, states)
		if err != nil {
			return err
		}
		defer r.Close()
		for r.Next() {
			var state string
			var n int64
			if err := r.Scan(&state, &n); err != nil {
				return err
			}
			out[State(state)] = n
		}
		return r.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("taskmill: count tasks: %w", err)
	}
	return out, nil
}

// RetryMatching retries the dead and canceled tasks that match filter, as
// [Client.Retry] retries each one, and returns how many it retried. The
// filter must not be empty: to retry every dead task, set its States.
//
// It works in batches of 1,000 tasks, each in a short transaction of its
// own, and skips the tasks and the FIFO groups that other transactions hold
// at that moment; run it again to retry those. It retries only tasks that
// finished before it started, so it retries a task at most once, even if
// the task fails again meanwhile. If ctx ends first, it returns how many it
// retried before, with an error; each batch commits whole or not at all.
func (c *Client) RetryMatching(ctx context.Context, filter *TaskFilter) (int64, error) {
	m, err := c.bulk(ctx, filter, retryStates)
	if err != nil {
		return 0, err
	}
	n, err := c.inBatches(ctx, func(q querier) (int, error) { return c.retryBatch(ctx, q, m) })
	if err != nil {
		return n, fmt.Errorf("taskmill: retry tasks: %w", err)
	}
	return n, nil
}

// CancelMatching cancels the unfinished tasks that match filter, as
// [Client.Cancel] cancels each one, and returns how many it canceled,
// counting running tasks, which become canceled when their attempts end. It
// works like [Client.RetryMatching], and leaves alone the tasks enqueued
// after it started.
func (c *Client) CancelMatching(ctx context.Context, filter *TaskFilter) (int64, error) {
	m, err := c.bulk(ctx, filter, cancelStates)
	if err != nil {
		return 0, err
	}
	n, err := c.inBatches(ctx, func(q querier) (int, error) { return c.cancelBatch(ctx, q, m) })
	if err != nil {
		return n, fmt.Errorf("taskmill: cancel tasks: %w", err)
	}
	return n, nil
}

// DeleteMatching deletes the tasks that match filter, except running ones,
// as [Client.Delete] deletes each one, completed tasks in history included,
// and returns how many it deleted. It works like [Client.RetryMatching], and
// leaves alone the tasks enqueued after it started.
func (c *Client) DeleteMatching(ctx context.Context, filter *TaskFilter) (int64, error) {
	m, err := c.bulk(ctx, filter, deleteStates)
	if err != nil {
		return 0, err
	}
	n, err := c.inBatches(ctx, func(q querier) (int, error) { return c.deleteBatch(ctx, q, m) })
	if err == nil && m.history {
		var completed int64
		completed, err = c.inBatches(ctx, func(q querier) (int, error) {
			n, err := q.exec(ctx, c.sql.deleteCompleted, m.queues, m.kinds, m.groups, m.start)
			return int(n), err
		})
		n += completed
	}
	if err != nil {
		return n, fmt.Errorf("taskmill: delete tasks: %w", err)
	}
	return n, nil
}

// bulk checks the filter of a bulk action that applies to states, and
// notes when the action starts.
func (c *Client) bulk(ctx context.Context, filter *TaskFilter, states []State) (*matching, error) {
	if filter == nil || len(filter.Queues)+len(filter.States)+len(filter.Kinds)+len(filter.GroupKeys) == 0 {
		return nil, invalidArgument("the filter must not be empty")
	}
	m, err := match(filter, states)
	if err != nil {
		return nil, err
	}
	if err := c.pool.QueryRow(ctx, "SELECT now()").Scan(&m.start); err != nil {
		return nil, fmt.Errorf("taskmill: read the time: %w", err)
	}
	return m, nil
}

// inBatches runs batch, each time in a transaction of its own, until it
// changes less than a full batch, and returns how many tasks it changed.
func (c *Client) inBatches(ctx context.Context, batch func(q querier) (int, error)) (int64, error) {
	var total int64
	for {
		var n int
		err := c.inTx(ctx, func(q querier) error {
			// Custom plans see which filters are set, and so use the indexes
			// of the states that they select.
			if _, err := q.exec(ctx, "SET LOCAL plan_cache_mode = force_custom_plan"); err != nil {
				return err
			}
			var err error
			n, err = batch(q)
			return err
		})
		if err != nil {
			return total, err
		}
		total += int64(n)
		if n < maintenanceBatch {
			return total, nil
		}
	}
}

// bulkTask is a task that a bulk action picked, and holds the lock of.
type bulkTask struct {
	id           int64
	state        State
	queue, group string
}

// pick locks a batch of the tasks that match m, with one of the pick
// statements.
func pick(ctx context.Context, q querier, sql string, m *matching) ([]bulkTask, error) {
	r, err := q.query(ctx, sql, m.queues, m.kinds, m.groups, m.states, m.start)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var out []bulkTask
	for r.Next() {
		var t bulkTask
		var state string
		if err := r.Scan(&t.id, &state, &t.queue, &t.group); err != nil {
			return nil, err
		}
		t.state = State(state)
		out = append(out, t)
	}
	return out, r.Err()
}

func idsOf(ts []bulkTask) []int64 {
	ids := make([]int64, len(ts))
	for i, t := range ts {
		ids[i] = t.id
	}
	return ids
}

// retryBatch retries a batch of the tasks that match m.
func (c *Client) retryBatch(ctx context.Context, q querier, m *matching) (int, error) {
	if len(m.states) == 0 {
		return 0, nil
	}
	picked, err := pick(ctx, q, c.sql.pickRetry, m)
	if err != nil || len(picked) == 0 {
		return 0, err
	}
	pending, err := c.rejoinGroups(ctx, q, picked)
	if err != nil {
		return 0, err
	}
	var available []string
	for _, t := range picked {
		if !slices.Contains(pending, t.id) {
			available = append(available, t.queue)
		}
	}
	_, err = q.exec(ctx, c.sql.retryTasks, idsOf(picked), pending, c.schema, c.notifiable(ctx, available))
	return len(picked), err
}

// rejoinGroups is rejoinGroup for the grouped tasks among ts, with a few
// statements for all of them, and returns those that must wait in pending.
// A dead head that halts its group resumes it; the oldest task of a group
// that has no unfinished tasks becomes its head; the others wait behind
// their groups' heads.
func (c *Client) rejoinGroups(ctx context.Context, q querier, ts []bulkTask) ([]int64, error) {
	resumed, err := c.releaseHalts(ctx, q, ts)
	if err != nil {
		return nil, err
	}
	joining := make(map[[2]string][]int64) // by group, the tasks that enter it
	var set groupSet
	for _, t := range ts {
		key := [2]string{t.queue, t.group}
		if t.group == "" || resumed[t.id] {
			continue
		}
		if joining[key] == nil {
			set.add(t.queue, t.group)
		}
		joining[key] = append(joining[key], t.id)
	}
	pending := []int64{} // not nil: the statement compares with every element
	if len(set.queues) == 0 {
		return pending, nil
	}
	counts := make([]int, len(set.queues))
	for i := range set.queues {
		counts[i] = len(joining[[2]string{set.queues[i], set.groups[i]}])
	}
	r, err := q.query(ctx, c.sql.enterGroups, set.queues, set.groups, counts)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	for r.Next() {
		var key [2]string
		var tasks int
		if err := r.Scan(&key[0], &key[1], &tasks); err != nil {
			return nil, err
		}
		ids := joining[key]
		head := int64(0)
		if tasks == len(ids) { // the group had no unfinished tasks
			head = slices.Min(ids)
		}
		for _, id := range ids {
			if id != head {
				pending = append(pending, id)
			}
		}
	}
	return pending, r.Err()
}

// cancelBatch cancels a batch of the tasks that match m. Waiting tasks leave
// their FIFO groups now, and running ones when their attempts end.
func (c *Client) cancelBatch(ctx context.Context, q querier, m *matching) (int, error) {
	if len(m.states) == 0 {
		return 0, nil
	}
	picked, err := pick(ctx, q, c.sql.pickCancel, m)
	if err != nil || len(picked) == 0 {
		return 0, err
	}
	if _, err := q.exec(ctx, c.sql.cancelTasks, idsOf(picked), c.schema); err != nil {
		return 0, err
	}
	err = c.leaveAll(ctx, q, picked, func(t bulkTask) bool { return t.state != StateRunning })
	return len(picked), err
}

// deleteBatch deletes a batch of the unfinished, dead, and canceled tasks
// that match m. Unfinished tasks leave their FIFO groups, and so does a dead
// head that halts its group, which resumes it.
func (c *Client) deleteBatch(ctx context.Context, q querier, m *matching) (int, error) {
	if len(m.states) == 0 {
		return 0, nil
	}
	picked, err := pick(ctx, q, c.sql.pickDelete, m)
	if err != nil || len(picked) == 0 {
		return 0, err
	}
	halting, err := c.releaseHalts(ctx, q, picked)
	if err != nil {
		return 0, err
	}
	if _, err := q.exec(ctx, c.sql.deleteTasks, idsOf(picked)); err != nil {
		return 0, err
	}
	err = c.leaveAll(ctx, q, picked, func(t bulkTask) bool { return !t.state.Finished() || halting[t.id] })
	return len(picked), err
}

// releaseHalts ends the halts of the FIFO groups that dead tasks among ts
// halt, and returns those tasks.
func (c *Client) releaseHalts(ctx context.Context, q querier, ts []bulkTask) (map[int64]bool, error) {
	var dead []int64
	for _, t := range ts {
		if t.group != "" && t.state == StateDead {
			dead = append(dead, t.id)
		}
	}
	halting := make(map[int64]bool)
	if len(dead) == 0 {
		return halting, nil
	}
	r, err := q.query(ctx, c.sql.releaseHalts, dead)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	for r.Next() {
		var id int64
		if err := r.Scan(&id); err != nil {
			return nil, err
		}
		halting[id] = true
	}
	return halting, r.Err()
}

// leaveAll updates the FIFO groups of the tasks among ts that leave them,
// those for which leaves is true, after the tasks changed.
func (c *Client) leaveAll(ctx context.Context, q querier, ts []bulkTask, leaves func(bulkTask) bool) error {
	var left []groupLeave
	index := make(map[[2]string]int)
	for _, t := range ts {
		if t.group == "" || !leaves(t) {
			continue
		}
		key := [2]string{t.queue, t.group}
		i, ok := index[key]
		if !ok {
			i = len(left)
			index[key] = i
			left = append(left, groupLeave{queue: t.queue, group: t.group})
		}
		left[i].tasks++
		left[i].head = left[i].head || t.state != StatePending
	}
	if len(left) == 0 {
		return nil
	}
	return c.leaveGroups(ctx, q, left)
}
