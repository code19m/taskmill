package taskmill

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	countCap = 10_000 // most tasks that Client.Count counts in one state

	// batchTimeout bounds a batch of a bulk action, which goes on when ctx
	// ends meanwhile: a batch that began ends whole, so that the count of
	// changed tasks is exact, and no batch is left behind, on a connection
	// that ctx closed, to hold its rows after the action returns.
	batchTimeout = 5 * time.Second
)

// bulkSettings start each batch of a bulk action. Custom plans see which
// filters are set, and so use the indexes of the states they select. The
// lock timeout bounds the one wait that a batch can meet, far below the
// deadlock timeout, so that a batch never holds other transactions up for
// long, and never takes part in a deadlock.
const bulkSettings = `SELECT set_config('plan_cache_mode', 'force_custom_plan', true), set_config('lock_timeout', '100ms', true)`

// TaskFilter selects tasks by queue, state, kind, and FIFO group, for
// [Client.List], [Client.Count], and the bulk actions: a task matches if it
// matches every filter that is set.
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

// empty reports whether f sets none of the filters that select tasks by
// what they are.
func (f *TaskFilter) empty() bool {
	return f == nil || len(f.Queues)+len(f.States)+len(f.Kinds)+len(f.GroupKeys) == 0
}

// TaskCount is a number of tasks, from a count that stops at a cap.
type TaskCount struct {
	N int64

	// Capped reports that counting stopped at its cap with more tasks left,
	// so that N is a lower bound.
	Capped bool
}

// TaskCounts counts the tasks that match a filter.
type TaskCounts struct {
	// States holds how many of them are in each state that the filter
	// selects.
	States map[State]TaskCount

	// Retry, Cancel, and Delete are how many of them [Client.RetryMatching],
	// [Client.CancelMatching], and [Client.DeleteMatching] would change.
	Retry, Cancel, Delete TaskCount
}

// BulkParams selects the tasks of a bulk action, such as
// [Client.RetryMatching].
type BulkParams struct {
	// Filter selects the tasks. It must not be empty.
	Filter TaskFilter

	// Cursor goes on with an action that stopped early, from its
	// [BulkResult.Next].
	Cursor string
}

// BulkResult reports what a bulk action did.
type BulkResult struct {
	// Changed is how many tasks it changed.
	Changed int64

	// Skipped is how many matching tasks it left as they were, because other
	// transactions held them, their FIFO groups, or their dedupe keys at
	// that moment. To keep a group in order, a retry also leaves the later
	// tasks of a group it left. Run the action again to act on them.
	Skipped int64

	// Next goes on with the action, as [BulkParams.Cursor], if it stopped
	// before it went through every task: at the first task it skipped, if it
	// skipped any, so that a group it left comes back in order. Empty if it
	// went through every task.
	Next string
}

// The states, and those that each bulk action applies to.
var (
	allStates = []State{
		StateScheduled, StatePending, StateAvailable, StateRunning,
		StateRetryable, StateCompleted, StateDead, StateCanceled,
	}
	retryStates  = slices.DeleteFunc(slices.Clone(allStates), func(s State) bool { return !s.CanRetry() })
	cancelStates = slices.DeleteFunc(slices.Clone(allStates), func(s State) bool { return !s.CanCancel() })
	deleteStates = slices.DeleteFunc(slices.Clone(allStates), func(s State) bool { return !s.CanDelete() })
)

// matching is a checked filter, as the arguments of the statements, and
// how far a bulk action got.
type matching struct {
	queues, kinds, groups []string
	states                []string // the selected states, except completed
	history               bool     // completed tasks are selected
	after, last           int64    // the action goes on after task after, up to task last
	left                  groupSet // the groups that a retry left, and whose later tasks it leaves
}

// match checks filter, and selects those of states that it allows. A nil
// filter matches every task.
func match(filter *TaskFilter, states []State) (*matching, error) {
	if filter == nil {
		filter = &TaskFilter{}
	}
	m := matching{left: groupSet{queues: []string{}, groups: []string{}}}
	var err error
	if m.queues, err = checkFilter("queue name", filter.Queues, maxNameLength); err != nil {
		return nil, err
	}
	if m.kinds, err = checkFilter("kind", filter.Kinds, maxNameLength); err != nil {
		return nil, err
	}
	if m.groups, err = checkFilter("group key", filter.GroupKeys, maxGroupKeyLength); err != nil {
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

// Count counts the tasks that match filter, which may be nil. It counts at
// most 10,000 tasks per state. Like [Client.List], the query is canceled
// after 5 seconds.
func (c *Client) Count(ctx context.Context, filter *TaskFilter) (*TaskCounts, error) {
	m, err := match(filter, allStates)
	if err != nil {
		return nil, err
	}
	states := m.states
	if m.history {
		states = append(states, string(StateCompleted))
	}
	out := &TaskCounts{States: make(map[State]TaskCount, len(states))}
	err = pgx.BeginFunc(ctx, c.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '"+listTimeout+"'"); err != nil {
			return err
		}
		// Custom plans leave out the branches of the states not counted.
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
			var n, unflagged int64
			if err := r.Scan(&state, &n, &unflagged); err != nil {
				return err
			}
			out.add(State(state), n, unflagged)
		}
		return r.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("taskmill: count tasks: %w", err)
	}
	return out, nil
}

// add counts n tasks in state s, of which unflagged were not asked to stop;
// n and unflagged count one more than the cap if counting stopped there.
func (c *TaskCounts) add(s State, n, unflagged int64) {
	count := TaskCount{N: min(n, countCap), Capped: n > countCap}
	c.States[s] = count
	if s.CanRetry() {
		c.Retry.plus(count)
	}
	// Cancel skips running tasks already asked to stop.
	if s.CanCancel() {
		c.Cancel.plus(TaskCount{N: min(unflagged, countCap), Capped: unflagged > countCap})
	}
	if s.CanDelete() {
		c.Delete.plus(count)
	}
}

func (c *TaskCount) plus(other TaskCount) {
	c.N += other.N
	c.Capped = c.Capped || other.Capped
}

// RetryMatching retries the dead and canceled tasks that match the filter of
// params, as [Client.Retry] retries each one. The filter must not be empty:
// to retry every dead task, set its States.
//
// It goes through the tasks enqueued before it started, oldest first, in
// batches of 1,000, each in a short transaction of its own. It reads each
// task once, so it retries a task at most once, even if the task fails again
// meanwhile, and it ends however fast new tasks arrive. It skips the tasks,
// the FIFO groups, and the dedupe keys that other transactions hold at that
// moment, never waiting while it holds rows. If ctx ends first, it finishes
// the batch it is in, for at most 5 seconds, and returns what it did, with an
// error and [BulkResult.Next] to go on; each batch commits whole or not at
// all.
func (c *Client) RetryMatching(ctx context.Context, params *BulkParams) (BulkResult, error) {
	m, err := checkBulk(params, retryStates)
	if err != nil {
		return BulkResult{}, err
	}
	res, err := c.inBatches(ctx, m, c.retryBatch)
	if err != nil {
		return res, fmt.Errorf("taskmill: retry tasks: %w", err)
	}
	return res, nil
}

// CancelMatching cancels the unfinished tasks that match the filter of
// params, as [Client.Cancel] cancels each one, counting running tasks, which
// become canceled when their attempts end. It skips running tasks already
// asked to stop, and otherwise works like [Client.RetryMatching].
func (c *Client) CancelMatching(ctx context.Context, params *BulkParams) (BulkResult, error) {
	m, err := checkBulk(params, cancelStates)
	if err != nil {
		return BulkResult{}, err
	}
	res, err := c.inBatches(ctx, m, c.cancelBatch)
	if err != nil {
		return res, fmt.Errorf("taskmill: cancel tasks: %w", err)
	}
	return res, nil
}

// DeleteMatching deletes the tasks that match the filter of params, except
// running ones, as [Client.Delete] deletes each one, completed tasks in
// history included. It works like [Client.RetryMatching].
func (c *Client) DeleteMatching(ctx context.Context, params *BulkParams) (BulkResult, error) {
	m, err := checkBulk(params, deleteStates)
	if err != nil {
		return BulkResult{}, err
	}
	res, err := c.inBatches(ctx, m, c.deleteBatch)
	if err != nil {
		return res, fmt.Errorf("taskmill: delete tasks: %w", err)
	}
	return res, nil
}

// checkBulk checks the params of a bulk action that applies to states.
func checkBulk(params *BulkParams, states []State) (*matching, error) {
	if params == nil || params.Filter.empty() {
		return nil, invalidArgument("the filter must not be empty")
	}
	m, err := match(&params.Filter, states)
	if err != nil {
		return nil, err
	}
	if params.Cursor != "" {
		if m.after, err = decodeCursor(params.Cursor); err != nil || m.after < 0 {
			return nil, invalidArgument("invalid cursor")
		}
	}
	return m, nil
}

// step is what a batch of a bulk action did.
type step struct {
	end              int64    // the batch went through the tasks up to this one
	changed, skipped int      // tasks
	firstSkipped     int64    // the first task it skipped, or 0
	left             groupSet // groups that a retry left
}

// inBatches runs batch, each time in a transaction of its own, until it goes
// through the last task. A batch that began goes on if ctx ends meanwhile,
// for at most batchTimeout, so that the result is exact.
func (c *Client) inBatches(ctx context.Context, m *matching, batch func(context.Context, querier, *matching) (step, error)) (BulkResult, error) {
	var res BulkResult
	var firstSkipped int64
	// stop returns what the action did when it stops early, with where to go
	// on: before the first task it skipped, so that the next run takes that
	// task first, or else after the last task it went through.
	stop := func(err error) (BulkResult, error) {
		res.Next = encodeCursor(m.after)
		if firstSkipped > 0 {
			res.Next = encodeCursor(firstSkipped - 1)
		}
		return res, err
	}
	// The action goes up to the last task enqueued before it starts.
	if err := c.pool.QueryRow(ctx, c.sql.lastTask).Scan(&m.last); err != nil {
		return stop(err)
	}
	for m.after < m.last {
		if err := ctx.Err(); err != nil {
			return stop(err)
		}
		s, err := c.runBatch(ctx, m, batch)
		if err != nil {
			return stop(err)
		}
		m.after = s.end
		m.left.queues, m.left.groups = append(m.left.queues, s.left.queues...), append(m.left.groups, s.left.groups...)
		res.Changed += int64(s.changed)
		res.Skipped += int64(s.skipped)
		if firstSkipped == 0 {
			firstSkipped = s.firstSkipped
		}
	}
	return res, nil
}

// runBatch runs batch in a transaction of its own. Once the transaction
// began, the batch goes on if ctx ends meanwhile, for at most batchTimeout.
func (c *Client) runBatch(ctx context.Context, m *matching, batch func(context.Context, querier, *matching) (step, error)) (step, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return step{}, err
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), batchTimeout)
	defer cancel()
	var s step
	if _, err = tx.Exec(ctx, bulkSettings); err == nil {
		s, err = batch(ctx, pgxQuerier{q: tx}, m)
	}
	if err != nil {
		_ = tx.Rollback(ctx) // err says what went wrong
		return step{}, err
	}
	return s, tx.Commit(ctx)
}

// bulkTask is a task that a bulk action read.
type bulkTask struct {
	id           int64
	queue, group string
	held         bool  // the action holds its lock, and it still matches
	state        State // if held
	inGroup      bool  // if held: its group has a row, which the action holds
}

// pick reads the next batch of the tasks that match m, with one of the pick
// statements, and locks those it can.
func pick(ctx context.Context, q querier, sql string, m *matching) ([]bulkTask, error) {
	r, err := q.query(ctx, sql, m.queues, m.kinds, m.groups, m.states, m.after, m.last, m.left.queues, m.left.groups)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var out []bulkTask
	for r.Next() {
		var t bulkTask
		var state string
		if err := r.Scan(&t.id, &t.queue, &t.group, &t.held, &state, &t.inGroup); err != nil {
			return nil, err
		}
		t.state = State(state)
		out = append(out, t)
	}
	return out, r.Err()
}

// end returns the last task that a batch with ids goes through: the last one
// it read, or the last of the action if it read less than a full batch.
func (m *matching) end(ids []int64) int64 {
	if len(ids) < maintenanceBatch {
		return m.last
	}
	return ids[len(ids)-1]
}

func idsOf(ts []bulkTask) []int64 {
	ids := make([]int64, len(ts))
	for i, t := range ts {
		ids[i] = t.id
	}
	return ids
}

// held returns the tasks among ts that the batch holds, and counts the
// others as skipped.
func (s *step) held(ts []bulkTask) []bulkTask {
	var out []bulkTask
	for _, t := range ts {
		if t.held {
			out = append(out, t)
		} else {
			s.skip(t, nil)
		}
	}
	return out
}

// skip counts a task that the action leaves. A retry, which passes left,
// also leaves the task's group, recorded there, for the rest of the action.
func (s *step) skip(t bulkTask, left map[[2]string]bool) {
	s.skipped++
	if s.firstSkipped == 0 || t.id < s.firstSkipped {
		s.firstSkipped = t.id
	}
	if key := [2]string{t.queue, t.group}; left != nil && t.group != "" && !left[key] {
		left[key] = true
		s.left.add(t.queue, t.group)
	}
}

// retryBatch retries a batch of the tasks that match m. Once it leaves a
// task of a FIFO group, it leaves the group's later tasks too, for the rest
// of the action, so that a later run retries the group in order.
func (c *Client) retryBatch(ctx context.Context, q querier, m *matching) (step, error) {
	if len(m.states) == 0 {
		return step{end: m.last}, nil
	}
	ts, err := pick(ctx, q, c.sql.pickRetry, m)
	if err != nil {
		return step{}, err
	}
	s := step{end: m.end(idsOf(ts))}
	left := make(map[[2]string]bool)
	for i := range m.left.queues {
		left[[2]string{m.left.queues[i], m.left.groups[i]}] = true
	}
	var held []bulkTask
	for _, t := range ts {
		if t.held && (t.group == "" || !left[[2]string{t.queue, t.group}]) {
			held = append(held, t)
		} else {
			s.skip(t, left)
		}
	}
	if len(held) == 0 {
		return s, nil
	}
	retried, pending, err := c.rejoinGroups(ctx, q, held)
	if err != nil {
		return step{}, err
	}
	for _, t := range held {
		if !slices.ContainsFunc(retried, func(r bulkTask) bool { return r.id == t.id }) {
			s.skip(t, left)
		}
	}
	var available []string
	for _, t := range retried {
		if !slices.Contains(pending, t.id) {
			available = append(available, t.queue)
		}
	}
	_, err = q.exec(ctx, c.sql.retryTasks, idsOf(retried), pending, c.schema, c.notifiable(ctx, available))
	s.changed = len(retried)
	return s, err
}

// rejoinGroups is rejoinGroup for a batch of tasks to retry, with a few
// statements for all of them. It returns the tasks to retry, and those of
// them that wait in pending. A dead head that halts its group resumes it.
// The other tasks of a group that has a row wait behind its head. A group
// that has no row gets one, and its oldest task becomes its head; if it
// cannot get one now, its tasks are left out.
func (c *Client) rejoinGroups(ctx context.Context, q querier, ts []bulkTask) (retried []bulkTask, pending []int64, err error) {
	resumed, err := c.releaseHalts(ctx, q, ts)
	if err != nil {
		return nil, nil, err
	}
	pending = []int64{} // not nil: the statement compares with every element
	for _, t := range ts {
		if t.group == "" || resumed[t.id] {
			retried = append(retried, t)
		}
	}
	counted, missing, byGroup := groupTasks(ts, resumed)
	if len(counted.queues) > 0 {
		if _, err := q.exec(ctx, c.sql.recountGroups, counted.queues, counted.groups, counted.sizes(byGroup)); err != nil {
			return nil, nil, err
		}
		for _, t := range counted.tasks(byGroup) {
			retried = append(retried, t)
			pending = append(pending, t.id)
		}
	}
	if len(missing.queues) == 0 {
		return retried, pending, nil
	}
	created, err := c.createGroups(ctx, q, missing, byGroup)
	if err != nil {
		return nil, nil, err
	}
	for _, key := range created {
		for i, t := range byGroup[key] {
			retried = append(retried, t)
			if i > 0 {
				pending = append(pending, t.id)
			}
		}
	}
	return retried, pending, nil
}

// groupTasks sorts the grouped tasks of ts, except those of skip, by group:
// the groups that have a row, and those that have none. A group's tasks
// stay in ID order, as picked.
func groupTasks(ts []bulkTask, skip map[int64]bool) (counted, missing groupSet, byGroup map[[2]string][]bulkTask) {
	byGroup = make(map[[2]string][]bulkTask)
	for _, t := range ts {
		if t.group == "" || skip[t.id] {
			continue
		}
		key := [2]string{t.queue, t.group}
		switch {
		case byGroup[key] != nil:
		case t.inGroup:
			counted.add(t.queue, t.group)
		default:
			missing.add(t.queue, t.group)
		}
		byGroup[key] = append(byGroup[key], t)
	}
	return counted, missing, byGroup
}

// createGroups creates the rows of groups s, which had none, and returns
// those it created. A concurrent enqueue may be creating the same row: the
// batch then waits for it for at most its lock timeout, and creates none.
func (c *Client) createGroups(ctx context.Context, q querier, s groupSet, byGroup map[[2]string][]bulkTask) ([][2]string, error) {
	if _, err := q.exec(ctx, "SAVEPOINT create_groups"); err != nil {
		return nil, err
	}
	var created [][2]string
	r, err := q.query(ctx, c.sql.createGroups, s.queues, s.groups, s.sizes(byGroup))
	if err == nil {
		for r.Next() {
			var key [2]string
			if err = r.Scan(&key[0], &key[1]); err != nil {
				break
			}
			created = append(created, key)
		}
		r.Close()
		if err == nil {
			err = r.Err()
		}
	}
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == "55P03" { // lock_not_available
		_, err = q.exec(ctx, "ROLLBACK TO SAVEPOINT create_groups")
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	_, err = q.exec(ctx, "RELEASE SAVEPOINT create_groups")
	return created, err
}

// tasks returns the tasks of byGroup in the groups of s.
func (s *groupSet) tasks(byGroup map[[2]string][]bulkTask) []bulkTask {
	var out []bulkTask
	for i := range s.queues {
		out = append(out, byGroup[[2]string{s.queues[i], s.groups[i]}]...)
	}
	return out
}

// sizes returns how many of the tasks of byGroup are in each group of s.
func (s *groupSet) sizes(byGroup map[[2]string][]bulkTask) []int {
	out := make([]int, len(s.queues))
	for i := range s.queues {
		out[i] = len(byGroup[[2]string{s.queues[i], s.groups[i]}])
	}
	return out
}

// cancelBatch cancels a batch of the tasks that match m. Waiting tasks leave
// their FIFO groups now, and running ones when their attempts end.
func (c *Client) cancelBatch(ctx context.Context, q querier, m *matching) (step, error) {
	if len(m.states) == 0 {
		return step{end: m.last}, nil
	}
	ts, err := pick(ctx, q, c.sql.pickCancel, m)
	if err != nil {
		return step{}, err
	}
	s := step{end: m.end(idsOf(ts))}
	held := s.held(ts)
	if len(held) == 0 {
		return s, nil
	}
	if _, err := q.exec(ctx, c.sql.cancelTasks, idsOf(held), c.schema); err != nil {
		return step{}, err
	}
	s.changed = len(held)
	return s, c.leaveAll(ctx, q, held, func(t bulkTask) bool { return t.state != StateRunning })
}

// deleteBatch deletes a batch of the tasks that match m, unfinished, dead,
// canceled, and completed in history. It reads the next batch of both
// tables, and goes up to the end of the shorter one, so that the two go on
// together. Unfinished tasks leave their FIFO groups, and so does a dead
// head that halts its group, which resumes it.
func (c *Client) deleteBatch(ctx context.Context, q querier, m *matching) (step, error) {
	var ts, completed []bulkTask
	var err error
	if len(m.states) > 0 {
		if ts, err = pick(ctx, q, c.sql.pickDelete, m); err != nil {
			return step{}, err
		}
	}
	if m.history {
		if completed, err = c.pickCompleted(ctx, q, m); err != nil {
			return step{}, err
		}
	}
	s := step{end: m.last}
	if len(m.states) > 0 {
		s.end = min(s.end, m.end(idsOf(ts)))
	}
	if m.history {
		s.end = min(s.end, m.end(idsOf(completed)))
	}
	// The tasks after the end come again in the next batch.
	beyond := func(t bulkTask) bool { return t.id > s.end }
	ts, completed = s.held(slices.DeleteFunc(ts, beyond)), s.held(slices.DeleteFunc(completed, beyond))
	if len(completed) > 0 {
		if _, err := q.exec(ctx, c.sql.deleteCompleted, idsOf(completed)); err != nil {
			return step{}, err
		}
	}
	s.changed = len(ts) + len(completed)
	if len(ts) == 0 {
		return s, nil
	}
	halting, err := c.releaseHalts(ctx, q, ts)
	if err != nil {
		return step{}, err
	}
	if _, err := q.exec(ctx, c.sql.deleteTasks, idsOf(ts)); err != nil {
		return step{}, err
	}
	return s, c.leaveAll(ctx, q, ts, func(t bulkTask) bool { return !t.state.Finished() || halting[t.id] })
}

// pickCompleted is pick for the completed tasks in history.
func (c *Client) pickCompleted(ctx context.Context, q querier, m *matching) ([]bulkTask, error) {
	r, err := q.query(ctx, c.sql.pickCompleted, m.queues, m.kinds, m.groups, m.after, m.last)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var out []bulkTask
	for r.Next() {
		t := bulkTask{state: StateCompleted}
		if err := r.Scan(&t.id, &t.held); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, r.Err()
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
