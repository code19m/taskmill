package taskmill

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	countCap = 10_000 // most tasks that Client.Count counts in one state

	// batchTimeout bounds a batch of a bulk action, which continues when ctx
	// ends meanwhile: a batch that began ends whole, so that the action
	// counts it, and no batch is left behind, on a connection that ctx
	// closed, to hold its rows after the action returns. The bound is the
	// client's: past it, the action gives the batch up and closes its
	// connection. If that happens while the batch commits, the database may
	// still commit it, and the action returns an error without its count.
	batchTimeout = 5 * time.Second

	// openTimeout bounds each statement of a batch that reads without a
	// window, and bulkWindow is the most tasks that a batch in a window
	// reads from a table, whether they match or not: see nextBatch. A
	// window of 100 batches costs little to find where most tasks match.
	openTimeout = batchTimeout / 5
	bulkWindow  = 100 * maintenanceBatch
)

// TaskFilter selects tasks by queue, state, kind, and FIFO group, for
// [Client.List], [Client.Count], and the bulk actions: a task matches if it
// matches every filter that is set. Each filter takes at most 100 values. A
// queue name or kind has at most 128 characters, and a group key at most
// 256. An unknown state, like a value that is empty or too long, or a filter
// with too many values, is an error that wraps [ErrInvalidArgument].
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
// [Client.RetryMatching]. Nil params, an empty filter, and a cursor that is
// not valid are errors that wrap [ErrInvalidArgument].
type BulkParams struct {
	// Filter selects the tasks. Required: it must set at least one field.
	Filter TaskFilter

	// Cursor continues an action that stopped early, from its
	// [BulkResult.Next]. Empty means from the oldest task.
	Cursor string
}

// BulkResult reports what a bulk action did.
type BulkResult struct {
	// Changed is how many tasks it changed. If the action returns an error,
	// the batch it was in may have committed all the same, and is not
	// counted: at most 1,000 tasks more.
	Changed int64

	// Skipped is how many matching tasks it left as they were, because other
	// transactions held them, their FIFO groups, or their dedupe keys at
	// that moment. To keep a group in order, a retry also leaves the later
	// tasks of a group it left. Run the action again to act on them.
	Skipped int64

	// Conflicts is how many canceled tasks a retry left as they were,
	// because another task holds their dedupe key: see [Client.Retry]. A
	// later run leaves them again while that task holds the key. Zero for
	// the other actions.
	Conflicts int64

	// Next continues the action, as [BulkParams.Cursor], if it stopped
	// before it went through every task: at the first task it skipped, if it
	// skipped any, so that a group it left comes back in order. Empty if it
	// went through every task.
	Next string
}

// The states that each bulk action applies to.
var (
	retryStates  = slices.DeleteFunc(slices.Clone(allStates), func(s State) bool { return !s.CanRetry() })
	cancelStates = slices.DeleteFunc(slices.Clone(allStates), func(s State) bool { return !s.CanCancel() })
	deleteStates = slices.DeleteFunc(slices.Clone(allStates), func(s State) bool { return !s.CanDelete() })
)

// filterArgs is a checked filter, as the arguments of the statements.
type filterArgs struct {
	queues, kinds, groups []string
	states                []string // the selected states, except completed
	history               bool     // completed tasks are selected
}

// bulkRun is a bulk action's filter, and how far the action got.
type bulkRun struct {
	filterArgs
	after, last int64    // the action continues after task after, up to task last
	windowed    bool     // the batches read in windows: one without a window ran out of time
	until       int64    // the batches read up to task until: last, or where their window ends
	left        groupSet // the groups that a retry left, and whose later tasks it leaves
}

// match checks filter, and selects those of states that it allows. A nil
// filter matches every task.
func match(filter *TaskFilter, states []State) (*filterArgs, error) {
	if filter == nil {
		filter = &TaskFilter{}
	}

	var f filterArgs
	var err error
	if f.queues, err = checkFilter("queue name", filter.Queues, maxNameLength); err != nil {
		return nil, err
	}
	if f.kinds, err = checkFilter("kind", filter.Kinds, maxNameLength); err != nil {
		return nil, err
	}
	if f.groups, err = checkFilter("group key", filter.GroupKeys, maxGroupKeyLength); err != nil {
		return nil, err
	}
	if len(filter.States) > maxFilterValues {
		return nil, invalidArgument("%d states, more than the limit of %d", len(filter.States), maxFilterValues)
	}
	for _, s := range filter.States {
		if !s.valid() {
			return nil, invalidArgument("unknown state %q", prefix(string(s)))
		}
	}

	f.states = []string{}
	for _, s := range states {
		switch {
		case len(filter.States) > 0 && !slices.Contains(filter.States, s):
		case s == StateCompleted:
			f.history = true
		default:
			f.states = append(f.states, string(s))
		}
	}
	return &f, nil
}

// CountParams selects the tasks that [Client.Count] counts.
type CountParams struct {
	// Filter selects the tasks. Empty means every task.
	Filter TaskFilter
}

// Count counts the tasks that params select. params may be nil, which
// counts every task. It counts at most 10,000 tasks per state. Like
// [Client.List], the query is canceled after 5 seconds.
func (c *Client) Count(ctx context.Context, params *CountParams) (*TaskCounts, error) {
	var filter *TaskFilter
	if params != nil {
		filter = &params.Filter
	}
	f, err := match(filter, allStates)
	if err != nil {
		return nil, err
	}

	states := f.states
	if f.history {
		states = append(states, string(StateCompleted))
	}

	out := &TaskCounts{States: make(map[State]TaskCount, len(states))}
	args := []any{
		f.queues, // $1
		f.kinds,  // $2
		f.groups, // $3
		states,   // $4
	}
	err = c.readAlone(ctx, countSettings, c.sql.countTasks, args, func(r pgx.Rows) error {
		var state string
		var n, unflagged int64
		if err := r.Scan(&state, &n, &unflagged); err != nil {
			return err
		}
		out.add(State(state), n, unflagged)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("taskmill: count tasks: %w", err)
	}
	return out, nil
}

// add counts the n tasks read in state s, of which unflagged were not asked
// to stop. n is one more than the cap if counting stopped there.
func (c *TaskCounts) add(s State, n, unflagged int64) {
	count := TaskCount{N: min(n, countCap), Capped: n > countCap}
	c.States[s] = count

	if s.CanRetry() {
		c.Retry.plus(count)
	}
	// Cancel skips running tasks already asked to stop. It is capped
	// whenever the state is, even if it counts fewer tasks than the cap:
	// the tasks that were not read may be ones that it would cancel.
	if s.CanCancel() {
		c.Cancel.plus(TaskCount{N: min(unflagged, countCap), Capped: count.Capped})
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
// meanwhile, and it ends however fast new tasks arrive. If a batch cannot
// reach its tasks within a second, because too many tasks that do not match
// lie before them, the action continues in batches that read at most 100,000
// tasks each, and so gets past those. It skips the tasks, the FIFO groups,
// and the dedupe keys that other transactions hold at that moment, never
// waiting while it holds rows. Like [Client.Retry], it gives a canceled task
// its dedupe key back, and leaves a task whose key another task holds: it
// counts those in [BulkResult.Conflicts], and returns no error for them. If
// ctx ends first, it finishes the batch it is in, for at most 5 seconds, and
// returns what it did, with an error and [BulkResult.Next] to continue; each
// batch commits whole or not at all.
//
// A call that returns an error may have done one batch more than it
// reports: if the commit of its last batch does not answer in those 5
// seconds, or the connection breaks, the database may still commit it.
// Running the action again, or continuing from Next, is safe: the tasks
// that it changed no longer match.
func (c *Client) RetryMatching(ctx context.Context, params *BulkParams) (BulkResult, error) {
	run, err := checkBulk(params, retryStates)
	if err != nil {
		return BulkResult{}, err
	}

	res, err := c.inBatches(ctx, run, c.retryBatch)
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
	run, err := checkBulk(params, cancelStates)
	if err != nil {
		return BulkResult{}, err
	}

	res, err := c.inBatches(ctx, run, c.cancelBatch)
	if err != nil {
		return res, fmt.Errorf("taskmill: cancel tasks: %w", err)
	}
	return res, nil
}

// DeleteMatching deletes the tasks that match the filter of params, except
// running ones, as [Client.Delete] deletes each one, completed tasks in
// history included. It works like [Client.RetryMatching].
func (c *Client) DeleteMatching(ctx context.Context, params *BulkParams) (BulkResult, error) {
	run, err := checkBulk(params, deleteStates)
	if err != nil {
		return BulkResult{}, err
	}

	res, err := c.inBatches(ctx, run, c.deleteBatch)
	if err != nil {
		return res, fmt.Errorf("taskmill: delete tasks: %w", err)
	}
	return res, nil
}

// checkBulk checks the params of a bulk action that applies to states.
func checkBulk(params *BulkParams, states []State) (*bulkRun, error) {
	if params == nil || params.Filter.empty() {
		return nil, invalidArgument("the filter must not be empty")
	}
	f, err := match(&params.Filter, states)
	if err != nil {
		return nil, err
	}

	run := &bulkRun{filterArgs: *f, left: groupSet{queues: []string{}, groups: []string{}}}
	if params.Cursor != "" {
		if run.after, err = decodeCursor(params.Cursor); err != nil || run.after < 0 {
			return nil, invalidArgument("invalid cursor")
		}
	}
	return run, nil
}

// step is what a batch of a bulk action did.
type step struct {
	end              int64    // the batch went through the tasks up to this one
	changed, skipped int      // tasks
	conflicts        int      // tasks that a retry left because other tasks hold their dedupe keys
	firstSkipped     int64    // the first task it skipped, or 0
	left             groupSet // groups that a retry left
}

// batchFunc does the work of one batch of a bulk action on the tasks that
// run matches, in the transaction of q.
type batchFunc func(ctx context.Context, q querier, run *bulkRun) (step, error)

// inBatches runs batch, each time in a transaction of its own, until it goes
// through the last task. A batch that began continues if ctx ends meanwhile,
// for at most batchTimeout, so that the result counts it.
func (c *Client) inBatches(ctx context.Context, run *bulkRun, batch batchFunc) (BulkResult, error) {
	var res BulkResult
	var firstSkipped int64

	// stop returns what the action did when it stops early, with where to
	// continue: before the first task it skipped, so that the next run takes that
	// task first, or else after the last task it went through.
	stop := func(err error) (BulkResult, error) {
		res.Next = encodeCursor(run.after)
		if firstSkipped > 0 {
			res.Next = encodeCursor(firstSkipped - 1)
		}
		return res, err
	}

	// The action goes up to the last task enqueued before it starts.
	if err := c.pool.QueryRow(ctx, c.sql.lastTask).Scan(&run.last); err != nil {
		return stop(err)
	}

	run.until = run.last
	for run.after < run.last {
		if err := ctx.Err(); err != nil {
			return stop(err)
		}
		s, err := c.nextBatch(ctx, run, batch)
		if err != nil {
			return stop(err)
		}

		run.after = s.end
		run.left.queues, run.left.groups = append(run.left.queues, s.left.queues...), append(run.left.groups, s.left.groups...)
		res.Changed += int64(s.changed)
		res.Skipped += int64(s.skipped)
		res.Conflicts += int64(s.conflicts)
		if firstSkipped == 0 {
			firstSkipped = s.firstSkipped
		}
	}
	return res, nil
}

// nextBatch runs the next batch of the action, within batchTimeout from when
// its first transaction began: the wait for a connection is not the batch's
// time.
//
// A batch first reads without a window. Once the database ends one of its
// statements after openTimeout, the action reads in windows from there on,
// and tries the batch again in one, also if ctx ended meanwhile: a batch in
// a window always moves the position. The page on operator actions says
// why.
func (c *Client) nextBatch(ctx context.Context, run *bulkRun, batch batchFunc) (step, error) {
	var ends time.Time
	if !run.windowed {
		s, err := c.runBulkBatch(ctx, run, batch, openSettings, &ends)
		// Before the transaction began, ends is not set.
		if pgCode(err) != codeQueryCanceled || ends.IsZero() {
			return s, err
		}

		run.windowed, run.until = true, run.after
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(context.WithoutCancel(ctx), ends)
		defer cancel()
	}

	if run.after >= run.until {
		if err := c.nextWindow(ctx, run); err != nil {
			return step{}, err
		}
	}
	return c.runBulkBatch(ctx, run, batch, bulkSettings, &ends)
}

// nextWindow sets the window of the next batches: the next bulkWindow tasks
// after the position, in each table that the action reads. The batches read
// no task beyond its end, run.until, and move the position there once they
// find less than a full batch before it.
//
// A window is counted in tasks, not in IDs, and its end only bounds what a
// batch reads, so the action still reads each task once.
//
// The statement runs in a transaction of its own with windowSettings, all in
// one round trip: see readAlone.
func (c *Client) nextWindow(ctx context.Context, run *bulkRun) error {
	args := []any{
		run.after,           // $1
		run.last,            // $2
		len(run.states) > 0, // $3
		run.history,         // $4
	}
	return c.readAlone(ctx, windowSettings, c.sql.windowEnd, args, func(r pgx.Rows) error {
		return r.Scan(&run.until)
	})
}

// runBulkBatch runs batch in a transaction of its own, which starts with
// settings and ends with liftLockTimeout. Once the transaction began, the
// batch continues if ctx ends meanwhile, until ends: batchTimeout from now,
// unless a transaction of the same batch set it before.
func (c *Client) runBulkBatch(ctx context.Context, run *bulkRun, batch batchFunc, settings string, ends *time.Time) (step, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return step{}, err
	}

	if ends.IsZero() {
		*ends = time.Now().Add(batchTimeout)
	}
	ctx, cancel := context.WithDeadline(context.WithoutCancel(ctx), *ends)
	defer cancel()

	var s step
	if _, err = tx.Exec(ctx, settings); err == nil {
		s, err = batch(ctx, pgxExecutor{q: tx}, run)
	}
	if err == nil {
		_, err = tx.Exec(ctx, liftLockTimeout)
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
	locked       bool      // the action holds its lock, and it still matches
	state        State     // if locked
	inGroup      bool      // if locked: its group has a row, which the action holds
	keyed        bool      // if locked: its cancel freed a dedupe key whose window still lasts
	finalized    time.Time // if completed: when it was
}

// pick reads the next batch of the tasks that run matches, with one of the pick
// statements, and locks those it can.
func pick(ctx context.Context, q querier, sql string, run *bulkRun) ([]bulkTask, error) {
	r, err := q.query(ctx, sql,
		run.queues,      // $1
		run.kinds,       // $2
		run.groups,      // $3
		run.states,      // $4
		run.after,       // $5
		run.until,       // $6
		run.left.queues, // $7
		run.left.groups, // $8
	)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var out []bulkTask
	for r.Next() {
		var t bulkTask
		var state string
		if err := r.Scan(&t.id, &t.queue, &t.group, &t.locked, &state, &t.inGroup, &t.keyed); err != nil {
			return nil, err
		}
		t.state = State(state)
		out = append(out, t)
	}
	return out, r.Err()
}

// end returns the last task that a batch with ids goes through: the last one
// it read, or the last one it may read if it read less than a full batch.
func (run *bulkRun) end(ids []int64) int64 {
	if len(ids) < maintenanceBatch {
		return run.until
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

// locked returns the tasks among ts that the batch holds, and counts the
// others as skipped.
func (s *step) locked(ts []bulkTask) []bulkTask {
	var out []bulkTask
	for _, t := range ts {
		if t.locked {
			out = append(out, t)
		} else {
			s.skip(t, nil)
		}
	}
	return out
}

// skip counts a task that the action leaves. A retry, which passes left,
// also leaves the task's group, recorded there, for the rest of the action.
func (s *step) skip(t bulkTask, left map[groupID]bool) {
	s.skipped++
	if s.firstSkipped == 0 || t.id < s.firstSkipped {
		s.firstSkipped = t.id
	}

	key := groupID{t.queue, t.group}
	if left != nil && t.group != "" && !left[key] {
		left[key] = true
		s.left.add(t.queue, t.group)
	}
}

// retryBatch retries a batch of the tasks that run matches. Once it leaves a
// task of a FIFO group, it leaves the group's later tasks too, for the rest
// of the action, so that a later run retries the group in order.
func (c *Client) retryBatch(ctx context.Context, q querier, run *bulkRun) (step, error) {
	if len(run.states) == 0 {
		return step{end: run.last}, nil
	}

	ts, err := pick(ctx, q, c.sql.pickRetry, run)
	if err != nil {
		return step{}, err
	}

	s := step{end: run.end(idsOf(ts))}
	left := make(map[groupID]bool)
	for i := range run.left.queues {
		left[groupID{run.left.queues[i], run.left.groups[i]}] = true
	}

	var locked []bulkTask
	for _, t := range ts {
		if t.locked && (t.group == "" || !left[groupID{t.queue, t.group}]) {
			locked = append(locked, t)
		} else {
			s.skip(t, left)
		}
	}

	// The keys come before the groups' counts, which cannot be undone for
	// a task that does not get its key.
	if locked, err = c.takeKeysBack(ctx, q, locked, &s, left); err != nil {
		return step{}, err
	}
	if len(locked) == 0 {
		return s, nil
	}

	retried, pending, err := c.rejoinGroups(ctx, q, locked)
	if err != nil {
		return step{}, err
	}

	if err := c.leaveOut(ctx, q, locked, retried, &s, left); err != nil {
		return step{}, err
	}

	var available []string
	for _, t := range retried {
		if !slices.Contains(pending, t.id) {
			available = append(available, t.queue)
		}
	}

	_, err = q.exec(ctx, c.sql.retryTasks,
		idsOf(retried),               // $1
		pending,                      // $2
		c.schema,                     // $3
		c.notifiable(ctx, available), // $4
	)
	s.changed = len(retried)
	return s, err
}

// leaveOut skips the tasks of locked that are not among retried: those that
// rejoinGroups left out. Such a task stays canceled, so it gives away again
// the dedupe key that it took back.
func (c *Client) leaveOut(ctx context.Context, q querier, locked, retried []bulkTask, s *step, left map[groupID]bool) error {
	var keyed []int64
	for _, t := range locked {
		if slices.ContainsFunc(retried, func(r bulkTask) bool { return r.id == t.id }) {
			continue
		}
		s.skip(t, left)
		if t.keyed {
			keyed = append(keyed, t.id)
		}
	}

	if len(keyed) == 0 {
		return nil
	}
	_, err := q.exec(ctx, c.sql.releaseKeys, keyed)
	return err
}

// takeKeysBack gives their dedupe keys back to the tasks of ts whose cancels
// freed them, and returns the tasks to retry. It leaves out a task whose key
// another task holds, and counts it as a conflict. A concurrent enqueue
// may be claiming one of the keys, or hold its row: the batch then waits for
// it for at most its lock timeout, takes no key, and leaves those tasks for
// a later run, with the later tasks of their groups. The tasks of ts have
// passed the groups that the action left before: a task ahead of one that
// was left is still retried, and so keeps the key that it takes here.
func (c *Client) takeKeysBack(ctx context.Context, q querier, ts []bulkTask, s *step, left map[groupID]bool) ([]bulkTask, error) {
	var keyed []int64
	for _, t := range ts {
		if t.keyed {
			keyed = append(keyed, t.id)
		}
	}
	if len(keyed) == 0 {
		return ts, nil
	}

	var refused map[int64]bool
	taken, err := briefly(ctx, q, func() (err error) {
		refused, err = c.takeKeys(ctx, q, keyed)
		return err
	})
	if err != nil {
		return nil, err
	}

	var out []bulkTask
	leftNow := make(map[groupID]bool) // the groups of the tasks that are left here
	for _, t := range ts {
		group := groupID{t.queue, t.group}
		switch {
		case t.keyed && !taken, t.group != "" && leftNow[group]:
			s.skip(t, left)
			leftNow[group] = true
		case refused[t.id]:
			s.conflicts++
		default:
			out = append(out, t)
		}
	}
	return out, nil
}

// briefly runs fn, a step of a batch that may wait for a row that another
// transaction is writing, under a savepoint. If fn gives up the wait, at
// the batch's lock timeout, briefly undoes what fn did and reports false:
// the batch continues without that step.
func briefly(ctx context.Context, q querier, fn func() error) (done bool, err error) {
	if _, err := q.exec(ctx, "SAVEPOINT brief"); err != nil {
		return false, err
	}

	err = fn()
	if pgCode(err) == codeLockNotAvailable {
		_, err = q.exec(ctx, "ROLLBACK TO SAVEPOINT brief")
		return false, err
	}
	if err != nil {
		return false, err
	}

	_, err = q.exec(ctx, "RELEASE SAVEPOINT brief")
	return true, err
}

// cancelBatch cancels a batch of the tasks that run matches. Waiting tasks leave
// their FIFO groups now, and running ones when their attempts end.
func (c *Client) cancelBatch(ctx context.Context, q querier, run *bulkRun) (step, error) {
	if len(run.states) == 0 {
		return step{end: run.last}, nil
	}

	ts, err := pick(ctx, q, c.sql.pickCancel, run)
	if err != nil {
		return step{}, err
	}

	s := step{end: run.end(idsOf(ts))}
	locked := s.locked(ts)
	if len(locked) == 0 {
		return s, nil
	}

	if _, err := q.exec(ctx, c.sql.cancelTasks, idsOf(locked), c.schema); err != nil {
		return step{}, err
	}

	s.changed = len(locked)
	return s, c.leaveAll(ctx, q, locked, func(t bulkTask) bool { return t.state != StateRunning })
}

// deleteBatch deletes a batch of the tasks that run matches, unfinished, dead,
// canceled, and completed in history. It reads the next batch of both
// tables, and goes up to the end of the shorter one, so that the two advance
// together. Unfinished tasks leave their FIFO groups, and so does a dead
// head that halts its group, which resumes it.
func (c *Client) deleteBatch(ctx context.Context, q querier, run *bulkRun) (step, error) {
	var ts, completed []bulkTask
	var err error
	if len(run.states) > 0 {
		if ts, err = pick(ctx, q, c.sql.pickDelete, run); err != nil {
			return step{}, err
		}
	}
	if run.history {
		if completed, err = c.pickCompleted(ctx, q, run); err != nil {
			return step{}, err
		}
	}

	s := step{end: run.last}
	if len(run.states) > 0 {
		s.end = min(s.end, run.end(idsOf(ts)))
	}
	if run.history {
		s.end = min(s.end, run.end(idsOf(completed)))
	}

	// The tasks after the end come again in the next batch. Of both tables,
	// a batch takes one batch of tasks in all, as BulkResult.Changed says.
	beyond := func(t bulkTask) bool { return t.id > s.end }
	ts, completed = slices.DeleteFunc(ts, beyond), slices.DeleteFunc(completed, beyond)
	if len(ts)+len(completed) > maintenanceBatch {
		s.end = slices.Sorted(slices.Values(append(idsOf(ts), idsOf(completed)...)))[maintenanceBatch-1]
		ts, completed = slices.DeleteFunc(ts, beyond), slices.DeleteFunc(completed, beyond)
	}
	ts, completed = s.locked(ts), s.locked(completed)

	if len(completed) > 0 {
		first, last := finalizedRange(completed)
		if _, err := q.exec(ctx, c.sql.deleteCompleted, idsOf(completed), first, last); err != nil {
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
func (c *Client) pickCompleted(ctx context.Context, q querier, run *bulkRun) ([]bulkTask, error) {
	r, err := q.query(ctx, c.sql.pickCompleted,
		run.queues, // $1
		run.kinds,  // $2
		run.groups, // $3
		run.after,  // $4
		run.until,  // $5
	)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var out []bulkTask
	for r.Next() {
		t := bulkTask{state: StateCompleted}
		if err := r.Scan(&t.id, &t.finalized, &t.locked); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, r.Err()
}

// finalizedRange returns the first and the last time that the completed
// tasks ts were finalized. ts is not empty.
func finalizedRange(ts []bulkTask) (first, last time.Time) {
	first, last = ts[0].finalized, ts[0].finalized
	for _, t := range ts[1:] {
		if t.finalized.Before(first) {
			first = t.finalized
		}
		if t.finalized.After(last) {
			last = t.finalized
		}
	}
	return first, last
}
