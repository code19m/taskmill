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

	// keptIDs is the most IDs of a range of finish times that a read keeps
	// for the next batches, and readCost about how many tasks a pick in ID
	// order reads in the time that such a read takes for each task of the
	// range: see finishWalk. Where the range is sparse, each of its tasks
	// lies on a page of its own: there, it measured 8 to 13 on PostgreSQL 15
	// and 18. The lower end holds where a pick in ID order reads the range in
	// its index instead, which the planner may choose for a batch.
	keptIDs  = 50 * maintenanceBatch
	readCost = 8
)

// TaskFilter selects tasks by what they are, by queue, state, kind, and FIFO
// group, and by when they were created or finished, for [Client.List],
// [Client.Count], and the bulk actions: a task matches if it matches every
// field that is set. Each filter takes at most 100 values. A queue name or
// kind has at most 128 characters, and a group key at most 256. An unknown
// state, like a value that is empty or too long, a filter with too many
// values, or a range of times that breaks its rules, is an error that wraps
// [ErrInvalidArgument].
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

	// CreatedSince limits the tasks to those created at this time or later:
	// those whose [TaskInfo.CreatedAt] is not before it. Zero means no lower
	// bound. Any filter takes it, whatever its states, and it puts a list in
	// creation order: see [Client.List]. A bulk action still goes through
	// its tasks in ID order. Together with a range of finish times it is an
	// error that wraps [ErrInvalidArgument]: a list has one order.
	//
	// The time is an instant: its location does not matter, and neither
	// does the time zone of the database or of the process. It must be a
	// time that PostgreSQL can store, from 4713 BC to 294276 AD, and it
	// must be before CreatedBefore if both are set.
	CreatedSince time.Time

	// CreatedBefore limits the tasks to those created before this time,
	// under the rules of CreatedSince. Zero means no upper bound. A task
	// created at this very time is left out, so two ranges that meet at a
	// time share no task.
	CreatedBefore time.Time

	// FinishedSince limits the tasks to those that finished at this time or
	// later: those whose [TaskInfo.FinishedAt] is not before it. Zero means
	// no lower bound. Only a filter of only dead tasks, only canceled tasks,
	// or both takes it, without a range of creation times: it lists them in
	// finish order, see [Client.List]. With any other filter it is an error
	// that wraps [ErrInvalidArgument].
	//
	// The time is an instant, under the rules of CreatedSince, and it must
	// be before FinishedBefore if both are set.
	FinishedSince time.Time

	// FinishedBefore limits the tasks to those that finished before this
	// time, under the rules of FinishedSince. Zero means no upper bound. A
	// task that finished at this very time is left out, so two ranges that
	// meet at a time share no task.
	FinishedBefore time.Time
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

// CountResult counts the tasks that match a filter.
type CountResult struct {
	// States holds how many of them are in each state that the filter
	// selects.
	States map[State]TaskCount

	// Retry, Cancel, and Delete are how many of them [Client.RetryMatching],
	// [Client.CancelMatching], and [Client.DeleteMatching] would change.
	Retry, Cancel, Delete TaskCount
}

// BulkParams selects the tasks of a bulk action, such as
// [Client.RetryMatching]. Nil params, a filter that sets none of Queues,
// States, Kinds, and GroupKeys, and a cursor that is not valid are errors
// that wrap [ErrInvalidArgument].
type BulkParams struct {
	// Filter selects the tasks. Required: it must set Queues, States, Kinds,
	// or GroupKeys, since a range of times alone does not say which tasks
	// to change. The action goes through the tasks in ID order, also in a
	// range of times.
	Filter TaskFilter

	// Cursor continues an action that stopped early, from its
	// [BulkResult.Next], with the filter that it came from. Empty means from
	// the oldest task.
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

// onlyFinished reports whether f selects only dead tasks, only canceled
// tasks, or both.
func (f *filterArgs) onlyFinished() bool {
	unfinished := func(state string) bool { return state != string(StateDead) && state != string(StateCanceled) }
	return !f.history && !slices.ContainsFunc(f.states, unfinished)
}

// bulkRun is a bulk action's filter and range, and how far the action got.
type bulkRun struct {
	filterArgs
	ranges      timeRanges
	after, last int64    // the action continues after task after, up to task last
	windowed    bool     // the batches read in windows: one without a window ran out of time
	until       int64    // the batches read up to task until: last, or where their window ends
	left        groupSet // the groups that a retry left, and whose later tasks it leaves

	// How the batches read a range of creation times in each table, if the
	// filter names no queue: see rangeWalk.
	tasksWalk, historyWalk rangeWalk

	// The IDs of a range of finish times that the batches have in hand, and
	// whether they read the rest of the range in ID order: see finishWalk.
	ahead     finishWalk
	inIDOrder bool
}

// rangeWithoutQueue reports whether a statement reads the tasks f in the
// ranges r queue by queue: if r has a range, and f names no queue. The
// indexes that hold times lead with the queue. Before PostgreSQL 18, which
// can skip from one queue to the next, a read without a queue can use them
// only whole, so the planner reads the tasks in ID order instead, past every
// task outside the range, or every task of their state. Queue by queue, a
// statement first finds the queues that hold tasks of its states, with one
// index probe each, and names them. The planner can then read each queue's
// part of the index of the range's time, as for the queues of a filter.
func rangeWithoutQueue(f *filterArgs, r timeRanges) bool {
	return len(f.queues) == 0 && (r.created.set || r.finished.set)
}

// rangeWalk is how the batches of an action with a range of creation times,
// and no queue in its filter, read one table: queue by queue, or in ID
// order. Queue by queue, a batch reads the range in the index of creation
// times, to find the first tasks in ID order, and counts the tasks that it
// finds. In ID order, it reads every task after the position until it has a
// batch, in the range or not. See bulkRun.readsQueues for when a batch reads
// which way. A range of finish times, which only dead and canceled tasks
// take, is read another way: see finishWalk.
type rangeWalk struct {
	counted bool  // a batch read the range queue by queue, and counted its tasks
	rest    int64 // if counted: the tasks of the range after the position
}

// next returns w after a batch that went through the tasks ids. found is
// how many tasks of the range the batch found after the position, if it
// read them queue by queue, or -1.
func (w rangeWalk) next(ids []int64, found int64) rangeWalk {
	if found < 0 {
		w.rest = max(0, w.rest-int64(len(ids)))
		return w
	}
	w.counted, w.rest = true, found-int64(len(ids))
	return w
}

// readsQueues reports whether the next batch of run reads the table of w
// queue by queue: see rangeWalk. Two batches do:
//
//   - The first, which so skips the tasks before the range.
//   - The last of a range with an end, once the rest of it fits in a batch,
//     which so stops with the range. In ID order, it would read every task
//     after the range, up to the action's last.
//
// The other batches read in ID order, which reads the heap in order and
// stops with a batch. Tasks created close in time lie together in ID order,
// so that reads about a batch of the range. Queue by queue, each batch would
// read the rest of the range again. A batch in a window reads in ID order
// too: a window bounds such a read, and not one by queue.
func (run *bulkRun) readsQueues(w rangeWalk) bool {
	if run.windowed || len(run.queues) > 0 || !run.ranges.created.set {
		return false
	}
	return !w.counted || run.ranges.created.until.Valid && w.rest <= maintenanceBatch
}

// finishWalk is how the batches of an action with a range of finish times
// find its tasks. Such tasks can lie far apart in ID order, as tasks that
// died in an incident but were created over weeks do. In ID order, a batch
// would read every task between them. The index of finish times holds them
// by queue and time, and not by ID: a batch that found the first tasks in ID
// order there would read the rest of the range again each time. So a batch
// with no IDs in hand reads the range whole, queue by queue, through that
// index, keeps the IDs of the first keptIDs tasks after the position, in ID
// order, and picks the first batch of them, all in one statement. The next
// batches pick the other IDs, each task by its primary key. A range that
// fits in a batch so reads what a pick in ID order reads: each task once,
// and those it picks once more to lock them.
//
// A read costs about the whole range, wherever the position is, since the
// index holds no IDs: n tasks left take some n/keptIDs more reads, of
// n+keptIDs tasks or so each, a cost that grows with the square of n. So
// once the first read's IDs run out, the action weighs, once, reading the
// range again against going on in ID order, which reads each task after the
// position once: see Client.weighRead. If reading again pays off, the
// batches read the range again whenever their IDs run out. Else they go on
// in ID order, as without the IDs, where the planner picks for each batch
// the index that reads less.
//
// The action still goes through the tasks in ID order, so its cursor, the
// order of its FIFO groups, and its one read of each task stay as they are.
// A pick checks each task again, and passes by one that left the range
// meanwhile, as one in ID order does. A task that comes into the range
// meanwhile, as one that dies, is changed if the action has not read past
// it yet, as in ID order, where a batch reads up to its end: a read of the
// range reads up to keptIDs tasks ahead.
//
// 50 batches of IDs take 400 KB in the client, and 600 KB from the
// database, which sorts them in memory under its default work_mem: 100
// batches sorted on disk. A read that runs out of a batch's time, as one of
// a range of millions of tasks can, sends the action on in windows, which
// read in ID order.
type finishWalk struct {
	ids   []int64 // the IDs that the last read kept, after the position, in order
	more  bool    // the range has more tasks after the last of ids
	again bool    // the action weighed reading the range again, and does
}

// keepsIDs reports whether the next batch of run finds its tasks by the IDs
// that a read of its range of finish times kept: see finishWalk.
func (run *bulkRun) keepsIDs() bool {
	return !run.windowed && !run.inIDOrder && run.ranges.finished.set
}

// weighs reports whether the action weighs reading its range of finish
// times again before the next batch: the IDs of the first read ran out,
// and the range has more. See finishWalk.
func (run *bulkRun) weighs() bool {
	return run.keepsIDs() && len(run.ahead.ids) == 0 && run.ahead.more && !run.ahead.again
}

// past returns w after a batch that went through the tasks up to end.
func (w finishWalk) past(end int64) finishWalk {
	i, found := slices.BinarySearch(w.ids, end)
	if found {
		i++
	}
	w.ids = w.ids[i:]
	return w
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

// CountParams selects the tasks that [Client.Count] counts. A field outside
// its limits is an error that wraps [ErrInvalidArgument].
type CountParams struct {
	// Filter selects the tasks. Empty means every task. With the filter of
	// a bulk action, the counts say what that action would change.
	Filter TaskFilter
}

// Count counts the tasks that params select. params may be nil, which
// counts every task. It counts at most 10,000 tasks per state. Like
// [Client.List], the query is canceled after 5 seconds.
func (c *Client) Count(ctx context.Context, params *CountParams) (*CountResult, error) {
	if params == nil {
		params = &CountParams{}
	}
	f, err := match(&params.Filter, allStates)
	if err != nil {
		return nil, err
	}
	r, err := checkRanges(f, &params.Filter)
	if err != nil {
		return nil, err
	}

	out := &CountResult{States: make(map[State]TaskCount, len(f.states)+1)}
	sql := c.sql.countTasks.choose(rangeWithoutQueue(f, r))
	err = c.readAlone(ctx, countSettings, sql, countArgs(f, r), func(r pgx.Rows) error {
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

// countArgs returns the arguments of countTasks for the tasks f in the
// ranges r.
func countArgs(f *filterArgs, r timeRanges) []any {
	states := f.states
	if f.history {
		states = append(slices.Clone(states), string(StateCompleted))
	}
	return append([]any{
		f.queues, // $1
		f.kinds,  // $2
		f.groups, // $3
		states,   // $4
	}, r.args()...) // $5 to $8
}

// add counts the n tasks read in state s, of which unflagged were not asked
// to stop. n is one more than the cap if counting stopped there.
func (c *CountResult) add(s State, n, unflagged int64) {
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

// RetryMatching retries the dead and canceled tasks that params select, as
// [Client.Retry] retries each one: those that match its filter. The filter
// must set Queues, States, Kinds, or GroupKeys: to retry every dead task,
// set its States.
//
// It goes through the tasks enqueued before it started, oldest first, in
// batches of 1,000, each in a short transaction of its own. It reads each
// task once, so it retries a task at most once, even if the task fails again
// meanwhile, and it ends however fast new tasks arrive. A task that dies
// meanwhile is retried if the action has not yet read past it, and a read of
// a range of finish times looks up to 50 batches ahead: a later run retries
// the others. If a batch cannot reach its tasks within a second, because too
// many tasks that do not match lie before them, the action continues in
// batches that read at most 100,000 tasks each, and so gets past those. It
// skips the tasks, the FIFO groups, and the dedupe keys that other
// transactions hold at that moment, never waiting while it holds rows. Like
// [Client.Retry], it gives a canceled task its dedupe key back, and leaves a
// task whose key another task holds: it counts those in
// [BulkResult.Conflicts], and returns no error for them. If ctx ends first,
// it finishes the batch it is in, for at most 5 seconds, and returns what it
// did, with an error and [BulkResult.Next] to continue; each batch commits
// whole or not at all.
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

// CancelMatching cancels the unfinished tasks that params select, as
// [Client.Cancel] cancels each one, counting running tasks, which
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

// DeleteMatching deletes the tasks that params select, except running ones,
// as [Client.Delete] deletes each one, completed tasks in history included.
// It works like [Client.RetryMatching].
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

	// The ranges follow the rules of a list of the filter's tasks, so that
	// Count and List take the same filter: a range of finish times needs a
	// filter of only dead and canceled tasks, not only an action on them.
	selected, err := match(&params.Filter, allStates)
	if err != nil {
		return nil, err
	}
	r, err := checkRanges(selected, &params.Filter)
	if err != nil {
		return nil, err
	}
	f, err := match(&params.Filter, states)
	if err != nil {
		return nil, err
	}

	run := &bulkRun{filterArgs: *f, ranges: r, left: groupSet{queues: []string{}, groups: []string{}}}
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

	tasksWalk, historyWalk rangeWalk  // the walks of the run after the batch
	ahead                  finishWalk // the IDs that the run has in hand after the batch
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

		run.after, run.tasksWalk, run.historyWalk, run.ahead = s.end, s.tasksWalk, s.historyWalk, s.ahead
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
	if run.weighs() {
		if err := c.weighRead(ctx, run); err != nil {
			return step{}, err
		}
	}

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

// weighRead weighs reading run's range of finish times again for its tasks
// after the position, once the IDs of its first read ran out, against going
// on in ID order: see finishWalk. Reading again pays off if, for n tasks
// left in the range, (n+keptIDs)²/keptIDs × readCost is at most the tasks
// of the table after the position. The action estimates those from where
// the next bulkWindow of them end, as nextWindow finds, and counts the
// tasks left up to the most for which reading again pays off, and one more.
// A count that runs out of openTimeout sends the action on in ID order: a
// read of the range again would run out of it too, as a read in a batch,
// and send the action on in windows, which read in ID order.
//
// The statement runs in a transaction of its own with weighSettings, all
// in one round trip: see readAlone.
func (c *Client) weighRead(ctx context.Context, run *bulkRun) error {
	args := append([]any{
		run.queues, // $1
		run.kinds,  // $2
		run.groups, // $3
		run.states, // $4
		run.after,  // $5
		run.until,  // $6
	}, run.ranges.args()...) // $7 to $10
	var most int64
	var left *int64
	err := c.readAlone(ctx, weighSettings, c.sql.readAgain, args, func(r pgx.Rows) error {
		return r.Scan(&most, &left)
	})
	switch {
	case pgCode(err) == codeQueryCanceled && ctx.Err() == nil:
		// The count ran out of its time: the action goes on in ID order.
	case err != nil:
		return err
	default:
		run.ahead.again = left != nil && *left <= most
	}
	run.inIDOrder = !run.ahead.again
	return nil
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

// picked is the next batch of the tasks table that pick read.
type picked struct {
	tasks []bulkTask
	end   int64      // the last task that the batch goes through: see bulkRun.end
	found int64      // the tasks of the range after the position, if pick read them queue by queue, or -1
	ahead finishWalk // the IDs that the run had in hand for the batch, before it went through them
}

// pick reads the next batch of the tasks that run matches, with the pick
// statement p, and locks those it can.
func pick(ctx context.Context, q querier, p pickForms, run *bulkRun) (picked, error) {
	if run.keepsIDs() {
		return pickKept(ctx, q, p, run)
	}
	from := fromIDOrder
	if run.readsQueues(run.tasksWalk) {
		from = fromQueues
	}
	ts, found, _, err := pickRows(ctx, q, p.choose(from == fromQueues), run.pickArgs(), from)
	if err != nil {
		return picked{}, err
	}
	return picked{tasks: ts, end: run.end(idsOf(ts)), found: found}, nil
}

// pickKept is pick from the next batch of the IDs that run has in hand.
// Once the run has none, it reads the range of finish times for more, and
// picks the first batch of them in the same statement. The batch goes
// through its IDs, or up to the action's end if they are the range's last.
func pickKept(ctx context.Context, q querier, p pickForms, run *bulkRun) (picked, error) {
	b := picked{end: run.until, found: -1, ahead: run.ahead}
	if len(b.ahead.ids) == 0 {
		ts, _, rest, err := pickRows(ctx, q, p.reads, run.pickArgs(), fromRange)
		if err != nil {
			return picked{}, err
		}
		ids := append(idsOf(ts), rest...)
		b.tasks, b.ahead = ts, finishWalk{ids: ids[:min(len(ids), keptIDs)], more: len(ids) > keptIDs, again: run.ahead.again}
		if len(rest) > 0 {
			b.end = ts[len(ts)-1].id
		}
		return b, nil
	}

	ids := b.ahead.ids[:min(len(b.ahead.ids), maintenanceBatch)]
	if len(ids) < len(b.ahead.ids) || b.ahead.more {
		b.end = ids[len(ids)-1]
	}
	var err error
	b.tasks, _, _, err = pickRows(ctx, q, p.kept, append(run.pickArgs(), ids), fromKept)
	return b, err
}

// pickRows runs the pick statement sql, of the form from, with args, and
// returns its tasks. A pick from queues also returns how many tasks of the
// range it found after the position, or else -1, and a pick from a range of
// finish times the IDs that it kept after its tasks.
func pickRows(ctx context.Context, q querier, sql string, args []any, from pickFrom) (ts []bulkTask, found int64, rest []int64, err error) {
	found = -1
	if from == fromQueues {
		found = 0
	}
	r, err := q.query(ctx, sql, args...)
	if err != nil {
		return nil, 0, nil, err
	}
	defer r.Close()

	for r.Next() {
		var t bulkTask
		var state string
		var kept []int64
		dest := []any{&t.id, &t.queue, &t.group, &t.locked, &state, &t.inGroup, &t.keyed}
		switch from {
		case fromQueues:
			dest = append(dest, &found)
		case fromRange:
			dest = append(dest, &kept)
		case fromIDOrder, fromKept:
		}
		if err := r.Scan(dest...); err != nil {
			return nil, 0, nil, err
		}
		if kept != nil {
			rest = kept
		}
		t.state = State(state)
		ts = append(ts, t)
	}
	return ts, found, rest, r.Err()
}

// pickArgs returns the arguments of the pick statements of pickSQL, up to
// the IDs of one that picks from kept IDs.
func (run *bulkRun) pickArgs() []any {
	return append([]any{
		run.queues,      // $1
		run.kinds,       // $2
		run.groups,      // $3
		run.states,      // $4
		run.after,       // $5
		run.until,       // $6
		run.left.queues, // $7
		run.left.groups, // $8
	}, run.ranges.args()...) // $9 to $12
}

// historyArgs returns the arguments of pickCompleted.
func (run *bulkRun) historyArgs() []any {
	return []any{
		run.queues,               // $1
		run.kinds,                // $2
		run.groups,               // $3
		run.after,                // $4
		run.until,                // $5
		run.ranges.created.since, // $6
		run.ranges.created.until, // $7
	}
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

	b, err := pick(ctx, q, c.sql.pickRetry, run)
	if err != nil {
		return step{}, err
	}

	ts := b.tasks
	s := step{end: b.end, historyWalk: run.historyWalk}
	s.tasksWalk, s.ahead = run.tasksWalk.next(idsOf(ts), b.found), b.ahead.past(s.end)
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

	b, err := pick(ctx, q, c.sql.pickCancel, run)
	if err != nil {
		return step{}, err
	}

	s := step{end: b.end, historyWalk: run.historyWalk}
	s.tasksWalk, s.ahead = run.tasksWalk.next(idsOf(b.tasks), b.found), b.ahead.past(s.end)
	locked := s.locked(b.tasks)
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
	b := picked{found: -1}
	var completed []bulkTask
	historyFound := int64(-1)
	var err error
	if len(run.states) > 0 {
		if b, err = pick(ctx, q, c.sql.pickDelete, run); err != nil {
			return step{}, err
		}
	}
	if run.history {
		if completed, historyFound, err = c.pickCompleted(ctx, q, run); err != nil {
			return step{}, err
		}
	}

	ts := b.tasks
	s := step{end: run.last}
	if len(run.states) > 0 {
		s.end = min(s.end, b.end)
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
	s.tasksWalk, s.ahead = run.tasksWalk.next(idsOf(ts), b.found), b.ahead.past(s.end)
	s.historyWalk = run.historyWalk.next(idsOf(completed), historyFound)
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
func (c *Client) pickCompleted(ctx context.Context, q querier, run *bulkRun) ([]bulkTask, int64, error) {
	fromQueues, found := run.readsQueues(run.historyWalk), int64(-1)
	if fromQueues {
		found = 0
	}
	r, err := q.query(ctx, c.sql.pickCompleted.choose(fromQueues), run.historyArgs()...)
	if err != nil {
		return nil, 0, err
	}
	defer r.Close()

	var out []bulkTask
	for r.Next() {
		t := bulkTask{state: StateCompleted}
		dest := []any{&t.id, &t.finalized, &t.locked}
		if fromQueues {
			dest = append(dest, &found)
		}
		if err := r.Scan(dest...); err != nil {
			return nil, 0, err
		}
		out = append(out, t)
	}
	return out, found, r.Err()
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
