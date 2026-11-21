package taskmill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill/internal/migrations"
)

const defaultSchema = "taskmill"

// Client enqueues and manages tasks. Create one with [NewClient], and
// [Client.Close] it when it is no longer needed. It is safe for concurrent
// use, and an application typically creates one per installation and shares
// it. It uses the pool it was given and never closes it.
//
// Its methods, by what the caller wants to do:
//
//   - Enqueue: [Client.Enqueue] and [Client.EnqueueMany], and in the caller's
//     transaction [Client.EnqueueTx] and [Client.EnqueueManyTx].
//   - Read: [Client.Get], [Client.List], [Client.Count], [Client.Groups],
//     [Client.Queues], [Client.Schedules], [Client.Workers],
//     [Client.Activity], and [Client.Settings].
//   - Change one task: [Client.Cancel], [Client.CancelTx], [Client.Retry],
//     and [Client.Delete].
//   - Change every task that a filter matches: [Client.RetryMatching],
//     [Client.CancelMatching], and [Client.DeleteMatching].
//   - Change a queue or a schedule: [Client.PauseQueue],
//     [Client.ResumeQueue], [Client.UpdateQueue], [Client.PauseSchedule],
//     [Client.ResumeSchedule], and [Client.RunSchedule].
//   - Change what the installation keeps: [Client.UpdateSettings].
//
// The reads are bounded, so that a dashboard or a script cannot overload the
// database. The [operations guide] shows them at work.
//
// [operations guide]: https://github.com/code19m/taskmill/blob/main/docs/guides/operations-api.md
type Client struct {
	pool              *pgxpool.Pool
	schema            string
	sql               *statements
	logger            *slog.Logger
	notifier          *notifier
	enqueueMiddleware []EnqueueMiddleware

	// historyGapUntil is when completions keep history again, in Unix
	// nanoseconds, after one found no history partition.
	historyGapUntil atomic.Int64
}

// ClientConfig configures a [Client]. The zero value is valid. [NewClient]
// reports a Schema that is not valid as an error that wraps
// [ErrInvalidArgument].
type ClientConfig struct {
	// Schema is the PostgreSQL schema that holds taskmill's tables. It must
	// match ^[a-z_][a-z0-9_]{0,62}$. Default: "taskmill".
	Schema string

	// Logger receives the logs of the client and of its workers.
	// Default: logs are discarded.
	Logger *slog.Logger

	// EnqueueMiddleware wraps every enqueue, in order: the first is the
	// outermost. It can add metadata to tasks, for example to carry a trace
	// context from the producer to the handler. A nil entry is an error that
	// wraps [ErrInvalidArgument]. Default: none.
	EnqueueMiddleware []EnqueueMiddleware
}

// NewClient returns a client that uses pool for its own statements. cfg may
// be nil. It fails with [ErrSchemaOutdated] if the schema was not migrated.
//
// taskmill's statements rely on the READ COMMITTED isolation level, so
// NewClient fails with [ErrInvalidArgument] if the pool's
// default_transaction_isolation is another level; set it in the pool's
// ConnConfig.RuntimeParams. Transactions that callers pass to the Tx methods
// may use any level. At REPEATABLE READ or SERIALIZABLE, [Task.CompleteTx]
// must be the transaction's first statement, as its comment explains.
func NewClient(ctx context.Context, pool *pgxpool.Pool, cfg *ClientConfig) (*Client, error) {
	if pool == nil {
		return nil, invalidArgument("pool is nil")
	}
	if cfg == nil {
		cfg = &ClientConfig{}
	}
	schema, err := validateSchema(cfg.Schema)
	if err != nil {
		return nil, err
	}
	if slices.ContainsFunc(cfg.EnqueueMiddleware, func(m EnqueueMiddleware) bool { return m == nil }) {
		return nil, invalidArgument("an enqueue middleware is nil")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	c := &Client{
		pool:              pool,
		schema:            schema,
		sql:               newStatements(schema),
		logger:            logger,
		notifier:          newNotifier(pool, schema, logger),
		enqueueMiddleware: slices.Clone(cfg.EnqueueMiddleware),
	}

	if err := c.checkSchema(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// Close stops the client's background work: it drops the notifications
// that the client held back to wake workers less often, and waits for one
// that is being sent. A worker starts the task of a dropped notification
// when it next polls its queue, instead of within milliseconds. Close does
// not close the pool, so call it before the pool's Close. The client's
// methods still work after Close, but hold back no notification any more:
// each enqueue notifies at once or not at all. Close may be called more
// than once.
func (c *Client) Close() {
	c.notifier.close()
}

// checkSchema verifies that the schema is migrated to at least the version
// this code needs, and that the pool uses READ COMMITTED. A newer schema is
// accepted: migrations stay compatible with the previous release, so rolling
// deploys work.
func (c *Client) checkSchema(ctx context.Context) error {
	var version int
	var isolation string
	err := c.pool.QueryRow(ctx, c.sql.schemaVersion, unprepared).Scan(&version, &isolation)
	if pgCode(err) == codeUndefinedTable {
		return fmt.Errorf("%w: schema %q is not migrated", ErrSchemaOutdated, c.schema)
	}
	if err != nil {
		return fmt.Errorf("taskmill: read schema version: %w", err)
	}

	if want := migrations.Latest(); version < want {
		return fmt.Errorf("%w: schema %q is at version %d, need %d", ErrSchemaOutdated, c.schema, version, want)
	}
	if isolation != "read committed" {
		return fmt.Errorf("%w: the pool's default_transaction_isolation is %q, but taskmill needs "+
			"\"read committed\"; set it in the pool's ConnConfig.RuntimeParams", ErrInvalidArgument, isolation)
	}
	return nil
}

// Get returns a task by ID, whether it is unfinished, finished, or in
// history. It returns [ErrNotFound] if no such task exists.
func (c *Client) Get(ctx context.Context, id int64) (*TaskInfo, error) {
	fail := func(err error) error { return fmt.Errorf("taskmill: get task %d: %w", id, err) }
	r, err := c.pool.Query(ctx, c.sql.getTask, id)
	if err != nil {
		return nil, fail(err)
	}
	defer r.Close()

	if !r.Next() {
		if err := r.Err(); err != nil {
			return nil, fail(err)
		}
		return nil, fmt.Errorf("%w: task %d", ErrNotFound, id)
	}
	info, err := scanTaskInfo(r)
	if err != nil {
		return nil, fail(err)
	}
	return info, nil
}

// Cancel cancels a task. A waiting task is canceled right away; a running
// task's context is canceled, and the task becomes canceled when its attempt
// ends, unless the handler completed it. A batch keeps running, and its
// handler sees the request with [Task.CancelRequested]: the task becomes
// canceled when its batch ends, unless the batch completed it. Cancel frees
// the task's dedupe key right away, and [Client.Retry] of the canceled task
// takes it back. It returns [ErrNotFound] if no task has the ID, and
// [ErrInvalidState] if the task already finished.
//
// Cancel runs again by itself, up to 5 times in all, when another
// transaction changes the task under its locks, or when PostgreSQL ends its
// transaction to end a deadlock (SQLSTATE 40P01). So it returns that error
// only if the fifth try meets it too.
func (c *Client) Cancel(ctx context.Context, id int64) error {
	return c.inLockOrder(ctx, "cancel", id, func(q querier, verifyOrder bool) error { return c.cancel(ctx, q, id, verifyOrder) })
}

// CancelTx is [Client.Cancel] in the caller's transaction.
//
// Unlike Cancel, it cannot take its locks again in another order: that
// would take a rollback, and the transaction is the caller's. So when
// another transaction changes the same task or its FIFO group at the same
// moment, the two may wait for each other: a retry of the task, the end of
// the group's head, which makes the task the next head, or the commit of the
// task's enqueue. PostgreSQL then ends one of them after a second with a
// deadlock error (SQLSTATE 40P01), which may hit the other transaction. The
// transaction that got the error can be run again.
func (c *Client) CancelTx(ctx context.Context, tx Executor, id int64) error {
	q, err := txQuerier(tx)
	if err != nil {
		return err
	}

	verifyOrder := false // the transaction is the caller's: it cannot start again
	return c.cancel(ctx, q, id, verifyOrder)
}

// cancel cancels task id on q. verifyOrder is for takeTask.
func (c *Client) cancel(ctx context.Context, q querier, id int64, verifyOrder bool) error {
	fail := func(err error) error { return fmt.Errorf("taskmill: cancel task %d: %w", id, err) }
	t, found, err := c.takeTask(ctx, q, id, c.sql.lockTask, verifyOrder)
	if err != nil {
		return fail(err)
	}
	if !found {
		return c.stateError(ctx, q, id, "cancel")
	}
	if t.state.Finished() {
		return fmt.Errorf("%w: cannot cancel task %d in state %s", ErrInvalidState, id, t.state)
	}

	// A waiting task is canceled, and a running one is asked to stop.
	if _, err := q.exec(ctx, c.sql.cancelTasks, []int64{id}, c.schema); err != nil {
		return fail(err)
	}

	if t.state == StateRunning || t.group == "" {
		return nil
	}
	left := []groupLeave{{id: id, queue: t.queue, group: t.group, head: t.state != StatePending}}
	if err := c.leaveGroups(ctx, c.logger, q, left); err != nil {
		return fail(err)
	}
	return nil
}

// Retry makes a dead or canceled task available again, with a fresh set of
// attempts. Retrying the dead task that halts its FIFO group resumes the
// group; another task of a group waits until the group's current head
// finishes, and then runs before the group's tasks that were enqueued after
// it.
//
// A canceled task takes back the dedupe key that its cancel freed, for the
// rest of the key's window, so that an enqueue of the key is a duplicate
// again. If another task took the key meanwhile and still holds it, Retry
// changes nothing and returns [ErrConflict]: the two would stand for the
// same work. A task whose window has ended takes no key, like any task that
// outlives its window.
//
// It returns [ErrNotFound] if no task has the ID, and [ErrInvalidState] for
// tasks in other states. Like [Client.Cancel], it runs again by itself, up
// to 5 times in all, when it loses to another transaction.
func (c *Client) Retry(ctx context.Context, id int64) error {
	return c.inLockOrder(ctx, "retry", id, func(q querier, _ bool) error { return c.retry(ctx, q, id) })
}

func (c *Client) retry(ctx context.Context, q querier, id int64) error {
	fail := func(err error) error { return fmt.Errorf("taskmill: retry task %d: %w", id, err) }
	pendingOnly := false // a retry takes a dead or canceled task
	t, found, err := readLocked(ctx, q, c.sql.lockTask, id, pendingOnly)
	if err != nil {
		return fail(err)
	}
	if !found {
		return c.stateError(ctx, q, id, "retry")
	}
	if !t.state.CanRetry() {
		return fmt.Errorf("%w: cannot retry task %d in state %s", ErrInvalidState, id, t.state)
	}

	// The key comes before the group, as in an enqueue.
	refused, err := c.takeKeys(ctx, q, []int64{id})
	if err != nil {
		return fail(err)
	}
	if refused[id] {
		held, err := c.keyHolder(ctx, q, id)
		if err != nil {
			return fail(err)
		}
		return fmt.Errorf("%w: cannot retry task %d: %s", ErrConflict, id, held)
	}

	pending := false
	if t.group != "" {
		if pending, err = c.rejoinGroup(ctx, q, t.queue, t.group, id); err != nil {
			return fail(err)
		}
	}

	// The statement of a bulk retry, with one task: it also notifies the
	// queue of a task that is available again.
	pendingIDs := []int64{} // the tasks that become pending: this one, or none
	var notify []string
	if pending {
		pendingIDs = append(pendingIDs, id)
	} else {
		notify = c.notifiable(ctx, []string{t.queue})
	}
	_, err = q.exec(ctx, c.sql.retryTasks,
		[]int64{id}, // $1
		pendingIDs,  // $2
		c.schema,    // $3
		notify,      // $4
	)
	if err != nil {
		return fail(err)
	}
	return nil
}

// heldKey says who holds the dedupe key that a retried task did not get.
type heldKey struct {
	by    int64     // the task that holds it; 0 if that task no longer exists, or no task holds the key now
	until time.Time // the end of that task's window; zero if no task holds the key now
}

// String says it as the reason of a refused retry. It names the holder only
// if a caller can still find that task, to cancel or delete it.
func (h heldKey) String() string {
	switch {
	case h.by != 0:
		return "task " + strconv.FormatInt(h.by, 10) + " holds its dedupe key"
	case !h.until.IsZero():
		return "a task that no longer exists holds its dedupe key until " + h.until.UTC().Format(time.RFC3339)
	}
	return "another task holds its dedupe key"
}

// keyHolder reads who holds the dedupe key that the canceled task id did not
// get back.
func (c *Client) keyHolder(ctx context.Context, q querier, id int64) (heldKey, error) {
	var held heldKey
	_, err := queryRow(ctx, q, c.sql.keyHolder, []any{id}, &held.by, &held.until)
	return held, err
}

// takeKeys gives the tasks ids, which a retry holds, the dedupe keys that
// their cancels freed. It returns the tasks that did not get theirs, because
// another task holds the key.
func (c *Client) takeKeys(ctx context.Context, q querier, ids []int64) (refused map[int64]bool, err error) {
	r, err := q.query(ctx, c.sql.takeKeys, ids)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	refused = make(map[int64]bool)
	for r.Next() {
		var id int64
		if err := r.Scan(&id); err != nil {
			return nil, err
		}
		refused[id] = true
	}
	return refused, r.Err()
}

// Delete deletes a task that is not running, including a completed task in
// history, and frees its dedupe key. Deleting the dead task that halts its
// FIFO group resumes the group. It returns [ErrNotFound] if no task has the
// ID, and [ErrInvalidState] for a running task; cancel it first. Like
// [Client.Cancel], it runs again by itself, up to 5 times in all, when it
// loses to another transaction.
func (c *Client) Delete(ctx context.Context, id int64) error {
	return c.inLockOrder(ctx, "delete", id, func(q querier, verifyOrder bool) error { return c.delete(ctx, q, id, verifyOrder) })
}

// delete deletes task id on q. verifyOrder is for takeTask.
func (c *Client) delete(ctx context.Context, q querier, id int64, verifyOrder bool) error {
	fail := func(err error) error { return fmt.Errorf("taskmill: delete task %d: %w", id, err) }
	t, found, err := c.takeTask(ctx, q, id, c.sql.deleteTask, verifyOrder)
	if err != nil {
		return fail(err)
	}
	if !found {
		n, err := q.exec(ctx, c.sql.deleteHistory, id)
		if err != nil {
			return fail(err)
		}
		if n == 0 {
			return c.stateError(ctx, q, id, "delete")
		}
	}

	if _, err := q.exec(ctx, c.sql.releaseKeys, []int64{id}); err != nil {
		return fail(err)
	}

	if !found || t.group == "" {
		return nil
	}

	// Unfinished tasks are counted in their group, and so is a dead head that
	// halts it: its halt ends here.
	counted := !t.state.Finished()
	if !counted {
		resumed, err := q.exec(ctx, c.sql.resumeHaltedHead, t.queue, t.group, id)
		if err != nil {
			return fail(err)
		}
		counted = resumed == 1
	}

	if !counted {
		return nil
	}
	left := []groupLeave{{id: id, queue: t.queue, group: t.group, head: t.state != StatePending}}
	if err := c.leaveGroups(ctx, c.logger, q, left); err != nil {
		return fail(err)
	}
	return nil
}

// errLockOrder means that takeTask holds locks in the wrong order, because
// the task changed between pending and another state while it took them.
var errLockOrder = errors.New("taskmill: the task changed while its rows were locked")

// deadlockTries is how many times inLockOrder runs its transaction, and an
// enqueue on the client's pool its statement. Each new try takes another
// transaction that changes the same task or its group while the try before
// runs.
const deadlockTries = 5

// inLockOrder runs fn in a transaction on the client's pool, as Cancel,
// Delete, and Retry need it. It runs fn again, in a new transaction, when
// the first lost to another transaction over the order of their locks:
//
//   - takeTask found its locks in the wrong order: the rollback releases
//     them.
//   - PostgreSQL ended the transaction to end a deadlock. It may pick any of
//     the transactions that wait for each other, also one that kept the
//     order, when another, such as a CancelTx, could not.
//
// The last try runs without verifyOrder, as CancelTx does, and returns what
// it gets.
//
// fn's errors say what it was doing. The begin and the commit of the
// transaction fail outside fn, so inLockOrder wraps their errors in the same
// words: op, such as "cancel", and the task's id.
func (c *Client) inLockOrder(ctx context.Context, op string, id int64, fn func(q querier, verifyOrder bool) error) error {
	for try := 1; ; try++ {
		last := try == deadlockTries
		var inFn error
		err := c.inTx(ctx, func(q querier) error {
			inFn = fn(q, !last)
			return inFn
		})
		if last || !errors.Is(err, errLockOrder) && !isDeadlock(err) {
			if err != nil && inFn == nil {
				err = fmt.Errorf("taskmill: %s task %d: %w", op, id, err)
			}
			return err
		}
	}
}

// isDeadlock reports whether err is PostgreSQL's error for the transaction
// that it ended to end a deadlock.
func isDeadlock(err error) bool {
	return pgCode(err) == codeDeadlockDetected
}

// takeTask runs take, which is lockTask or deleteTask, on task id for Cancel
// and Delete, and returns what it read of the task. It reports whether take
// found the task.
//
// The lock order of FIFO groups is: a pending task's row is locked only
// after its group's row, and with a group's row in hand, no row of a task
// that is not pending is waited for. takeTask reads whether the task is
// pending before it holds a lock, and a Retry, a promotion, or the commit of
// the task's enqueue may change that before it holds one. With verifyOrder,
// it finds out once it holds a lock:
//
//   - With the group's row, which every change into and out of pending
//     takes, it takes the task only if the task is still pending.
//   - With the task's row alone, it reads the state that the lock protects.
//
// verifyOrder: return errLockOrder if the task changed between pending and
// another state while it was being locked; the caller must then roll back,
// which releases the locks, and start again. Only a caller that owns its
// transaction can do that, so verifyOrder is false on the last try and in
// CancelTx, where takeTask continues with the locks as they are.
func (c *Client) takeTask(ctx context.Context, q querier, id int64, take string, verifyOrder bool) (t lockedTask, found bool, err error) {
	groupLocked, err := queryRow(ctx, q, c.sql.lockPendingGroup, []any{id}, new(bool))
	if err != nil {
		return t, false, err
	}

	if t, found, err = readLocked(ctx, q, take, id, groupLocked && verifyOrder); err != nil {
		return t, false, err
	}
	if pending := found && t.state == StatePending; verifyOrder && pending != groupLocked {
		return t, false, errLockOrder
	}
	return t, found, nil
}

// lockedTask is what a statement that locks a task, or deletes it, read of
// the task.
type lockedTask struct {
	state        State
	queue, group string // group is empty for a task without a FIFO group
}

// readLocked runs take, lockTask or deleteTask, on task id, and returns what
// it read of the task. pendingOnly is their $2. It reports whether take found
// the task.
func readLocked(ctx context.Context, q querier, take string, id int64, pendingOnly bool) (t lockedTask, found bool, err error) {
	var state string
	found, err = queryRow(ctx, q, take, []any{id, pendingOnly}, &state, &t.queue, &t.group)
	t.state = State(state)
	return t, found, err
}

// stateError explains why an operation found no task to act on.
func (c *Client) stateError(ctx context.Context, q querier, id int64, op string) error {
	var state string
	found, err := queryRow(ctx, q, c.sql.taskExists, []any{id}, &state)
	if err != nil {
		return fmt.Errorf("taskmill: %s task %d: %w", op, id, err)
	}
	if !found {
		return fmt.Errorf("%w: task %d", ErrNotFound, id)
	}
	return fmt.Errorf("%w: cannot %s task %d in state %s", ErrInvalidState, op, id, state)
}

// inTx runs fn in a transaction on the client's pool.
func (c *Client) inTx(ctx context.Context, fn func(q querier) error) error {
	return pgx.BeginFunc(ctx, c.pool, func(tx pgx.Tx) error {
		return fn(pgxExecutor{q: tx})
	})
}
