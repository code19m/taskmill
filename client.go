package taskmill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill/internal/migrations"
)

const defaultSchema = "taskmill"

// Client enqueues and manages tasks. It is safe for concurrent use, and an
// application typically creates one per installation and shares it.
type Client struct {
	pool            *pgxpool.Pool
	schema          string
	sql             *statements
	logger          *slog.Logger
	notifier        *notifier
	contextMetadata func(context.Context) map[string]string

	// historyGapUntil is when completions keep history again, in Unix
	// nanoseconds, after one found no history partition.
	historyGapUntil atomic.Int64
}

// ClientConfig configures a [Client]. The zero value is valid.
type ClientConfig struct {
	// Schema is the PostgreSQL schema that holds taskmill's tables. It must
	// match ^[a-z_][a-z0-9_]{0,62}$. Default: "taskmill".
	Schema string

	// Logger receives the logs of the client and of its workers.
	// Default: logs are discarded.
	Logger *slog.Logger

	// ContextMetadata, if set, is called on every enqueue and its result is
	// added to the task's metadata, for example to carry a trace context from
	// the producer to the handler. Metadata set in [EnqueueOptions] wins.
	ContextMetadata func(ctx context.Context) map[string]string
}

// NewClient returns a client that uses pool for its own statements. cfg may
// be nil. It fails with [ErrSchemaOutdated] if the schema was not migrated.
//
// taskmill's statements rely on the READ COMMITTED isolation level, so
// NewClient fails if the pool's default_transaction_isolation is another
// level; set it in the pool's ConnConfig.RuntimeParams. Transactions that
// callers pass to the Tx methods may use any level.
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
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	c := &Client{
		pool:            pool,
		schema:          schema,
		sql:             newStatements(schema),
		logger:          logger,
		notifier:        newNotifier(pool, schema, logger),
		contextMetadata: cfg.ContextMetadata,
	}
	if err := c.checkSchema(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// checkSchema verifies that the schema is migrated to at least the version
// this code needs, and that the pool uses READ COMMITTED. A newer schema is
// accepted: migrations stay compatible with the previous release, so rolling
// deploys work.
func (c *Client) checkSchema(ctx context.Context) error {
	var version int
	var isolation string
	err := c.pool.QueryRow(ctx, c.sql.schemaVersion).Scan(&version, &isolation)
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == "42P01" { // undefined_table
		return fmt.Errorf("%w: schema %q is not migrated", ErrSchemaOutdated, c.schema)
	}
	if err != nil {
		return fmt.Errorf("taskmill: read schema version: %w", err)
	}
	if want := migrations.Latest(); version < want {
		return fmt.Errorf("%w: schema %q is at version %d, need %d", ErrSchemaOutdated, c.schema, version, want)
	}
	if isolation != "read committed" {
		return fmt.Errorf("taskmill: the pool's default_transaction_isolation is %q, but taskmill needs "+
			"\"read committed\"; set it in the pool's ConnConfig.RuntimeParams", isolation)
	}
	return nil
}

// Get returns a task by ID, whether it is unfinished, finished, or in
// history. It returns [ErrNotFound] if no such task exists.
func (c *Client) Get(ctx context.Context, id int64) (*TaskInfo, error) {
	r, err := c.pool.Query(ctx, c.sql.getTask, id)
	if err != nil {
		return nil, fmt.Errorf("taskmill: get task %d: %w", id, err)
	}
	defer r.Close()
	if !r.Next() {
		if err := r.Err(); err != nil {
			return nil, fmt.Errorf("taskmill: get task %d: %w", id, err)
		}
		return nil, fmt.Errorf("%w: task %d", ErrNotFound, id)
	}
	info, err := scanTaskInfo(r)
	if err != nil {
		return nil, fmt.Errorf("taskmill: get task %d: %w", id, err)
	}
	return info, nil
}

// Cancel cancels a task. A waiting task is canceled at once; a running task's
// context is canceled, and the task becomes canceled when its attempt ends,
// unless the handler completed it. A task of a batch is canceled when its
// batch ends. Cancel frees the task's dedupe key. It returns
// [ErrInvalidState] if the task already finished.
func (c *Client) Cancel(ctx context.Context, id int64) error {
	return c.inTx(ctx, func(q querier) error { return c.cancel(ctx, q, id) })
}

// CancelTx is [Client.Cancel] in the caller's transaction.
func (c *Client) CancelTx(ctx context.Context, tx Executor, id int64) error {
	q, err := txQuerier(tx)
	if err != nil {
		return err
	}
	return c.cancel(ctx, q, id)
}

func (c *Client) cancel(ctx context.Context, q querier, id int64) error {
	fail := func(err error) error { return fmt.Errorf("taskmill: cancel task %d: %w", id, err) }
	if _, err := q.exec(ctx, c.sql.lockPendingGroup, id); err != nil {
		return fail(err)
	}
	var state, queue, group string
	found, err := queryRow(ctx, q, c.sql.lockTask, []any{id}, &state, &queue, &group)
	if err != nil {
		return fail(err)
	}
	if !found {
		return c.stateError(ctx, q, id, "cancel")
	}
	s := State(state)
	if s.Finished() {
		return fmt.Errorf("%w: cannot cancel task %d in state %s", ErrInvalidState, id, s)
	}
	if _, err := q.exec(ctx, c.sql.releaseKey, id); err != nil {
		return fail(err)
	}
	if s == StateRunning {
		if _, err := q.exec(ctx, c.sql.flagCancel, id); err != nil {
			return fail(err)
		}
		if _, err := q.exec(ctx, notifySQL, c.schema, notifyCancel+"|"+strconv.FormatInt(id, 10)); err != nil {
			return fail(err)
		}
		return nil
	}
	if _, err := q.exec(ctx, c.sql.cancelWaiting, id); err != nil {
		return fail(err)
	}
	if group == "" {
		return nil
	}
	how := leftHead
	if s == StatePending {
		how = leftWaiting
	}
	if err := c.leaveGroup(ctx, q, queue, group, id, how); err != nil {
		return fail(err)
	}
	return nil
}

// Retry makes a dead or canceled task available again, with a fresh set of
// attempts. Retrying the dead task that halts its FIFO group resumes the
// group; another task of a group waits behind the group's unfinished tasks.
// It returns [ErrInvalidState] for tasks in other states.
func (c *Client) Retry(ctx context.Context, id int64) error {
	return c.inTx(ctx, func(q querier) error { return c.retry(ctx, q, id) })
}

func (c *Client) retry(ctx context.Context, q querier, id int64) error {
	var state, queue, group string
	found, err := queryRow(ctx, q, c.sql.lockTask, []any{id}, &state, &queue, &group)
	if err != nil {
		return fmt.Errorf("taskmill: retry task %d: %w", id, err)
	}
	if !found {
		return c.stateError(ctx, q, id, "retry")
	}
	if s := State(state); s != StateDead && s != StateCanceled {
		return fmt.Errorf("%w: cannot retry task %d in state %s", ErrInvalidState, id, s)
	}

	pending := false
	if group != "" {
		if pending, err = c.rejoinGroup(ctx, q, queue, group, id); err != nil {
			return fmt.Errorf("taskmill: retry task %d: %w", id, err)
		}
	}
	if _, err := q.exec(ctx, c.sql.retry, id, pending); err != nil {
		return fmt.Errorf("taskmill: retry task %d: %w", id, err)
	}
	if !pending {
		if err := c.notifyAvailable(ctx, q, queue); err != nil {
			return fmt.Errorf("taskmill: retry task %d: %w", id, err)
		}
	}
	return nil
}

// rejoinGroup counts a retried task back into its FIFO group, and reports
// whether it must wait behind the group's other tasks. A dead head that halts
// its group is still counted: retrying it resumes the group.
func (c *Client) rejoinGroup(ctx context.Context, q querier, queue, group string, id int64) (pending bool, err error) {
	halting, err := c.haltedBy(ctx, q, queue, group, id)
	if err != nil {
		return false, err
	}
	if halting {
		_, err = q.exec(ctx, c.sql.resumeHaltedHead, queue, group, id)
		return false, err
	}
	var tasks int
	if _, err := queryRow(ctx, q, c.sql.enterGroup, []any{queue, group}, &tasks); err != nil {
		return false, err
	}
	return tasks > 1, nil
}

// haltedBy reports whether task id halts its group. The caller must hold the
// task's lock.
func (c *Client) haltedBy(ctx context.Context, q querier, queue, group string, id int64) (bool, error) {
	var halting bool
	_, err := queryRow(ctx, q, c.sql.haltedBy, []any{queue, group, id}, &halting)
	return halting, err
}

// Delete deletes a task that is not running, including a completed task in
// history, and frees its dedupe key. Deleting the dead task that halts its
// FIFO group resumes the group. It returns [ErrInvalidState] for a running
// task; cancel it first.
func (c *Client) Delete(ctx context.Context, id int64) error {
	return c.inTx(ctx, func(q querier) error { return c.delete(ctx, q, id) })
}

func (c *Client) delete(ctx context.Context, q querier, id int64) error {
	if _, err := q.exec(ctx, c.sql.lockPendingGroup, id); err != nil {
		return fmt.Errorf("taskmill: delete task %d: %w", id, err)
	}
	var state, queue, group string
	found, err := queryRow(ctx, q, c.sql.deleteTask, []any{id}, &state, &queue, &group)
	if err != nil {
		return fmt.Errorf("taskmill: delete task %d: %w", id, err)
	}
	if !found {
		n, herr := q.exec(ctx, c.sql.deleteHistory, id)
		if herr != nil {
			return fmt.Errorf("taskmill: delete task %d: %w", id, herr)
		}
		if n == 0 {
			return c.stateError(ctx, q, id, "delete")
		}
	}
	if _, err := q.exec(ctx, c.sql.releaseKey, id); err != nil {
		return fmt.Errorf("taskmill: delete task %d: %w", id, err)
	}
	if !found || group == "" {
		return nil
	}
	// Unfinished tasks are counted in their group, and so is a dead head that
	// halts it.
	s := State(state)
	counted := !s.Finished()
	if !counted {
		if counted, err = c.haltedBy(ctx, q, queue, group, id); err != nil {
			return fmt.Errorf("taskmill: delete task %d: %w", id, err)
		}
	}
	if !counted {
		return nil
	}
	how := leftHead
	if s == StatePending {
		how = leftWaiting
	}
	if err := c.leaveGroup(ctx, q, queue, group, id, how); err != nil {
		return fmt.Errorf("taskmill: delete task %d: %w", id, err)
	}
	return nil
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

// leaving says how a counted task left its FIFO group.
type leaving int

const (
	leftWaiting  leaving = iota // a pending task left: the head stays
	leftHead                    // the head finished or was removed: the next task becomes the head
	leftDeadHead                // the head became dead: the group halts behind it
)

// leaveGroup updates a FIFO group after one of its counted tasks finished or
// was removed. Callers run it in the same transaction as the change to the
// task.
func (c *Client) leaveGroup(ctx context.Context, q querier, queue, group string, id int64, how leaving) error {
	if how == leftDeadHead {
		halted, err := q.exec(ctx, c.sql.haltGroup, queue, group, id)
		if err != nil || halted == 1 {
			return err
		}
	}

	var remaining int
	var halted bool
	found, err := queryRow(ctx, q, c.sql.decrementGroup, []any{queue, group, id}, &remaining, &halted)
	if err != nil {
		return err
	}
	switch {
	case !found:
		c.logger.LogAttrs(ctx, slog.LevelWarn, "fifo group missing",
			slog.String(logKeyQueue, queue), slog.String(logKeyGroup, group), slog.Int64(logKeyTaskID, id))
		return nil
	case remaining == 0:
		_, err = q.exec(ctx, c.sql.deleteEmptyGroup, queue, group)
		return err
	case how != leftWaiting && !halted:
		_, err = q.exec(ctx, c.sql.promoteGroup, queue, group, c.schema, c.notifier.allow(ctx, queue))
		return err
	default:
		return nil
	}
}

// groupHead is a group whose head finished.
type groupHead struct {
	queue, group string
	id           int64 // the finished head
}

// finishHeads is leaveGroup for heads that completed, one per group, with a
// few statements for all of them. The caller holds the groups' locks.
func (c *Client) finishHeads(ctx context.Context, q querier, heads []groupHead) error {
	queues := make([]string, len(heads))
	groups := make([]string, len(heads))
	ids := make([]int64, len(heads))
	for i, h := range heads {
		queues[i], groups[i], ids[i] = h.queue, h.group, h.id
	}
	r, err := q.query(ctx, c.sql.decrementGroups, queues, groups, ids)
	if err != nil {
		return err
	}
	var empty, next groupSet
	for r.Next() {
		var queue, group string
		var remaining int
		var halted bool
		if err := r.Scan(&queue, &group, &remaining, &halted); err != nil {
			r.Close()
			return err
		}
		switch {
		case remaining == 0:
			empty.add(queue, group)
		case !halted:
			next.add(queue, group)
		}
	}
	r.Close()
	if err := r.Err(); err != nil {
		return err
	}
	if len(empty.queues) > 0 {
		if _, err := q.exec(ctx, c.sql.deleteEmptyGroups, empty.queues, empty.groups); err != nil {
			return err
		}
	}
	if len(next.queues) == 0 {
		return nil
	}
	return c.promoteGroups(ctx, q, next)
}

// groupSet holds groups as two parallel slices, as statements take them.
type groupSet struct {
	queues, groups []string
}

func (s *groupSet) add(queue, group string) {
	s.queues = append(s.queues, queue)
	s.groups = append(s.groups, group)
}

// promoteGroups makes the oldest pending task of every group in s its head.
func (c *Client) promoteGroups(ctx context.Context, q querier, s groupSet) error {
	var notify []string
	for _, queue := range s.queues {
		if !slices.Contains(notify, queue) && c.notifier.allow(ctx, queue) {
			notify = append(notify, queue)
		}
	}
	_, err := q.exec(ctx, c.sql.promoteGroups, s.queues, s.groups, c.schema, notify)
	return err
}

// notifyAvailable announces new available tasks in queue, inside q's
// transaction, when the throttle allows it. In a transaction, a failed
// notification aborts the transaction, so it is returned; outside one, it
// is only logged.
func (c *Client) notifyAvailable(ctx context.Context, q querier, queue string) error {
	if !c.notifier.allow(ctx, queue) {
		return nil
	}
	_, err := q.exec(ctx, notifySQL, c.schema, notifyAvailable+"|"+queue)
	if err != nil && !q.inTx() {
		c.logger.LogAttrs(ctx, slog.LevelWarn, "notification failed",
			slog.String(logKeyQueue, queue), slog.Any(logKeyError, err))
		return nil
	}
	return err
}

// inTx runs fn in a transaction on the client's pool.
func (c *Client) inTx(ctx context.Context, fn func(q querier) error) error {
	return pgx.BeginFunc(ctx, c.pool, func(tx pgx.Tx) error {
		return fn(pgxQuerier{q: tx})
	})
}

// txQuerier returns the querier of an Executor that must be a transaction,
// because the operation runs several statements that must commit together.
func txQuerier(tx Executor) (querier, error) {
	if tx == nil {
		return nil, invalidArgument("executor is nil")
	}
	q := tx.querier()
	if q == nil {
		return nil, invalidArgument("executor has no querier; create it with Pgx or SQL")
	}
	if !q.inTx() {
		return nil, invalidArgument("executor must wrap a transaction (pgx.Tx or *sql.Tx)")
	}
	return q, nil
}
