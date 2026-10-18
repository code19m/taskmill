package taskmill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill/internal/migrations"
)

const (
	defaultSchema          = "taskmill"
	defaultMaxPayloadBytes = 1 << 20
)

// Client enqueues and manages tasks. It is safe for concurrent use, and an
// application typically creates one per database and shares it.
type Client struct {
	pool            *pgxpool.Pool
	schema          string
	sql             *statements
	logger          *slog.Logger
	notifier        *notifier
	maxPayloadBytes int
	contextMetadata func(context.Context) map[string]string
}

// ClientConfig configures a [Client]. The zero value is valid.
type ClientConfig struct {
	// Schema is the PostgreSQL schema that holds taskmill's tables.
	// Default: "taskmill".
	Schema string

	// Logger receives the client's logs. Default: logs are discarded.
	Logger *slog.Logger

	// MaxPayloadBytes rejects larger payloads with [ErrInvalidArgument], to
	// protect the database from oversized rows. Default: 1 MiB.
	MaxPayloadBytes int

	// ContextMetadata, if set, is called on every enqueue and its result is
	// added to the task's metadata, for example to carry a trace context from
	// the producer to the handler. Metadata set in [EnqueueOptions] wins.
	ContextMetadata func(ctx context.Context) map[string]string
}

// NewClient returns a client that uses pool for its own statements. cfg may
// be nil. It fails with [ErrSchemaOutdated] if the schema was not migrated.
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
	if cfg.MaxPayloadBytes < 0 {
		return nil, invalidArgument("MaxPayloadBytes must not be negative")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	maxPayload := cfg.MaxPayloadBytes
	if maxPayload == 0 {
		maxPayload = defaultMaxPayloadBytes
	}

	c := &Client{
		pool:            pool,
		schema:          schema,
		sql:             newStatements(schema),
		logger:          logger,
		notifier:        newNotifier(pool, schema, logger),
		maxPayloadBytes: maxPayload,
		contextMetadata: cfg.ContextMetadata,
	}
	if err := c.checkSchema(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// checkSchema verifies that the schema is migrated to at least the version
// this code needs. A newer schema is accepted: migrations stay compatible
// with the previous release, so rolling deploys work.
func (c *Client) checkSchema(ctx context.Context) error {
	var version int
	err := c.pool.QueryRow(ctx, c.sql.schemaVersion).Scan(&version)
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == "42P01" { // undefined_table
		return fmt.Errorf("%w: schema %q is not migrated", ErrSchemaOutdated, c.schema)
	}
	if err != nil {
		return fmt.Errorf("taskmill: read schema version: %w", err)
	}
	if want := migrations.Latest(); version < want {
		return fmt.Errorf("%w: schema %q is at version %d, need %d", ErrSchemaOutdated, c.schema, version, want)
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
		return nil, fmt.Errorf("%w: %d", ErrNotFound, id)
	}
	info, err := scanTaskInfo(r)
	if err != nil {
		return nil, fmt.Errorf("taskmill: get task %d: %w", id, err)
	}
	return info, nil
}

// Cancel cancels a task. A waiting task is canceled at once; a running task's
// context is canceled, and the task becomes canceled when its attempt ends,
// unless the handler completed it. It returns [ErrInvalidState] if the task
// already finished.
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
	var prev, queue, group string
	found, err := queryRow(ctx, q, c.sql.cancel, []any{id}, &prev, &queue, &group)
	if err != nil {
		return fmt.Errorf("taskmill: cancel task %d: %w", id, err)
	}
	if !found {
		return c.stateError(ctx, q, id, "cancel")
	}
	if State(prev) == StateRunning {
		if _, err := q.exec(ctx, "SELECT pg_notify($1, $2)", c.schema, cancelPayload(id)); err != nil {
			return fmt.Errorf("taskmill: cancel task %d: %w", id, err)
		}
		return nil
	}
	if group != "" {
		head := State(prev) != StatePending
		if err := c.leaveGroup(ctx, q, queue, group, id, head, false); err != nil {
			return fmt.Errorf("taskmill: cancel task %d: %w", id, err)
		}
	}
	return nil
}

// Retry makes a dead or canceled task available again, with a fresh set of
// attempts. A task in a FIFO group waits behind the group's unfinished tasks.
// It returns [ErrInvalidState] for tasks in other states.
func (c *Client) Retry(ctx context.Context, id int64) error {
	return c.inTx(ctx, func(q querier) error { return c.retry(ctx, q, id) })
}

func (c *Client) retry(ctx context.Context, q querier, id int64) error {
	var state, queue, group string
	found, err := queryRow(ctx, q, c.sql.lockForRetry, []any{id}, &state, &queue, &group)
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
		c.notifyAvailable(ctx, q, queue)
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
// history. It returns [ErrInvalidState] for a running task; cancel it first.
func (c *Client) Delete(ctx context.Context, id int64) error {
	return c.inTx(ctx, func(q querier) error { return c.delete(ctx, q, id) })
}

func (c *Client) delete(ctx context.Context, q querier, id int64) error {
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
		return nil
	}

	if group == "" {
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
	if counted {
		if err := c.leaveGroup(ctx, q, queue, group, id, s != StatePending, false); err != nil {
			return fmt.Errorf("taskmill: delete task %d: %w", id, err)
		}
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
		return fmt.Errorf("%w: %d", ErrNotFound, id)
	}
	return fmt.Errorf("%w: cannot %s task %d in state %s", ErrInvalidState, op, id, state)
}

// leaveGroup updates a FIFO group after one of its counted tasks finished or
// was removed. head reports whether that task was the group's head, and dead
// whether it became dead, which halts the group if its queue says so. Callers
// run it in the same transaction as the change to the task.
func (c *Client) leaveGroup(ctx context.Context, q querier, queue, group string, id int64, head, dead bool) error {
	if head && dead {
		halted, err := q.exec(ctx, c.sql.haltGroup, queue, group, id)
		if err != nil {
			return err
		}
		if halted == 1 {
			return nil
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
	case head && !halted:
		return c.promoteGroup(ctx, q, queue, group)
	default:
		return nil
	}
}

// promoteGroup makes the oldest pending task of a group its head.
func (c *Client) promoteGroup(ctx context.Context, q querier, queue, group string) error {
	allow := c.notifier.allow(ctx, queue)
	var state string
	var notified bool
	_, err := queryRow(ctx, q, c.sql.promoteGroup, []any{queue, group, c.schema, allow}, &state, &notified)
	return err
}

// notifyAvailable announces new available tasks in queue, inside q's
// transaction when the throttle allows it.
func (c *Client) notifyAvailable(ctx context.Context, q querier, queue string) {
	if !c.notifier.allow(ctx, queue) {
		return
	}
	if _, err := q.exec(ctx, "SELECT pg_notify($1, $2)", c.schema, availablePayload(queue)); err != nil {
		c.logger.LogAttrs(ctx, slog.LevelWarn, "notification failed",
			slog.String(logKeyQueue, queue), slog.Any(logKeyError, err))
	}
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

// cancelPayload is the notification payload that asks the worker running a
// task to cancel it.
func cancelPayload(id int64) string {
	return fmt.Sprintf("c|%d", id)
}
