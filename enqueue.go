package taskmill

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"time"
)

// DefaultQueue is the queue that tasks go to when neither the enqueue call
// nor the kind's defaults name one.
const DefaultQueue = "default"

// Defaults and limits of enqueue options.
const (
	defaultMaxAttempts  = 25 // with the default backoff, retries span about 13 hours
	defaultDedupeWindow = 24 * time.Hour
	maxNameLength       = 128
	maxGroupKeyLength   = 256
	maxAttemptsLimit    = 10_000
	maxPayloadBytes     = 1 << 20 // protects the database from oversized rows
	maxMetadataBytes    = 64 << 10
	maxEnqueueTasks     = 10_000   // most tasks of one enqueue call
	maxEnqueueBytes     = 64 << 20 // most payload and metadata of one enqueue call
)

// EnqueueOptions control how a task is enqueued. Zero values mean "not set":
// a field set in the call wins over the kind's defaults (see
// [ArgsWithDefaults]), which win over taskmill's defaults. Metadata merges
// key by key instead: the call's keys win over the same keys of the
// defaults, and the other keys of both are kept.
//
// An enqueue returns an error that wraps [ErrInvalidArgument] for a field
// outside its limits. The queue and the group key must also be valid UTF-8,
// and neither they nor the metadata may hold a NUL character.
//
// The [enqueue guide] shows each option at work.
//
// [enqueue guide]: https://github.com/code19m/taskmill/blob/main/docs/guides/enqueue.md
type EnqueueOptions struct {
	// Queue is the queue to enqueue into, at most 128 characters.
	// Default: [DefaultQueue].
	Queue string

	// Priority orders tasks within the queue, from [PriorityLow] to
	// [PriorityCritical]. Default: [PriorityNormal].
	Priority Priority

	// RunAt is the earliest time the task may start. Default: now. A task
	// with a later RunAt becomes available within about a second of that
	// time, while a maintainer runs. It must be a time that PostgreSQL can
	// store, from 4713 BC to 294276 AD, so a time that stands for "never",
	// such as Go's largest, is rejected.
	RunAt time.Time

	// MaxAttempts is how many times the task may run, from 1 to 10,000.
	// Default: 25, which with the default backoff spans about 13 hours.
	MaxAttempts int

	// GroupKey puts the task in a FIFO group: tasks with the same key in the
	// same queue run one at a time, in enqueue order. A group's dead task
	// halts it until an operator retries or deletes that task. At most 256
	// characters. Default: none.
	GroupKey string

	// DedupeKey makes the enqueue idempotent: while the key's window lasts,
	// enqueueing the same kind with the same key in the same queue creates no
	// new task, and reports the existing one. Canceling or deleting the task
	// frees the key right away, and [Client.Retry] of the canceled task
	// takes it back. The key may have any length: taskmill stores a SHA-256
	// hash of the queue, the kind, and the key. Default: none.
	DedupeKey string

	// DedupeWindow is how long DedupeKey blocks duplicates, counted from the
	// enqueue. It must not be negative. Default: 24 hours.
	DedupeWindow time.Duration

	// Metadata is stored with the task and passed to its handler, for
	// example for tracing. Encoded as JSON, it may take at most 64 KiB,
	// including what [EnqueueMiddleware] adds. Keys that start with
	// "taskmill." belong to taskmill, such as [MetadataSchedule], and later
	// releases may add more of them: do not use the prefix for keys of
	// your own. A test of a handler may set taskmill's keys, so that its
	// task stands for a scheduled run. Default: none.
	Metadata map[string]string
}

// EnqueueParams is one task in a batch enqueue.
type EnqueueParams struct {
	// Args is the task. Required.
	Args Args

	// Options are the task's options, as in [Client.Enqueue]. It may be nil.
	Options *EnqueueOptions
}

// EnqueueResult reports the outcome of enqueueing one task.
type EnqueueResult struct {
	// ID identifies the task: the new one, or the existing one for a
	// duplicate.
	//
	// It is 0 for a duplicate whose existing task could not be read. That
	// can happen only when another transaction took the key while the
	// enqueue ran: the enqueue then reads the key a second time. The ID is 0
	// if the task that took the key was canceled or deleted meanwhile, or if
	// that read failed. [Client.Enqueue] and [Client.EnqueueMany] have
	// committed by then, so they log a failed read and return no error. In
	// the caller's transaction, the enqueue returns the error of a failed
	// read, so that the caller can roll back.
	ID int64

	// Duplicate reports that the DedupeKey was already taken, so no task was
	// created.
	Duplicate bool
}

// EnqueueMiddleware wraps every enqueue, for tracing or metrics, including
// the enqueues of schedules. It receives the tasks being enqueued, at least
// one, validated and with their options resolved, in the order of the call.
// It may change their metadata, for example to carry a trace context to the
// handler, and then calls next, which stores the tasks and sets their
// results. Inside the caller's transaction, next returns before the
// transaction commits, and the tasks exist only if it does.
//
// Middleware must call next at most once, and return its error. Returning nil
// without calling next fails the call, stores nothing, and gives the
// middleware outside an error from its next. If middleware fails after next
// succeeded, an enqueue in the caller's transaction returns its error, so
// that the caller can roll back; any other enqueue has committed, so it
// returns the results and logs the error, and a retry does not duplicate the
// tasks.
//
// Add enqueue middleware in [ClientConfig.EnqueueMiddleware]. The
// [middleware guide] shows one that tags every task.
//
// [middleware guide]: https://github.com/code19m/taskmill/blob/main/docs/guides/middleware.md
type EnqueueMiddleware func(ctx context.Context, tasks []*EnqueueTask, next func(context.Context) error) error

// EnqueueTask is a task being enqueued, as [EnqueueMiddleware] sees it.
// Changes to fields other than Metadata are ignored.
type EnqueueTask struct {
	Queue string
	Kind  string

	// Metadata holds the task's metadata, from [EnqueueOptions.Metadata]. It
	// is never nil, and middleware may change it before calling next. For a
	// schedule's run, it also holds taskmill's own keys, such as
	// [MetadataSchedule]: middleware can read them, but taskmill sets them
	// again after it, so it cannot change or remove them.
	Metadata map[string]string

	// Result is set once next returns nil.
	Result EnqueueResult
}

// Enqueue enqueues one task in its own transaction, and returns the task's
// ID. opts may be nil: [EnqueueOptions] has the options and their defaults.
// If the options set a DedupeKey that is taken, Enqueue creates no task: the
// result holds the ID of the existing task, and its Duplicate is true. In a
// rare race that ID is 0: see [EnqueueResult.ID].
//
// The [ClientConfig.EnqueueMiddleware] runs around the enqueue. The enqueue
// notifies the workers of a task that may start now, so a worker with a free
// slot usually starts it within milliseconds.
//
// It returns an error that wraps [ErrInvalidArgument] for nil args, for an
// option outside its limits, and for a payload that cannot be stored: [Args]
// and [EnqueueOptions] have the rules. To enqueue in the same transaction as
// the application's own writes, use [Client.EnqueueTx].
//
// If PostgreSQL ends the transaction to end a deadlock, which other
// transactions can open around a FIFO group's row or a dedupe key (SQLSTATE
// 40P01), Enqueue runs again by itself, up to 5 times in all: nothing of
// the try before was saved. [Client.EnqueueTx] cannot do that in the
// caller's transaction, and returns the error.
func (c *Client) Enqueue(ctx context.Context, args Args, opts *EnqueueOptions) (EnqueueResult, error) {
	res, err := c.enqueue(ctx, pgxExecutor{q: c.pool}, []EnqueueParams{{Args: args, Options: opts}})
	if err != nil {
		return EnqueueResult{}, err
	}
	return res[0], nil
}

// EnqueueTx enqueues one task in the caller's transaction: the task exists
// only if that transaction commits, and workers see it from then on. opts
// may be nil. In all else it works like [Client.Enqueue], and returns the
// same errors.
//
// tx may be a transaction, a connection, or a pool: see [Executor]. A nil
// executor is an error that wraps [ErrInvalidArgument]. An option or a
// payload that fails taskmill's checks fails before any statement runs, so
// the transaction stays usable.
func (c *Client) EnqueueTx(ctx context.Context, tx Executor, args Args, opts *EnqueueOptions) (EnqueueResult, error) {
	q, err := executorQuerier(tx)
	if err != nil {
		return EnqueueResult{}, err
	}
	res, err := c.enqueue(ctx, q, []EnqueueParams{{Args: args, Options: opts}})
	if err != nil {
		return EnqueueResult{}, err
	}
	return res[0], nil
}

// EnqueueMany enqueues several tasks in one round trip and one transaction:
// all of them, or none if it returns an error. Results are in the same order
// as params. One call takes at most 10,000 tasks, with at most 64 MiB of
// payload and metadata in all.
//
// A call over these limits, or with a task that is not valid, is an error
// that wraps [ErrInvalidArgument]; for a task, the error says which one. A
// call with no params does nothing, and returns nil.
//
// Like [Client.Enqueue], it runs again by itself after a deadlock.
func (c *Client) EnqueueMany(ctx context.Context, params []EnqueueParams) ([]EnqueueResult, error) {
	return c.enqueue(ctx, pgxExecutor{q: c.pool}, params)
}

// EnqueueManyTx is [Client.EnqueueMany] in the caller's transaction. tx may
// be a transaction, a connection, or a pool, as for [Client.EnqueueTx].
func (c *Client) EnqueueManyTx(ctx context.Context, tx Executor, params []EnqueueParams) ([]EnqueueResult, error) {
	q, err := executorQuerier(tx)
	if err != nil {
		return nil, err
	}
	return c.enqueue(ctx, q, params)
}

// enqueueBatch holds a batch as the parallel arrays that the enqueue
// statement takes.
type enqueueBatch struct {
	queues, kinds, groups []string
	priorities            []int16
	maxAttempts           []int16
	runAts                []*time.Time
	dedupeKeys            [][]byte
	dedupeWindows         []int64
	payloads, metadata    []string
	due                   map[string]bool // queues with tasks due now
	plain                 bool            // no task uses a dedupe key or a FIFO group
}

func (c *Client) enqueue(ctx context.Context, q querier, params []EnqueueParams) ([]EnqueueResult, error) {
	if len(params) == 0 {
		return nil, nil
	}
	if len(params) > maxEnqueueTasks {
		return nil, invalidArgument("%d tasks in one call, more than the limit of %d", len(params), maxEnqueueTasks)
	}
	tasks := make([]*resolvedTask, len(params))
	payloads := 0
	for i, p := range params {
		t, err := resolve(p)
		if err != nil {
			return nil, inBatch(err, i, len(params))
		}
		// Stop at the first task past the limit, before encoding the rest;
		// store checks the limit again with the metadata.
		if payloads += len(t.payload); payloads > maxEnqueueBytes {
			return nil, tooLarge(len(params))
		}
		tasks[i] = t
	}
	return c.storeThrough(ctx, q, tasks)
}

// storeThrough stores tasks through the enqueue middleware, if there is any.
// The middleware sees taskmill's own metadata of a task, and store sets it
// again after the middleware.
func (c *Client) storeThrough(ctx context.Context, q querier, tasks []*resolvedTask) ([]EnqueueResult, error) {
	if len(c.enqueueMiddleware) == 0 {
		return c.store(ctx, q, tasks)
	}
	views := make([]*EnqueueTask, len(tasks))
	for i, t := range tasks {
		// A copy: the middleware may change it, and t.meta may belong to the
		// caller.
		views[i] = &EnqueueTask{Queue: t.queue, Kind: t.kind, Metadata: t.metadataCopy()}
	}
	var results []EnqueueResult
	err := through(ctx, c.enqueueMiddleware, views, func(ctx context.Context) error {
		for i, v := range views {
			tasks[i].meta = v.Metadata
		}
		res, err := c.store(ctx, q, tasks)
		if err != nil {
			return err
		}
		for i, v := range views {
			v.Result = res[i]
		}
		results = res
		return nil
	}, errEnqueueNextTwice, errNotEnqueued)
	switch {
	case results == nil && err == nil: // a middleware hid the error of the store
		return nil, errNotEnqueued
	case results == nil || err == nil:
		return results, err
	case q.inTx():
		return nil, err // the caller rolls back
	}
	c.logger.LogAttrs(ctx, slog.LevelWarn, "enqueue middleware failed after the tasks were enqueued",
		slog.Int(logKeyTasks, len(results)), slog.Any(logKeyError, err))
	return results, nil
}

// errEnqueueNextTwice fails the second call of next in an enqueue middleware.
var errEnqueueNextTwice = errors.New("taskmill: enqueue middleware called next twice")

// errNotEnqueued fails an enqueue whose middleware returned nil, although
// the tasks were not stored.
var errNotEnqueued = classError("taskmill: enqueue middleware returned nil without enqueueing the tasks", "middleware")

// store writes resolved tasks with one statement.
func (c *Client) store(ctx context.Context, q querier, tasks []*resolvedTask) ([]EnqueueResult, error) {
	b := enqueueBatch{due: make(map[string]bool), plain: true}
	pos := make([]int, len(tasks))     // the batch row of tasks[i]
	repeat := make([]bool, len(tasks)) // tasks[i] repeats an earlier dedupe key of the batch
	rowOf := make(map[string]int)      // the batch row of each dedupe key
	now := time.Now()
	size := 0 // the payload and metadata of the call

	for i, t := range tasks {
		meta, err := encodeMetadata(t.metadata())
		if err != nil {
			return nil, inBatch(err, i, len(tasks))
		}
		if size += len(t.payload) + len(meta); size > maxEnqueueBytes {
			return nil, tooLarge(len(tasks))
		}
		if t.dedupeKey != nil {
			if row, ok := rowOf[string(t.dedupeKey)]; ok {
				pos[i], repeat[i] = row, true
				continue
			}
			rowOf[string(t.dedupeKey)] = len(b.queues)
		}
		pos[i] = len(b.queues)
		b.add(t, meta, now)
	}

	results, err := c.insertWithRetry(ctx, q, &b)
	if err != nil {
		return nil, err
	}

	out := make([]EnqueueResult, len(tasks))
	for i := range tasks {
		out[i] = results[pos[i]]
		out[i].Duplicate = out[i].Duplicate || repeat[i]
	}
	return out, nil
}

// tooLarge fails a call of n tasks over the limit of its payload and
// metadata.
func tooLarge(n int) error {
	return invalidArgument("%d tasks hold more than the limit of %d bytes of payload and metadata in one call",
		n, maxEnqueueBytes)
}

// inBatch says which task of a batch of n an error is about.
func inBatch(err error, i, n int) error {
	if n == 1 {
		return err
	}
	return fmt.Errorf("%w (task %d of %d)", err, i+1, n)
}

// insertWithRetry runs the enqueue statement on a batch, and runs it again
// after a deadlock when it is a transaction of its own, on the client's
// pool. It then finds the tasks of the duplicates that the statement could
// not see.
func (c *Client) insertWithRetry(ctx context.Context, q querier, b *enqueueBatch) ([]EnqueueResult, error) {
	notify := make([]string, 0, len(b.due))
	for queue := range b.due {
		if c.notifier.allow(ctx, queue) {
			notify = append(notify, queue)
		}
	}

	// Only on the client's own pool is the enqueue statement known to be a
	// transaction of its own: any other executor may be in the caller's.
	ownTx := q == querier(pgxExecutor{q: c.pool})
	var results []EnqueueResult
	var missing [][]byte // duplicates whose existing task the statement could not see
	var err error
	for try := 1; ; try++ {
		results, missing, err = c.insertTasks(ctx, q, b, notify)
		// PostgreSQL may end the statement to end a deadlock that other
		// transactions opened: the enqueue only waited between them, for a
		// group's row or a dedupe key. On the client's own pool, the
		// statement is a transaction of its own, so nothing of it stays,
		// and it runs again, as Cancel does: see inLockOrder. In the
		// caller's transaction it cannot: that transaction is over.
		if try == deadlockTries || !isDeadlock(err) || !ownTx {
			break
		}
	}
	if err != nil {
		return nil, enqueueError(err)
	}
	if len(missing) > 0 {
		if err := c.resolveDuplicates(ctx, q, b, results, missing); err != nil {
			if !ownTx {
				return nil, err // the caller rolls back
			}
			// The tasks are committed, and the read only fills in IDs. An
			// error would say that nothing was stored, and a caller that
			// then enqueues again would store the tasks without a dedupe
			// key twice.
			c.logger.LogAttrs(ctx, slog.LevelWarn, "find duplicates failed after the enqueue committed; the duplicates have no ID",
				slog.Int(logKeyTasks, len(missing)), slog.Any(logKeyError, err))
		}
	}
	return results, nil
}

// insertTasks runs the enqueue statement on a batch, and returns its results
// in the batch's order, with the dedupe keys of the duplicates whose
// existing task the statement could not see.
func (c *Client) insertTasks(ctx context.Context, q querier, b *enqueueBatch, notify []string) (results []EnqueueResult, missing [][]byte, err error) {
	stmt := c.sql.enqueue
	if b.plain {
		stmt = c.sql.enqueuePlain
	}
	r, err := q.query(ctx, stmt,
		b.queues,           // $1
		b.kinds,            // $2
		b.priorities,       // $3
		b.runAts,           // $4
		b.maxAttempts,      // $5
		nullable(b.groups), // $6
		b.dedupeKeys,       // $7
		b.dedupeWindows,    // $8
		b.payloads,         // $9
		b.metadata,         // $10
		c.schema,           // $11
		notify,             // $12
	)
	if err != nil {
		return nil, nil, err
	}
	defer r.Close()

	results = make([]EnqueueResult, len(b.queues))
	for r.Next() {
		var ord, id int64
		var duplicate bool
		if err := r.Scan(&ord, &id, &duplicate); err != nil {
			return nil, nil, err
		}
		results[ord-1] = EnqueueResult{ID: id, Duplicate: duplicate}
		if duplicate && id == 0 {
			missing = append(missing, b.dedupeKeys[ord-1])
		}
	}
	return results, missing, r.Err()
}

// enqueueError wraps an error of the enqueue statement. A data exception
// means that PostgreSQL rejected a value that the checks before it let
// through, such as an unpaired surrogate escape in the payload: retrying
// cannot help.
func enqueueError(err error) error {
	if strings.HasPrefix(pgCode(err), codeDataException) {
		return fmt.Errorf("%w: the database rejected a value: %w", ErrInvalidArgument, err)
	}
	return fmt.Errorf("taskmill: enqueue: %w", err)
}

// resolveDuplicates finds the existing tasks of dedupe keys that a concurrent
// enqueue committed while the enqueue statement ran, and sets their IDs in
// results. A key that has no row any more, because its task was canceled or
// deleted meanwhile, leaves its duplicates with the ID 0. So does every key
// if the read fails.
func (c *Client) resolveDuplicates(ctx context.Context, q querier, b *enqueueBatch, results []EnqueueResult, keys [][]byte) error {
	fail := func(err error) error { return fmt.Errorf("taskmill: enqueue: find duplicates: %w", err) }
	r, err := q.query(ctx, c.sql.dedupeLookup, keys)
	if err != nil {
		return fail(err)
	}
	defer r.Close()
	existing := make(map[string]int64, len(keys))
	for r.Next() {
		var key []byte
		var id int64
		if err := r.Scan(&key, &id); err != nil {
			return fail(err)
		}
		existing[string(key)] = id
	}
	if err := r.Err(); err != nil {
		return fail(err)
	}
	for i := range results {
		if results[i].Duplicate && results[i].ID == 0 {
			results[i].ID = existing[string(b.dedupeKeys[i])]
		}
	}
	return nil
}

// resolvedTask is one task with its options resolved and validated.
type resolvedTask struct {
	queue, kind, group string
	priority           Priority
	maxAttempts        int
	runAt              time.Time
	dedupeKey          []byte
	dedupeWindow       time.Duration
	payload            []byte
	meta               map[string]string // may belong to the caller or the kind: never modified
	reservedMeta       map[string]string // taskmill's own metadata, of a schedule's run: stored over meta
}

// metadata returns the task's metadata as it is stored: meta, and over it
// reservedMeta. Without reserved keys, it is meta itself, which the caller
// must not change.
func (t *resolvedTask) metadata() map[string]string {
	if len(t.reservedMeta) == 0 {
		return t.meta
	}
	return t.metadataCopy()
}

// metadataCopy returns the task's metadata as it is stored, in a new map.
func (t *resolvedTask) metadataCopy() map[string]string {
	all := make(map[string]string, len(t.meta)+len(t.reservedMeta))
	maps.Copy(all, t.meta)
	maps.Copy(all, t.reservedMeta)
	return all
}

// resolve merges a task's options over its kind's defaults and taskmill's,
// and validates the result.
func resolve(p EnqueueParams) (*resolvedTask, error) {
	if p.Args == nil {
		return nil, invalidArgument("args is nil")
	}
	kind := p.Args.Kind()
	if err := checkName("kind", kind, maxNameLength); err != nil {
		return nil, err
	}

	var o EnqueueOptions
	if d, ok := p.Args.(ArgsWithDefaults); ok {
		o = d.EnqueueDefaults()
	}
	if p.Options != nil {
		o = mergeOptions(o, *p.Options)
	}

	t := &resolvedTask{
		queue:        o.Queue,
		kind:         kind,
		group:        o.GroupKey,
		priority:     o.Priority,
		maxAttempts:  o.MaxAttempts,
		runAt:        o.RunAt,
		dedupeWindow: o.DedupeWindow,
	}
	if t.queue == "" {
		t.queue = DefaultQueue
	}
	if t.priority == 0 {
		t.priority = PriorityNormal
	}
	if t.maxAttempts == 0 {
		t.maxAttempts = defaultMaxAttempts
	}
	if t.dedupeWindow == 0 {
		t.dedupeWindow = defaultDedupeWindow
	}

	if err := t.validate(); err != nil {
		return nil, err
	}

	if o.DedupeKey != "" {
		sum := sha256.Sum256([]byte(t.queue + "\x00" + t.kind + "\x00" + o.DedupeKey))
		t.dedupeKey = sum[:]
	}

	payload, err := encodePayload(kind, p.Args)
	if err != nil {
		return nil, err
	}
	t.payload = payload

	t.meta = o.Metadata
	return t, nil
}

func (t *resolvedTask) validate() error {
	if err := checkName("queue", t.queue, maxNameLength); err != nil {
		return err
	}
	if t.group != "" {
		if err := checkName("group key", t.group, maxGroupKeyLength); err != nil {
			return err
		}
	}
	if t.priority < PriorityLow || t.priority > PriorityCritical {
		return invalidArgument("priority %d is out of range", t.priority)
	}
	if t.maxAttempts < 1 || t.maxAttempts > maxAttemptsLimit {
		return invalidArgument("max attempts %d must be between 1 and %d", t.maxAttempts, maxAttemptsLimit)
	}
	if t.dedupeWindow < 0 {
		return invalidArgument("dedupe window must not be negative")
	}
	return checkTime("run time", t.runAt)
}

// add appends t, with its encoded metadata, to the batch. now is the batch's
// time, which decides whether t is due right away.
func (b *enqueueBatch) add(t *resolvedTask, metadata []byte, now time.Time) {
	b.queues = append(b.queues, t.queue)
	b.kinds = append(b.kinds, t.kind)
	b.groups = append(b.groups, t.group)
	b.priorities = append(b.priorities, int16(t.priority))
	b.maxAttempts = append(b.maxAttempts, int16(t.maxAttempts)) //nolint:gosec // validated to at most maxAttemptsLimit
	b.runAts = append(b.runAts, timePtr(t.runAt))
	b.dedupeKeys = append(b.dedupeKeys, t.dedupeKey)
	b.dedupeWindows = append(b.dedupeWindows, t.dedupeWindow.Microseconds())
	b.payloads = append(b.payloads, string(t.payload))
	b.metadata = append(b.metadata, string(metadata))
	b.plain = b.plain && t.dedupeKey == nil && t.group == ""
	if !t.runAt.After(now) {
		b.due[t.queue] = true
	}
}

// mergeOptions returns base with every field that override sets replaced.
func mergeOptions(base, override EnqueueOptions) EnqueueOptions {
	if override.Queue != "" {
		base.Queue = override.Queue
	}
	if override.Priority != 0 {
		base.Priority = override.Priority
	}
	if !override.RunAt.IsZero() {
		base.RunAt = override.RunAt
	}
	if override.MaxAttempts != 0 {
		base.MaxAttempts = override.MaxAttempts
	}
	if override.GroupKey != "" {
		base.GroupKey = override.GroupKey
	}
	if override.DedupeKey != "" {
		base.DedupeKey = override.DedupeKey
	}
	if override.DedupeWindow != 0 {
		base.DedupeWindow = override.DedupeWindow
	}
	if override.Metadata != nil {
		merged := maps.Clone(base.Metadata)
		if merged == nil {
			merged = make(map[string]string, len(override.Metadata))
		}
		maps.Copy(merged, override.Metadata)
		base.Metadata = merged
	}
	return base
}

// timePtr returns nil for the zero time, which the database stores as NULL.
func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// nullable turns empty strings into NULLs.
func nullable(ss []string) []*string {
	out := make([]*string, len(ss))
	for i := range ss {
		if ss[i] != "" {
			out[i] = &ss[i]
		}
	}
	return out
}
