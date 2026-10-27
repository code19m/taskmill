package taskmill

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgconn"
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
type EnqueueOptions struct {
	// Queue is the queue to enqueue into, at most 128 characters.
	// Default: [DefaultQueue].
	Queue string

	// Priority orders tasks within the queue. Default: [PriorityNormal].
	Priority Priority

	// RunAt is the earliest time the task may start. Default: now.
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
	// frees the key at once. Default: none.
	DedupeKey string

	// DedupeWindow is how long DedupeKey blocks duplicates. Default: 24 hours.
	DedupeWindow time.Duration

	// Metadata is stored with the task and passed to its handler, for
	// example for tracing. Encoded as JSON, it may take at most 64 KiB,
	// including what [EnqueueMiddleware] adds. Default: none.
	Metadata map[string]string
}

// EnqueueParams is one task in a batch enqueue. Options may be nil.
type EnqueueParams struct {
	Args    Args
	Options *EnqueueOptions
}

// EnqueueResult reports the outcome of enqueueing one task.
type EnqueueResult struct {
	// ID identifies the task: the new one, or the existing one for a
	// duplicate.
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
type EnqueueMiddleware func(ctx context.Context, tasks []*EnqueueTask, next func(context.Context) error) error

// EnqueueTask is a task being enqueued, as [EnqueueMiddleware] sees it.
// Changes to fields other than Metadata are ignored.
type EnqueueTask struct {
	Queue string
	Kind  string

	// Metadata holds the task's metadata, from [EnqueueOptions.Metadata]. It
	// is never nil, and middleware may change it before calling next.
	Metadata map[string]string

	// Result is set once next returns nil.
	Result EnqueueResult
}

// Enqueue enqueues one task in its own transaction. opts may be nil.
func (c *Client) Enqueue(ctx context.Context, args Args, opts *EnqueueOptions) (EnqueueResult, error) {
	res, err := c.enqueue(ctx, pgxQuerier{q: c.pool}, []EnqueueParams{{Args: args, Options: opts}})
	if err != nil {
		return EnqueueResult{}, err
	}
	return res[0], nil
}

// EnqueueTx enqueues one task in the caller's transaction: the task exists
// only if that transaction commits. opts may be nil.
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

// EnqueueMany enqueues several tasks in one round trip and one transaction.
// Results are in the same order as params. One call takes at most 10,000
// tasks, with at most 64 MiB of payload and metadata in all.
func (c *Client) EnqueueMany(ctx context.Context, params []EnqueueParams) ([]EnqueueResult, error) {
	return c.enqueue(ctx, pgxQuerier{q: c.pool}, params)
}

// EnqueueManyTx is [Client.EnqueueMany] in the caller's transaction.
func (c *Client) EnqueueManyTx(ctx context.Context, tx Executor, params []EnqueueParams) ([]EnqueueResult, error) {
	q, err := executorQuerier(tx)
	if err != nil {
		return nil, err
	}
	return c.enqueue(ctx, q, params)
}

// executorQuerier returns the querier of an Executor for a single statement,
// which may run on a pool or connection as well as in a transaction.
func executorQuerier(tx Executor) (querier, error) {
	if tx == nil {
		return nil, invalidArgument("executor is nil")
	}
	q := tx.querier()
	if q == nil {
		return nil, invalidArgument("executor has no querier; create it with Pgx or SQL")
	}
	return q, nil
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
	if len(c.enqueueMiddleware) == 0 {
		return c.store(ctx, q, tasks)
	}
	return c.storeThrough(ctx, q, tasks)
}

// storeThrough stores tasks through the enqueue middleware.
func (c *Client) storeThrough(ctx context.Context, q querier, tasks []*resolvedTask) ([]EnqueueResult, error) {
	views := make([]*EnqueueTask, len(tasks))
	for i, t := range tasks {
		meta := make(map[string]string, len(t.meta)) // t.meta may belong to the caller
		maps.Copy(meta, t.meta)
		views[i] = &EnqueueTask{Queue: t.queue, Kind: t.kind, Metadata: meta}
	}
	var results []EnqueueResult
	var called atomic.Bool
	next := func(ctx context.Context) error {
		if called.Swap(true) {
			return errors.New("taskmill: enqueue middleware called next twice")
		}
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
	}
	// A middleware that returns nil without calling its next fails at once,
	// so that the middleware outside it sees the failure too.
	for _, m := range slices.Backward(c.enqueueMiddleware) {
		inner := next
		next = func(ctx context.Context) error {
			var reached atomic.Bool
			err := m(ctx, views, func(ctx context.Context) error {
				reached.Store(true)
				return inner(ctx)
			})
			if err == nil && !reached.Load() {
				return errNotEnqueued
			}
			return err
		}
	}
	err := next(ctx)
	switch {
	case results == nil && err == nil: // a middleware hid the error of its next
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

// errNotEnqueued fails an enqueue whose middleware returned nil, although
// the tasks were not stored.
var errNotEnqueued = errors.New("taskmill: enqueue middleware returned nil without enqueueing the tasks")

// store writes resolved tasks with one statement.
func (c *Client) store(ctx context.Context, q querier, tasks []*resolvedTask) ([]EnqueueResult, error) {
	b := enqueueBatch{due: make(map[string]bool), plain: true}
	pos := make([]int, len(tasks))     // the batch row of tasks[i]
	repeat := make([]bool, len(tasks)) // tasks[i] repeats an earlier dedupe key of the batch
	rowOf := make(map[string]int)      // the batch row of each dedupe key
	now := time.Now()
	size := 0 // the payload and metadata of the call

	for i, t := range tasks {
		meta, err := encodeMetadata(t.meta)
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

	results, err := c.runEnqueue(ctx, q, &b)
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

func (c *Client) runEnqueue(ctx context.Context, q querier, b *enqueueBatch) ([]EnqueueResult, error) {
	notify := make([]string, 0, len(b.due))
	for queue := range b.due {
		if c.notifier.allow(ctx, queue) {
			notify = append(notify, queue)
		}
	}

	stmt := c.sql.enqueue
	if b.plain {
		stmt = c.sql.enqueuePlain
	}
	r, err := q.query(ctx, stmt,
		b.queues, b.kinds, b.priorities, b.runAts, b.maxAttempts, nullable(b.groups),
		b.dedupeKeys, b.dedupeWindows, b.payloads, b.metadata, c.schema, notify)
	if err != nil {
		return nil, enqueueError(err)
	}
	defer r.Close()

	results := make([]EnqueueResult, len(b.queues))
	var missing [][]byte // duplicates whose existing task the statement could not see
	for r.Next() {
		var ord, id int64
		var duplicate bool
		if err := r.Scan(&ord, &id, &duplicate); err != nil {
			return nil, enqueueError(err)
		}
		results[ord-1] = EnqueueResult{ID: id, Duplicate: duplicate}
		if duplicate && id == 0 {
			missing = append(missing, b.dedupeKeys[ord-1])
		}
	}
	if err := r.Err(); err != nil {
		return nil, enqueueError(err)
	}
	r.Close()

	if len(missing) > 0 {
		if err := c.resolveDuplicates(ctx, q, b, results, missing); err != nil {
			return nil, err
		}
	}
	return results, nil
}

// enqueueError wraps an error of the enqueue statement. A data exception
// means that PostgreSQL rejected a value that the checks before it let
// through, such as an unpaired surrogate escape in the payload: retrying
// cannot help.
func enqueueError(err error) error {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && strings.HasPrefix(pgErr.Code, "22") { // data_exception
		return fmt.Errorf("%w: the database rejected a value: %w", ErrInvalidArgument, err)
	}
	return fmt.Errorf("taskmill: enqueue: %w", err)
}

// resolveDuplicates finds the existing tasks of dedupe keys that a concurrent
// enqueue committed while the enqueue statement ran.
func (c *Client) resolveDuplicates(ctx context.Context, q querier, b *enqueueBatch, results []EnqueueResult, keys [][]byte) error {
	r, err := q.query(ctx, c.sql.dedupeLookup, keys)
	if err != nil {
		return fmt.Errorf("taskmill: enqueue: find duplicates: %w", err)
	}
	defer r.Close()
	existing := make(map[string]int64, len(keys))
	for r.Next() {
		var key []byte
		var id int64
		if err := r.Scan(&key, &id); err != nil {
			return fmt.Errorf("taskmill: enqueue: find duplicates: %w", err)
		}
		existing[string(key)] = id
	}
	if err := r.Err(); err != nil {
		return fmt.Errorf("taskmill: enqueue: find duplicates: %w", err)
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

// encodeMetadata encodes a task's metadata as JSON, which jsonb can store.
func encodeMetadata(meta map[string]string) ([]byte, error) {
	if len(meta) == 0 {
		return []byte("{}"), nil
	}
	text, err := encodeJSON(meta)
	if err != nil {
		return nil, invalidArgument("encode metadata: %v", err)
	}
	if len(text) > maxMetadataBytes {
		return nil, invalidArgument("metadata is %d bytes, more than the limit of %d", len(text), maxMetadataBytes)
	}
	if hasNUL(text) {
		return nil, invalidArgument("metadata contains a NUL character")
	}
	return text, nil
}

// encodeJSON encodes v as JSON without escaping &, <, and >, which jsonb
// stores as they are, so that the size limits count what is stored.
// encoding/json still escapes U+2028 and U+2029.
func encodeJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// encodePayload encodes args as JSON, and checks that jsonb can store it
// within the payload limit. The checks run before the query, so a payload
// that fails them leaves the caller's transaction usable.
func encodePayload(kind string, args Args) ([]byte, error) {
	payload, err := encodeJSON(args)
	if err != nil {
		return nil, invalidArgument("encode payload of kind %q: %v", kind, err)
	}
	size, numbersFit := storedSize(payload)
	switch {
	case bytes.Equal(payload, []byte("null")):
		return nil, invalidArgument("payload of kind %q encodes to null", kind)
	case size > maxPayloadBytes:
		return nil, invalidArgument("payload of kind %q is more than %d bytes as stored", kind, maxPayloadBytes)
	case !numbersFit:
		return nil, invalidArgument("payload of kind %q holds a number beyond the range of PostgreSQL's numeric type", kind)
	case !utf8.Valid(payload):
		return nil, invalidArgument("payload of kind %q is not valid UTF-8", kind)
	case hasNUL(payload):
		return nil, invalidArgument("payload of kind %q contains a NUL character", kind)
	}
	return payload, nil
}

// Limits of PostgreSQL's numeric type, in which jsonb stores numbers.
const (
	maxNumericWhole = 131_072 // digits before the point
	maxNumericScale = 16_383  // digits after the point
)

// storedSize returns at most the size of JSON text as PostgreSQL writes it
// out of jsonb, where each number is written in full: 1e6 becomes 1000000,
// so a short payload could grow past any limit, up to values that PostgreSQL
// cannot read back. Strings are written as they came. jsonb also adds a
// space after each comma and colon; that at most doubles the size, and the
// limit leaves it out, as it always has. Counting stops past the limit.
// numbersFit reports whether every number counted fits PostgreSQL's numeric
// type.
func storedSize(text []byte) (size int, numbersFit bool) {
	size, numbersFit = len(text), true
	inString := false
	for i := 0; i < len(text) && size <= maxPayloadBytes && numbersFit; i++ {
		switch c := text[i]; {
		case inString && c == '\\':
			i++ // the escaped byte cannot end the string
		case c == '"':
			inString = !inString
		case !inString && (c == '-' || '0' <= c && c <= '9'):
			end := i + 1
			for end < len(text) && strings.IndexByte("+-.0123456789Ee", text[end]) >= 0 {
				end++
			}
			var n int
			n, numbersFit = numberSize(text[i:end])
			size += n - (end - i)
			i = end - 1
		}
	}
	return size, numbersFit
}

// numberSize returns at most the length of a JSON number as PostgreSQL
// writes it: without an exponent, and with as many digits after the point as
// the number has. fits reports whether the number fits PostgreSQL's numeric
// type.
func numberSize(num []byte) (size int, fits bool) {
	digits := bytes.TrimPrefix(num, []byte("-"))
	sign := len(num) - len(digits)
	exp := 0
	if at := bytes.IndexAny(digits, "eE"); at >= 0 {
		var err error
		if exp, err = strconv.Atoi(string(digits[at+1:])); err != nil {
			return 0, false // beyond the range of int
		}
		digits = digits[:at]
	}
	// Past this bound, a number is beyond numeric's range, or a zero; within
	// it, the sums below cannot overflow.
	bound := len(digits) + maxNumericWhole + 1
	exp = max(-bound, min(exp, bound))
	whole, frac, _ := bytes.Cut(digits, []byte("."))
	zeros := len(whole) - len(bytes.TrimLeft(whole, "0"))
	if zeros == len(whole) {
		zeros += len(frac) - len(bytes.TrimLeft(frac, "0"))
	}
	before := 1 // digits before the point
	if zeros < len(whole)+len(frac) {
		before = max(1, len(whole)+exp-zeros)
	}
	after := max(0, len(frac)-exp)
	size = sign + before
	if after > 0 {
		size += 1 + after
	}
	return size, before <= maxNumericWhole && after <= maxNumericScale
}

// hasNUL reports whether JSON text encodes a NUL character, which jsonb
// cannot store. An escape is one only if an odd number of backslashes leads
// to it: in \\u0000 the backslash itself is escaped.
func hasNUL(text []byte) bool {
	for i := 0; ; {
		j := bytes.Index(text[i:], []byte("u0000"))
		if j < 0 {
			return false
		}
		j += i
		slashes := 0
		for k := j - 1; k >= 0 && text[k] == '\\'; k-- {
			slashes++
		}
		if slashes%2 == 1 {
			return true
		}
		i = j + 1
	}
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
	return nil
}

// add appends t, with its encoded metadata, to the batch. now is the batch's
// time, which decides whether t is due at once.
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

// checkName validates a queue name, kind, or group key.
func checkName(what, s string, maxLen int) error {
	switch {
	case s == "":
		return invalidArgument("%s is empty", what)
	case !utf8.ValidString(s):
		return invalidArgument("%s is not valid UTF-8", what)
	case strings.ContainsRune(s, 0):
		return invalidArgument("%s contains a NUL character", what)
	case utf8.RuneCountInString(s) > maxLen:
		return invalidArgument("%s %q is longer than %d characters", what, s, maxLen)
	}
	return nil
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
