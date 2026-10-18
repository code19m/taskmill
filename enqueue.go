package taskmill

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"time"
	"unicode/utf8"
)

// DefaultQueue is the queue that tasks go to when neither the enqueue call
// nor the kind's defaults name one.
const DefaultQueue = "default"

// Defaults and limits of enqueue options.
const (
	defaultMaxAttempts  = 10
	defaultDedupeWindow = 24 * time.Hour
	maxNameLength       = 128
	maxGroupKeyLength   = 256
	maxAttemptsLimit    = 10_000
)

// EnqueueOptions control how a task is enqueued. Zero values mean "not set":
// a field set in the call wins over the kind's defaults (see
// [ArgsWithDefaults]), which win over taskmill's defaults.
type EnqueueOptions struct {
	// Queue is the queue to enqueue into. Default: [DefaultQueue].
	Queue string

	// Priority orders tasks within the queue. Default: [PriorityNormal].
	Priority Priority

	// RunAt is the earliest time the task may start. Default: now.
	RunAt time.Time

	// Deadline is the latest time the task may start. If it passes first,
	// the task becomes dead without running. Default: none.
	Deadline time.Time

	// MaxAttempts is how many times the task may run. Default: 10.
	MaxAttempts int

	// GroupKey puts the task in a FIFO group: tasks with the same key in the
	// same queue run one at a time, in enqueue order. Default: none.
	GroupKey string

	// DedupeKey makes the enqueue idempotent: while the key's window lasts,
	// enqueueing the same kind with the same key in the same queue creates no
	// new task, and reports the existing one. Default: none.
	DedupeKey string

	// DedupeWindow is how long DedupeKey blocks duplicates. Default: 24 hours.
	DedupeWindow time.Duration

	// Metadata is stored with the task and passed to its handler, for
	// example for tracing. Default: none.
	Metadata map[string]string
}

// EnqueueParams is one task in a batch enqueue.
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
// Results are in the same order as params.
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
	runAts, deadlines     []*time.Time
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

	b := enqueueBatch{due: make(map[string]bool), plain: true}
	// sent[i] is the batch position of params[i], or -1 for a repeat of an
	// earlier dedupe key in the same batch; firstOf maps it to that earlier one.
	sent := make([]int, len(params))
	firstOf := make(map[string]int)
	now := time.Now()

	for i, p := range params {
		t, err := c.resolve(ctx, p, now)
		if err != nil {
			return nil, fmt.Errorf("taskmill: enqueue task %d of %d: %w", i+1, len(params), err)
		}
		if t.dedupeKey != nil {
			if first, ok := firstOf[string(t.dedupeKey)]; ok {
				sent[i] = -1 - first
				continue
			}
			firstOf[string(t.dedupeKey)] = i
		}
		sent[i] = len(b.queues)
		b.add(t)
	}

	results, err := c.runEnqueue(ctx, q, &b)
	if err != nil {
		return nil, err
	}

	out := make([]EnqueueResult, len(params))
	for i, pos := range sent {
		if pos >= 0 {
			out[i] = results[pos]
			continue
		}
		first := out[-1-pos]
		out[i] = EnqueueResult{ID: first.ID, Duplicate: true}
	}
	return out, nil
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
		b.queues, b.kinds, b.priorities, b.runAts, b.deadlines, b.maxAttempts, nullable(b.groups),
		b.dedupeKeys, b.dedupeWindows, b.payloads, b.metadata, c.schema, notify)
	if err != nil {
		return nil, fmt.Errorf("taskmill: enqueue: %w", err)
	}
	defer r.Close()

	results := make([]EnqueueResult, len(b.queues))
	var missing [][]byte // duplicates whose existing task the statement could not see
	for r.Next() {
		var ord, id int64
		var duplicate bool
		if err := r.Scan(&ord, &id, &duplicate); err != nil {
			return nil, fmt.Errorf("taskmill: enqueue: %w", err)
		}
		results[ord-1] = EnqueueResult{ID: id, Duplicate: duplicate}
		if duplicate && id == 0 {
			missing = append(missing, b.dedupeKeys[ord-1])
		}
	}
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("taskmill: enqueue: %w", err)
	}
	r.Close()

	if len(missing) > 0 {
		if err := c.resolveDuplicates(ctx, q, b, results, missing); err != nil {
			return nil, err
		}
	}
	return results, nil
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
	runAt, deadline    time.Time
	dedupeKey          []byte
	dedupeWindow       time.Duration
	payload, metadata  []byte
}

// resolve merges a task's options over its kind's defaults and taskmill's,
// and validates the result.
func (c *Client) resolve(ctx context.Context, p EnqueueParams, now time.Time) (*resolvedTask, error) {
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
		deadline:     o.Deadline,
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

	if err := t.validate(now); err != nil {
		return nil, err
	}

	if o.DedupeKey != "" {
		sum := sha256.Sum256([]byte(t.queue + "\x00" + t.kind + "\x00" + o.DedupeKey))
		t.dedupeKey = sum[:]
	}

	payload, err := json.Marshal(p.Args)
	if err != nil {
		return nil, invalidArgument("encode payload of kind %q: %v", kind, err)
	}
	if bytes.Equal(payload, []byte("null")) {
		return nil, invalidArgument("payload of kind %q encodes to null", kind)
	}
	if len(payload) > c.maxPayloadBytes {
		return nil, invalidArgument("payload of kind %q is %d bytes, more than the limit of %d",
			kind, len(payload), c.maxPayloadBytes)
	}
	if hasNUL(payload) {
		return nil, invalidArgument("payload of kind %q contains a NUL character", kind)
	}
	t.payload = payload

	meta := o.Metadata
	if c.contextMetadata != nil {
		if fromCtx := c.contextMetadata(ctx); len(fromCtx) > 0 {
			merged := maps.Clone(fromCtx)
			maps.Copy(merged, meta)
			meta = merged
		}
	}
	if len(meta) == 0 {
		t.metadata = []byte("{}")
	} else if t.metadata, err = json.Marshal(meta); err != nil {
		return nil, invalidArgument("encode metadata: %v", err)
	}
	if hasNUL(t.metadata) {
		return nil, invalidArgument("metadata contains a NUL character")
	}
	return t, nil
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

func (t *resolvedTask) validate(now time.Time) error {
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
	if !t.deadline.IsZero() {
		start := t.runAt
		if start.IsZero() {
			start = now
		}
		if !t.deadline.After(start) {
			return invalidArgument("deadline %s is not after the run time %s",
				t.deadline.Format(time.RFC3339), start.Format(time.RFC3339))
		}
	}
	return nil
}

func (b *enqueueBatch) add(t *resolvedTask) {
	b.queues = append(b.queues, t.queue)
	b.kinds = append(b.kinds, t.kind)
	b.groups = append(b.groups, t.group)
	b.priorities = append(b.priorities, int16(t.priority))
	b.maxAttempts = append(b.maxAttempts, int16(t.maxAttempts)) //nolint:gosec // validated to at most maxAttemptsLimit
	b.runAts = append(b.runAts, timePtr(t.runAt))
	b.deadlines = append(b.deadlines, timePtr(t.deadline))
	b.dedupeKeys = append(b.dedupeKeys, t.dedupeKey)
	b.dedupeWindows = append(b.dedupeWindows, t.dedupeWindow.Microseconds())
	b.payloads = append(b.payloads, string(t.payload))
	b.metadata = append(b.metadata, string(t.metadata))
	b.plain = b.plain && t.dedupeKey == nil && t.group == ""
	if t.runAt.IsZero() || !t.runAt.After(time.Now()) {
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
	if !override.Deadline.IsZero() {
		base.Deadline = override.Deadline
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
