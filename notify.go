package taskmill

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// notifyWindow is the shortest gap between two batches of notifications
// from one client, and between two notifications for one queue in the
// transactions of its callers: see notifier.
const notifyWindow = 50 * time.Millisecond

// minForget is the fewest queues at which a notifier looks for those it can
// forget: below it, remembering them all costs less than looking.
const minForget = 64

// Notifications on an installation's channel, named after its schema, carry
// "<kind>|<argument>". Any role in the database can send one, so a
// notification is only a hint: before a worker changes anything, it reads
// what the tables say.
const (
	notifyAvailable = "a" // tasks became available in queue <argument>
	notifyCancel    = "c" // the cancellation of the running task with ID <argument> was requested
	notifyQueue     = "q" // the settings of queue <argument> changed
	notifyHeartbeat = "h" // the maintainer woke the queues whose tasks wait; no argument
	notifyResigned  = "r" // the maintainer resigned; no argument

	notifySQL = "SELECT pg_notify($1, $2)"

	// notifyQueuesSQL notifies channel $1 of available tasks in each queue
	// of $2, in one transaction: PostgreSQL wakes each listener once for
	// all of them.
	notifyQueuesSQL = "SELECT pg_notify($1, '" + notifyAvailable + "|' || q) FROM unnest($2::text[]) AS q"
)

// notifier sends a client's "tasks available" notifications. Each
// notifying transaction takes a lock of the whole server at its commit, and
// holds it while the commit flushes, so notifications must not come with
// every write.
//
// The client's own writes, on its pool, leave their notifications to the
// notifier once they commit (see ownQuerier). It sends them in batches, each
// a transaction of its own that writes nothing, so its commit flushes
// nothing and holds the lock for a moment: the first right away, and the next
// when the window after it ends, with all that arrived meanwhile.
//
// A write in the caller's transaction notifies in that transaction, which
// is the only way to notify at its commit, at most once per queue per
// window: allow holds the notifications back that come sooner, and adds
// them to the batch when the window ends.
type notifier struct {
	pool    *pgxpool.Pool
	channel string
	logger  *slog.Logger

	mu       sync.Mutex
	last     map[string]time.Time   // the last notification of each queue, until forget drops it
	held     map[string]*time.Timer // adds the notification that allow held back for each queue to the batch
	pending  map[string]struct{}    // the queues of the next batch
	sent     time.Time              // when the last batch went
	busy     bool                   // a batch is scheduled or being sent
	forgetAt int                    // the size of last at which forget next looks through it
	closed   bool                   // close was called: each notification goes right away
	sending  sync.WaitGroup         // batches being sent
}

func newNotifier(pool *pgxpool.Pool, channel string, logger *slog.Logger) *notifier {
	return &notifier{
		pool:    pool,
		channel: channel,
		logger:  logger,
		last:    make(map[string]time.Time),
		held:    make(map[string]*time.Timer),
		pending: make(map[string]struct{}),
	}
}

// allow reports whether a statement in the caller's transaction may notify
// queue itself now. If not, the notification is held back until the
// window after the queue's last one ends, and then goes with the next
// batch: by then, the transaction has most likely committed.
func (n *notifier) allow(queue string) bool {
	now := time.Now()

	n.mu.Lock()
	defer n.mu.Unlock()

	if elapsed := now.Sub(n.last[queue]); elapsed >= notifyWindow || n.closed {
		n.forget(now)
		n.last[queue] = now
		return true
	}
	if n.held[queue] == nil {
		n.held[queue] = time.AfterFunc(notifyWindow-now.Sub(n.last[queue]), func() { n.release(queue) })
	}
	return false
}

// release adds the notification that allow held back for queue to the next
// batch.
func (n *notifier) release(queue string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.closed { // close sent it
		return
	}
	delete(n.held, queue)
	n.pending[queue] = struct{}{}
	n.schedule()
}

// after notifies queues of the tasks that a committed write of the client
// made available, with the next batch. It does not wait for the batch,
// unless the notifier is closed: then it sends them itself.
func (n *notifier) after(queues []string) {
	if len(queues) == 0 {
		return
	}

	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		n.send(queues)
		return
	}
	for _, queue := range queues {
		n.pending[queue] = struct{}{}
	}
	n.schedule()
	n.mu.Unlock()
}

// schedule starts the next batch unless one is under way: right away if the
// last one went a window ago, and else when the window ends. n.mu is held.
func (n *notifier) schedule() {
	if n.busy || len(n.pending) == 0 {
		return
	}
	n.busy = true
	time.AfterFunc(time.Until(n.sent.Add(notifyWindow)), n.flush)
}

// flush sends the batch, and schedules the next one if more notifications
// arrived meanwhile. One batch at a time is sent, so a slow database holds
// one connection of the pool for them.
func (n *notifier) flush() {
	n.mu.Lock()
	if n.closed { // close sent the batch
		n.mu.Unlock()
		return
	}
	n.sent = time.Now()
	n.forget(n.sent)
	queues := n.take()
	for _, queue := range queues {
		n.last[queue] = n.sent
	}
	n.sending.Add(1)
	n.mu.Unlock()
	defer n.sending.Done()

	n.send(queues)

	n.mu.Lock()
	defer n.mu.Unlock()
	n.busy = false
	if !n.closed {
		n.schedule()
	}
}

// take empties the batch, and returns its queues. n.mu is held.
func (n *notifier) take() []string {
	queues := slices.Collect(maps.Keys(n.pending))
	clear(n.pending)
	return queues
}

// send notifies queues of available tasks, in a transaction of its own.
// Such a transaction writes nothing, so its commit does not flush, and it
// holds the server's notify lock only for a moment. A batch stands for the
// committed writes of many callers, so no caller's context bounds it.
//
//nolint:contextcheck // a batch outlives the calls whose writes it notifies of
func (n *notifier) send(queues []string) {
	if len(queues) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	if _, err := n.pool.Exec(ctx, notifyQueuesSQL, n.channel, queues); err != nil {
		n.logger.LogAttrs(ctx, slog.LevelWarn, "notification failed",
			slog.Any(logKeyQueues, queues), slog.Any(logKeyError, err))
	}
}

// close sends the notifications that are pending or held back, waits for
// a batch that is being sent, and makes the notifier send each later one
// right away.
func (n *notifier) close() {
	n.mu.Lock()
	n.closed = true
	for queue, t := range n.held {
		t.Stop()
		n.pending[queue] = struct{}{}
	}
	clear(n.held)
	queues := n.take()
	n.mu.Unlock()

	n.send(queues)
	n.sending.Wait()
}

// forget drops the queues whose last notification is older than the window:
// they hold no notification back any more. A client may enqueue into ever
// new queues, such as one for each tenant, and would otherwise remember them
// all. It looks through the queues only when their number has doubled since
// it last did, so that it costs each notification a constant.
func (n *notifier) forget(now time.Time) {
	if len(n.last) < n.forgetAt {
		return
	}
	for queue, at := range n.last {
		if now.Sub(at) >= notifyWindow {
			delete(n.last, queue)
		}
	}
	n.forgetAt = max(2*len(n.last), minForget)
}

// ownQuerier runs the client's own statements: on its pool or one of its
// connections, where each statement commits as it ends, or in a transaction
// that the client began on them, which inTx runs. Its statements notify
// nobody of available tasks: madeAvailable leaves their queues to the
// notifier once they commit.
type ownQuerier struct {
	pgxExecutor
	notifier  *notifier
	available *[]string // the queues to notify once the transaction commits; nil outside a transaction
}

// own returns an ownQuerier on db, the client's pool or one of its
// connections, outside a transaction.
func (c *Client) own(db PgxQuerier) ownQuerier {
	return ownQuerier{pgxExecutor: pgxExecutor{q: db}, notifier: c.notifier}
}

// inTx runs fn in a transaction on db, the client's pool or one of its
// connections, and notifies the queues in which it made tasks available
// once it commits.
func (c *Client) inTx(ctx context.Context, db interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}, fn func(q querier) error,
) error {
	var available []string
	err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		return fn(ownQuerier{pgxExecutor: pgxExecutor{q: tx}, notifier: c.notifier, available: &available})
	})
	if err == nil {
		c.notifier.after(available)
	}
	return err
}

// notifiable returns, once each, the queues of queues that a statement in
// q's transaction must notify itself of the tasks it makes available: none
// on an ownQuerier, and else those that the throttle allows now. Once the
// statement succeeded, madeAvailable takes the queues.
func (c *Client) notifiable(q querier, queues []string) []string {
	if _, own := q.(ownQuerier); own {
		return nil
	}
	var notify []string
	for _, queue := range queues {
		if !slices.Contains(notify, queue) && c.notifier.allow(queue) {
			notify = append(notify, queue)
		}
	}
	return notify
}

// madeAvailable takes the queues in which a statement on q, which
// succeeded, made tasks available. An ownQuerier notifies them once the
// statement commits: right away outside a transaction, and else with the
// transaction's commit. On any other querier, the statement notified those
// that notifiable returned.
func madeAvailable(q querier, queues ...string) {
	switch own, ok := q.(ownQuerier); {
	case !ok:
	case own.available == nil:
		own.notifier.after(queues)
	default:
		*own.available = append(*own.available, queues...)
	}
}
