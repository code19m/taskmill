package taskmill

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// notifyWindow is the shortest gap between two notifications for one queue
// from one client. NOTIFY serializes committing transactions, so sending one
// per enqueue caps throughput.
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
)

// notifier throttles "tasks available" notifications per queue. The first
// enqueue in a window notifies inside its own transaction; later ones in the
// same window share one trailing notification when the window ends.
type notifier struct {
	pool    *pgxpool.Pool
	channel string
	logger  *slog.Logger

	mu       sync.Mutex
	last     map[string]time.Time // last notification per queue, until forget drops it
	trailing map[string]bool      // the queues with a trailing notification scheduled
	forgetAt int                  // the size of last at which forget next looks through it
}

func newNotifier(pool *pgxpool.Pool, channel string, logger *slog.Logger) *notifier {
	return &notifier{
		pool:     pool,
		channel:  channel,
		logger:   logger,
		last:     make(map[string]time.Time),
		trailing: make(map[string]bool),
	}
}

// allow reports whether the caller may notify queue now, inside its own
// transaction. If not, it schedules a trailing notification instead, which
// keeps ctx's values but not its cancellation.
func (n *notifier) allow(ctx context.Context, queue string) bool {
	now := time.Now()

	n.mu.Lock()
	defer n.mu.Unlock()

	if elapsed := now.Sub(n.last[queue]); elapsed >= notifyWindow {
		n.forget(now)
		n.last[queue] = now
		return true
	}
	if !n.trailing[queue] {
		n.trailing[queue] = true
		detached := context.WithoutCancel(ctx)
		time.AfterFunc(notifyWindow-now.Sub(n.last[queue]), func() { n.sendTrailing(detached, queue) })
	}
	return false
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

// sendTrailing sends the notification that allow deferred.
func (n *notifier) sendTrailing(ctx context.Context, queue string) {
	n.mu.Lock()
	delete(n.trailing, queue)
	n.last[queue] = time.Now()
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, cleanupTimeout)
	defer cancel()
	if _, err := n.pool.Exec(ctx, notifySQL, n.channel, notifyAvailable+"|"+queue); err != nil {
		n.logger.LogAttrs(ctx, slog.LevelWarn, "notification failed",
			slog.String(logKeyQueue, queue), slog.Any(logKeyError, err))
	}
}

// notifiable returns, once each, the queues that the throttle allows to
// notify now of available tasks.
func (c *Client) notifiable(ctx context.Context, queues []string) []string {
	var notify []string
	for _, queue := range queues {
		if !slices.Contains(notify, queue) && c.notifier.allow(ctx, queue) {
			notify = append(notify, queue)
		}
	}
	return notify
}

// notifyAvailable announces new available tasks in queue, inside q's
// transaction, when the throttle allows it. In a transaction, a failed
// notification aborts the transaction, so it is returned; outside one, it
// is only logged, to logger.
func (c *Client) notifyAvailable(ctx context.Context, logger *slog.Logger, q querier, queue string) error {
	if !c.notifier.allow(ctx, queue) {
		return nil
	}
	_, err := q.exec(ctx, notifySQL, c.schema, notifyAvailable+"|"+queue)
	if err != nil && !q.inTx() {
		logger.LogAttrs(ctx, slog.LevelWarn, "notification failed",
			slog.String(logKeyQueue, queue), slog.Any(logKeyError, err))
		return nil
	}
	return err
}
