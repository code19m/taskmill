package taskmill

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// notifyWindow is the shortest gap between two notifications for one queue
// from one client. NOTIFY serializes committing transactions, so sending one
// per enqueue caps throughput; see docs/design/performance.md.
const notifyWindow = 50 * time.Millisecond

// notifier throttles "tasks available" notifications per queue. The first
// enqueue in a window notifies inside its own transaction; later ones in the
// same window share one trailing notification when the window ends.
type notifier struct {
	pool    *pgxpool.Pool
	channel string
	logger  *slog.Logger

	mu       sync.Mutex
	last     map[string]time.Time // last notification per queue
	trailing map[string]bool      // trailing notification scheduled per queue
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

// sendTrailing sends the notification that allow deferred.
func (n *notifier) sendTrailing(ctx context.Context, queue string) {
	n.mu.Lock()
	n.trailing[queue] = false
	n.last[queue] = time.Now()
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, cleanupTimeout)
	defer cancel()
	if _, err := n.pool.Exec(ctx, "SELECT pg_notify($1, $2)", n.channel, availablePayload(queue)); err != nil {
		n.logger.LogAttrs(ctx, slog.LevelWarn, "notification failed",
			slog.String(logKeyQueue, queue), slog.Any(logKeyError, err))
	}
}

// availablePayload is the notification payload announcing new available tasks
// in queue.
func availablePayload(queue string) string {
	return "a|" + queue
}
