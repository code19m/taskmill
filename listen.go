package taskmill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// settingsLoop reloads the installation-wide settings of the worker's queues.
func (w *Worker) settingsLoop(ctx context.Context) {
	tick := time.NewTicker(w.timing.settings)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			w.refreshQueueSettings(ctx)
		}
	}
}

func (w *Worker) refreshQueueSettings(ctx context.Context) {
	if len(w.queues) == 0 {
		return
	}
	names := make([]string, 0, len(w.queues))
	for name := range w.queues {
		names = append(names, name)
	}
	settings, err := w.loadQueueSettings(ctx, names)
	w.settingsFailures.observe(ctx, err)
	if err != nil {
		return
	}
	// A queue without a row has the default settings: running, no limit.
	for name, q := range w.queues {
		s := settings[name]
		q.maxRunning.Store(s.maxRunning)
		if q.paused.Swap(s.paused) && !s.paused {
			q.nudge()
		}
	}
}

// queueSetting is the part of a queue's settings that its worker applies.
type queueSetting struct {
	paused     bool
	maxRunning int32
}

func (w *Worker) loadQueueSettings(ctx context.Context, names []string) (map[string]queueSetting, error) {
	r, err := w.client.pool.Query(ctx, w.client.sql.queueSettings, names)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	settings := make(map[string]queueSetting, len(names))
	for r.Next() {
		var name string
		var s queueSetting
		if err := r.Scan(&name, &s.paused, &s.maxRunning); err != nil {
			return nil, err
		}
		settings[name] = s
	}
	return settings, r.Err()
}

// listenLoop receives notifications on a dedicated connection, reconnecting
// with backoff when it breaks. The backoff starts over after a connection
// that worked.
func (w *Worker) listenLoop(ctx context.Context) {
	backoff := w.timing.reconnectMin
	for ctx.Err() == nil {
		listened, err := w.listenOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if listened {
			backoff = w.timing.reconnectMin
		}
		if errors.Is(err, errListenConfig) || errors.Is(err, errTransactionPooling) {
			w.logger.LogAttrs(ctx, slog.LevelError, "notifications disabled; polling only", slog.Any(logKeyError, err))
			return
		}
		w.listenFailures.observe(ctx, err) // workers poll meanwhile
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, w.timing.reconnectMax)
	}
}

// heartbeatsMissed is how many of the maintainer's heartbeats may go missing
// before a worker stops counting on notifications. More than one, so that a
// slow wake or a change of maintainer does not count.
const heartbeatsMissed = 3

// listenOnce listens on one connection until it breaks. It reports whether
// it got as far as listening.
//
// The maintainer's heartbeats show that notifications reach the worker, and
// that the maintainer wakes the queues whose notifications were lost, so idle
// queues poll only every idlePoll while they arrive. Without them, they poll
// every PollInterval: no maintainer may run, an older one may, or
// notifications do not arrive. The connection is replaced if it does not
// answer then, since it may have died without a word.
func (w *Worker) listenOnce(ctx context.Context) (listened bool, err error) {
	// A network that drops packets would stall the connection for good.
	startCtx, cancel := context.WithTimeout(ctx, w.timing.listen)
	defer cancel()
	conn, err := w.connectListener(startCtx)
	if err != nil {
		return false, err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()
	if err := w.checkSession(startCtx, conn); err != nil {
		return false, err
	}
	listen := "LISTEN " + w.client.schema
	if _, err := conn.Exec(startCtx, listen); err != nil {
		return false, err
	}
	w.listenFailures.observe(ctx, nil)
	// Notifications may have been missed while disconnected.
	for _, q := range w.queues {
		q.nudge()
	}
	defer w.stopNotifying()
	timeout := heartbeatsMissed * w.timing.heartbeat
	deadline := time.Now().Add(timeout) // for the next heartbeat
	for {
		waitCtx, cancel := context.WithDeadline(ctx, deadline)
		n, err := conn.WaitForNotification(waitCtx)
		cancel()
		switch {
		case err == nil:
			if w.handleNotification(ctx, n.Payload) {
				deadline = time.Now().Add(timeout)
				if !w.notifying.Swap(true) {
					w.silent.Store(false)
					w.logger.LogAttrs(ctx, slog.LevelInfo, "maintainer heartbeats arrive; idle queues poll once a minute")
				}
			}
		case ctx.Err() != nil:
			return true, nil
		case !errors.Is(err, context.DeadlineExceeded):
			return true, err
		default:
			if !w.silent.Swap(true) {
				w.logger.LogAttrs(ctx, slog.LevelWarn, "maintainer heartbeats do not arrive; idle queues poll every PollInterval")
			}
			w.stopNotifying()
			// Listening again changes nothing, but the connection must answer.
			checkCtx, cancel := context.WithTimeout(ctx, w.timing.listen)
			_, err := conn.Exec(checkCtx, listen)
			cancel()
			if err != nil {
				return true, fmt.Errorf("the connection does not answer: %w", err)
			}
			deadline = time.Now().Add(timeout)
		}
	}
}

// stopNotifying notes that notifications may no longer reach the worker, so
// its idle queues poll every PollInterval again, starting now.
func (w *Worker) stopNotifying() {
	if !w.notifying.Swap(false) {
		return
	}
	for _, q := range w.queues {
		q.nudge()
	}
}

// errTransactionPooling is why a worker does not listen through a proxy that
// pools transactions, such as PgBouncer in transaction mode: LISTEN would
// stay on a server connection that goes back to the pool, whose
// notifications reach no worker.
var errTransactionPooling = errors.New("taskmill: the notification connection pools transactions, " +
	"so it cannot listen; set WorkerConfig.ListenConfig to a direct connection")

// sessionChecks is how many times checkSession compares server processes.
// Each comparison can miss transaction pooling while other clients take the
// processes in between; together they hardly can.
const sessionChecks = 3

// checkSession returns errTransactionPooling if conn's statements can run on
// different server processes. A direct connection, or one through a proxy
// that keeps sessions, always has the same process, which no other client
// has. Through a proxy that pools transactions, another client, here the
// pool, often gets the process that conn just used, or conn gets another.
func (w *Worker) checkSession(ctx context.Context, conn *pgx.Conn) error {
	pid := func(q PgxQuerier) (n int32, err error) {
		err = q.QueryRow(ctx, "SELECT pg_backend_pid()", pgx.QueryExecModeSimpleProtocol).Scan(&n)
		return n, err
	}
	own, err := pid(conn)
	if err != nil {
		return err
	}
	for range sessionChecks {
		other, err := pid(w.client.pool)
		if err != nil {
			return err
		}
		again, err := pid(conn)
		if err != nil {
			return err
		}
		if other == own || again != own {
			return errTransactionPooling
		}
	}
	return nil
}

// connectListener opens the notification connection. pgx panics on a
// configuration that was not created by pgx.ParseConfig; that is reported as
// errListenConfig instead.
func (w *Worker) connectListener(ctx context.Context) (conn *pgx.Conn, err error) {
	defer func() {
		if r := recover(); r != nil {
			conn, err = nil, fmt.Errorf("%w: %v", errListenConfig, r)
		}
	}()
	return pgx.ConnectConfig(ctx, w.listen)
}

// handleNotification acts on a notification. It reports whether it was the
// maintainer's heartbeat.
func (w *Worker) handleNotification(ctx context.Context, payload string) (heartbeat bool) {
	kind, arg, ok := strings.Cut(payload, "|")
	if !ok {
		return false
	}
	switch kind {
	case notifyAvailable:
		if q := w.queues[arg]; q != nil {
			q.nudge()
		}
	case notifyCancel:
		id, err := strconv.ParseInt(arg, 10, 64)
		if err != nil {
			return false
		}
		w.mu.Lock()
		t := w.attempts[id]
		w.mu.Unlock()
		if t != nil {
			t.requestCancel()
		}
	case notifyQueue:
		if w.queues[arg] != nil {
			w.refreshQueueSettings(ctx)
		}
	case notifyHeartbeat:
		return true
	case notifyResigned:
		select {
		case w.resigned <- struct{}{}:
		default:
		}
	}
	return false
}
