package taskmill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
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
		if errors.Is(err, errListenConfig) {
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

// listenOnce listens on one connection until it breaks. It reports whether
// it got as far as listening.
func (w *Worker) listenOnce(ctx context.Context) (listened bool, err error) {
	// A network that drops packets would stall the connection for good.
	startCtx, cancel := context.WithTimeout(ctx, listenTimeout)
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
	if _, err := conn.Exec(startCtx, "LISTEN "+w.client.schema); err != nil {
		return false, err
	}
	w.listenFailures.observe(ctx, nil)
	// Notifications may have been missed while disconnected.
	for _, q := range w.queues {
		q.nudge()
	}
	// The connection is replaced when the maintainer's heartbeats stop on
	// it, since it may have died without a word.
	connCtx, replace := context.WithCancel(ctx)
	beats := make(chan struct{}, 1)
	var watch sync.WaitGroup
	watch.Go(func() { w.watchHeartbeats(connCtx, beats, replace) })
	defer func() {
		replace()
		watch.Wait()
		w.stopNotifying()
	}()
	for {
		n, err := conn.WaitForNotification(connCtx)
		switch {
		case err == nil:
			w.handleNotification(ctx, n.Payload, beats)
		case ctx.Err() == nil && connCtx.Err() != nil:
			return true, nil // replaced: watchHeartbeats said why
		default:
			return true, err
		}
	}
}

// heartbeatsMissed is how many of the maintainer's heartbeats, one per
// maintenance interval, may go missing before a worker stops counting on
// notifications. More than one, so that a slow wake does not count.
const heartbeatsMissed = 3

// watchHeartbeats watches the maintainer's heartbeats on one connection.
// While they arrive, notifications reach the worker, and the maintainer
// wakes the queues whose notifications were lost, so idle queues poll only
// every idlePoll. Without them, they poll every PollInterval: no maintainer
// may run, an older one may, or notifications do not arrive. If heartbeats
// stop after they arrived, it calls replace too, since the connection may
// have died without a word.
func (w *Worker) watchHeartbeats(ctx context.Context, beats <-chan struct{}, replace context.CancelFunc) {
	timeout := heartbeatsMissed * w.timing.maintainer
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-beats:
			timer.Reset(timeout)
			if !w.notifying.Swap(true) {
				w.silent.Store(false)
				w.logger.LogAttrs(ctx, slog.LevelInfo, "maintainer heartbeats arrive; idle queues poll once a minute")
			}
		case <-timer.C:
			timer.Reset(timeout)
			if !w.silent.Swap(true) {
				w.logger.LogAttrs(ctx, slog.LevelWarn, "maintainer heartbeats do not arrive; idle queues poll every PollInterval")
			}
			if w.stopNotifying() {
				replace()
				return
			}
		}
	}
}

// stopNotifying notes that notifications may no longer reach the worker, so
// its idle queues poll every PollInterval again, starting now. It reports
// whether they reached it until now.
func (w *Worker) stopNotifying() bool {
	if !w.notifying.Swap(false) {
		return false
	}
	for _, q := range w.queues {
		q.nudge()
	}
	return true
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

// handleNotification acts on a notification, and passes the maintainer's
// heartbeats to beats.
func (w *Worker) handleNotification(ctx context.Context, payload string, beats chan<- struct{}) {
	kind, arg, ok := strings.Cut(payload, "|")
	if !ok {
		return
	}
	switch kind {
	case notifyAvailable:
		if q := w.queues[arg]; q != nil {
			q.nudge()
		}
	case notifyCancel:
		id, err := strconv.ParseInt(arg, 10, 64)
		if err != nil {
			return
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
		select {
		case beats <- struct{}{}:
		default: // the watcher has not taken the last one yet
		}
	}
}
