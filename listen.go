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
	conn, err := w.connectListener(ctx)
	if err != nil {
		return false, err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()
	if _, err := conn.Exec(ctx, "LISTEN "+w.client.schema); err != nil {
		return false, err
	}
	w.listenFailures.observe(ctx, nil)
	// Notifications may have been missed while disconnected.
	for _, q := range w.queues {
		q.nudge()
	}
	// Whether notifications reach the worker shows only once one arrives,
	// and stops being known when the connection ends.
	probeCtx, stopProbes := context.WithCancel(ctx)
	var probes sync.WaitGroup
	probes.Go(func() { w.probeLoop(probeCtx) })
	defer func() {
		stopProbes()
		probes.Wait()
		w.stopNotifying()
	}()
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return true, err
		}
		w.handleNotification(ctx, n.Payload)
	}
}

// probeLoop sends this worker a notification at once, and then every
// idlePoll, and finds that notifications do not arrive if one has not
// arrived by the time of the next. They do not through a transaction-pooling
// proxy, from a listener on another database, or on a connection that broke
// without a word.
func (w *Worker) probeLoop(ctx context.Context) {
	tick := time.NewTicker(w.timing.idlePoll)
	defer tick.Stop()
	for first := true; ; first = false {
		w.probed.Store(false)
		sendCtx, cancel := context.WithTimeout(ctx, cleanupTimeout)
		_, _ = w.client.pool.Exec(sendCtx, notifySQL, w.client.schema, notifyProbe+"|"+w.id) // a probe lost to an error counts as missing
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if !w.probed.Load() && (w.stopNotifying() || first) {
			w.logger.LogAttrs(ctx, slog.LevelWarn, "notifications do not arrive; idle queues poll every PollInterval")
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

func (w *Worker) handleNotification(ctx context.Context, payload string) {
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
	case notifyProbe:
		if arg != w.id {
			return
		}
		w.probed.Store(true)
		if !w.notifying.Swap(true) {
			w.logger.LogAttrs(ctx, slog.LevelInfo, "notifications arrive; idle queues poll once a minute")
		}
	}
}
