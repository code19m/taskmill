package taskmill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// settingsLoop reloads the cluster-wide settings of the worker's queues.
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
	if err != nil {
		w.logger.LogAttrs(ctx, slog.LevelWarn, "load queue settings failed", slog.Any(logKeyError, err))
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
// with backoff when it breaks.
func (w *Worker) listenLoop(ctx context.Context) {
	backoff := w.timing.reconnectMin
	for ctx.Err() == nil {
		err := w.listenOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, errListenConfig) {
			w.logger.LogAttrs(ctx, slog.LevelError, "notifications disabled; polling only", slog.Any(logKeyError, err))
			return
		}
		w.logger.LogAttrs(ctx, slog.LevelWarn, "notification connection lost; polling until it is back",
			slog.Any(logKeyError, err), slog.Duration(logKeyRetryIn, backoff))
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, w.timing.reconnectMax)
	}
}

func (w *Worker) listenOnce(ctx context.Context) error {
	conn, err := w.connectListener(ctx)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()
	if _, err := conn.Exec(ctx, "LISTEN "+w.client.schema); err != nil {
		return err
	}
	// Notifications may have been missed while disconnected.
	for _, q := range w.queues {
		q.nudge()
	}
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		w.handleNotification(ctx, n.Payload)
	}
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
	case "a":
		if q := w.queues[arg]; q != nil {
			q.nudge()
		}
	case "c":
		var id int64
		if _, err := fmt.Sscan(arg, &id); err != nil {
			return
		}
		w.mu.Lock()
		t := w.attempts[id]
		w.mu.Unlock()
		if t != nil {
			t.requestCancel()
		}
	case "q":
		if w.queues[arg] != nil {
			w.refreshQueueSettings(ctx)
		}
	}
}
