package taskmill_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/code19m/taskmill"
)

func TestSettings(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()

	want := func(history, dead time.Duration) {
		t.Helper()
		s, err := e.client.Settings(ctx)
		if err != nil || s.HistoryRetention != history || s.DeadRetention != dead {
			t.Fatalf("settings: %+v, %v; want history %s, dead %s", s, err, history, dead)
		}
	}

	want(7*24*time.Hour, 30*24*time.Hour)

	if err := e.client.UpdateSettings(ctx, func(s *taskmill.Settings) { s.HistoryRetention = 48 * time.Hour }); err != nil {
		t.Fatal(err)
	}
	want(48*time.Hour, 30*24*time.Hour)

	for _, update := range []func(*taskmill.Settings){nil, func(s *taskmill.Settings) { s.DeadRetention = time.Minute }} {
		if err := e.client.UpdateSettings(ctx, update); !errors.Is(err, taskmill.ErrInvalidArgument) {
			t.Fatalf("got %v, want ErrInvalidArgument", err)
		}
	}
	want(48*time.Hour, 30*24*time.Hour)

	// Concurrent updates of different settings both land.
	interleave(t,
		func(wait func()) error {
			return e.client.UpdateSettings(ctx, func(s *taskmill.Settings) { wait(); s.DeadRetention = 10 * 24 * time.Hour })
		},
		func(ctx context.Context) error {
			return e.client.UpdateSettings(ctx, func(s *taskmill.Settings) { s.HistoryRetention = 72 * time.Hour })
		})
	want(72*time.Hour, 10*24*time.Hour)

	// A setting changed to zero goes back to its default.
	if err := e.client.UpdateSettings(ctx, func(s *taskmill.Settings) { s.HistoryRetention = 0 }); err != nil {
		t.Fatal(err)
	}
	want(7*24*time.Hour, 10*24*time.Hour)
}

func TestEndlessConflictsGiveUp(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()

	// Each run of the outer update changes the queue behind its back.
	runs := 0
	err := e.client.UpdateQueue(ctx, "contended", func(s *taskmill.QueueSettings) {
		runs++
		if err := e.client.UpdateQueue(ctx, "contended", func(s *taskmill.QueueSettings) { s.MaxRunning++ }); err != nil {
			t.Error(err)
		}
		s.MaxRunning = 100
	})
	if !errors.Is(err, taskmill.ErrConflict) || runs != 10 {
		t.Fatalf("got %v after %d runs, want ErrConflict after 10", err, runs)
	}

	err = e.client.UpdateSettings(ctx, func(*taskmill.Settings) {
		if err := e.client.UpdateSettings(ctx, func(s *taskmill.Settings) { s.HistoryRetention += time.Hour }); err != nil {
			t.Error(err)
		}
	})
	if !errors.Is(err, taskmill.ErrConflict) {
		t.Fatalf("got %v, want ErrConflict", err)
	}
}
