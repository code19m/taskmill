package taskmill_test

import (
	"context"
	"errors"
	"testing"

	"github.com/code19m/taskmill"
)

// These tests cover Worker.Stats.

func TestStatsCountAttempts(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(_ context.Context, task *taskmill.Task[sendEmail]) error {
			switch {
			case task.Args.To == "dead":
				return taskmill.Permanent(errors.New("no such address"))
			case task.Args.To == "flaky" && task.Attempt == 1:
				return errors.New("try again")
			}
			return nil
		}, &taskmill.HandlerOptions{Retry: fastRetry}),
		Queues: defaultQueue(2),
	})
	if got := w.Stats(); got != (taskmill.WorkerStats{Slots: 2}) {
		t.Fatalf("a new worker: got %+v, want only its 2 slots", got)
	}
	startWorker(t, w)

	for _, to := range []string{"ok", "flaky", "dead"} {
		e.enqueue(t, sendEmail{To: to}, nil)
	}
	waitFor(t, "the attempts to be recorded", func() bool {
		s := w.Stats()
		return s.AttemptsSucceeded == 2 && s.AttemptsFailed == 2 && s.SlotsBusy == 0
	})

	// The pool's waits depend on the load of the machine, not on the attempts.
	got := w.Stats()
	got.PoolWaits = 0
	if want := (taskmill.WorkerStats{AttemptsStarted: 4, AttemptsSucceeded: 2, AttemptsFailed: 2, Slots: 2}); got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}
