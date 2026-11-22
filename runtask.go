package taskmill

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// This file holds Worker.RunTask: one task run now, in the calling
// goroutine, the way Run runs tasks.

// RunTask runs one task now, in the calling goroutine, the way [Worker.Run]
// runs tasks: it leases the task, runs the middleware and the handler of its
// kind while renewing the lease, and records the outcome. It ignores the
// task's run time, queue pauses, and MaxRunning limits, but not FIFO order.
// It does not need Run to be running, which makes it the tool for tests. If
// ctx is canceled before the handler returns, the handler's context is
// canceled too, and the task goes back to its queue without using an attempt.
// So does a task whose handler ends the goroutine with [runtime.Goexit], as
// t.Fatal in a test does.
//
// A task of a kind that the worker has no handler for is snoozed, with a
// no_handler note in its errors, as Run does, and returned.
//
// A task whose lease ended on its way, as when its row took longer than the
// lease to arrive, may run on another worker already: RunTask runs nothing
// then, and returns the task, which stays running until the maintainer
// rescues it.
//
// It returns the task afterwards. It returns [ErrNotFound] if no task has the
// ID, and [ErrInvalidState] if the task is running, waits behind its FIFO
// group, or has finished.
func (w *Worker) RunTask(ctx context.Context, id int64) (*TaskInfo, error) {
	t, err := w.leaseTask(ctx, id)
	if err != nil {
		return nil, err
	}
	t.direct = true
	w.track(ctx, t)
	w.runDirect(ctx, t)

	getCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recordTimeout)
	defer cancel()
	info, err := w.client.Get(getCtx, id)
	if errors.Is(err, ErrNotFound) && t.completedDirectly {
		// The kind discards completed tasks: the task was deleted as it
		// completed.
		done := *t.info
		done.State, done.FinishedAt = StateCompleted, time.Now()
		return &done, nil
	}
	return info, err
}

// runDirect runs the attempt t of RunTask in the calling goroutine, and
// renews its lease every third of it while the handler runs, and while its
// result is recorded. It tries a renewal that failed again soon, as Run
// does, also when Run does not run (see renewAgain). The cleanup is
// deferred: a handler that ends the goroutine with runtime.Goexit, as
// t.Fatal does in a test, stops the renewals all the same, and its task
// goes back to its queue (see runOne).
func (w *Worker) runDirect(ctx context.Context, t *liveAttempt) {
	renewCtx, stopRenewing := context.WithCancel(context.WithoutCancel(ctx))
	var renewing sync.WaitGroup
	defer renewing.Wait()
	defer stopRenewing()
	ts, failed := []*liveAttempt{t}, make(chan struct{}, 1)
	renewing.Go(func() {
		w.everyThirdOfLease(renewCtx, func(ctx context.Context) {
			if w.tryRenewLeases(ctx, ts) != nil {
				signal(failed)
			}
		})
	})
	renewing.Go(func() { w.renewAgain(renewCtx, failed, func() []*liveAttempt { return ts }) })

	attemptCtx, stopAttempt := context.WithCancelCause(context.WithoutCancel(ctx))
	defer stopAttempt(nil)
	defer context.AfterFunc(ctx, func() { stopAttempt(ErrShutdown) })()
	record := context.WithoutCancel(ctx)
	if t.reg != nil && t.reg.runBatch != nil {
		w.runBatch(attemptCtx, record, []*liveAttempt{t})
	} else {
		w.runOne(attemptCtx, record, t)
	}
}

// leaseTask leases one task for RunTask, or explains why it cannot.
func (w *Worker) leaseTask(ctx context.Context, id int64) (*liveAttempt, error) {
	fail := func(err error) error { return fmt.Errorf("taskmill: run task %d: %w", id, err) }
	sent := time.Now()
	r, err := w.client.pool.Query(ctx, w.client.sql.leaseTask, id, w.cfg.LeaseDuration.Microseconds(), w.id)
	if err != nil {
		return nil, fail(err)
	}
	defer r.Close()

	if r.Next() {
		t, err := w.scanLeased(r, sent)
		// The task is leased only if the statement ends without an error,
		// which the rows report once they are closed.
		r.Close()
		if err = cmp.Or(err, r.Err()); err != nil {
			return nil, fail(err)
		}
		return t, nil
	}
	if err := r.Err(); err != nil {
		return nil, fail(err)
	}
	return nil, w.client.stateError(ctx, pgxExecutor{q: w.client.pool}, id, "run")
}
