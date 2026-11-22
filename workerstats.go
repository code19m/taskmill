package taskmill

import "sync/atomic"

// This file holds Worker.Stats: what a worker did, and how busy its slots
// are.

// WorkerStats are counts of what a [Worker] did since [NewWorker] created it,
// and how busy its slots are now: [Worker.Stats] returns them. The counts
// only grow, so a metrics system reads them as counters, and their rates
// say how often each thing happens. Each task of a batch counts as one
// attempt.
//
// The [monitoring page] says which of them to alert on.
//
// [monitoring page]: https://github.com/code19m/taskmill/blob/main/docs/production/monitoring.md
type WorkerStats struct {
	// AttemptsStarted counts the attempts whose handler the worker started,
	// in Run and in RunTask. A task that the worker ends without a handler,
	// as one of a kind that it has no handler for, does not count.
	AttemptsStarted int64

	// AttemptsSucceeded counts the attempts whose success was recorded: their
	// tasks completed, also those that the handler completed with
	// [Task.CompleteTx].
	AttemptsSucceeded int64

	// AttemptsFailed counts the attempts whose failure was recorded: their
	// tasks wait to be retried, or are dead. An attempt whose task the
	// maintainer rescued after its worker lost the lease counts in
	// LeasesLost, on that worker, if it still runs.
	AttemptsFailed int64

	// LeasesLost counts the attempts that lost their leases: the worker
	// could not renew a lease in time, or fetched a task again whose older
	// attempt still ran. Their results are discarded, and their tasks may
	// run twice.
	LeasesLost int64

	// HandlersAbandoned counts the attempts whose handler did not return 5
	// seconds after its context ended, by its Timeout, a cancel, a lost
	// lease, or Run's shutdown, so that the worker abandoned it: see
	// [HandlerOptions.Timeout]. The handler's goroutine keeps its slot until
	// it returns.
	HandlersAbandoned int64

	// CompletionsBlocked counts the successful attempts whose completion
	// waited longer than LeaseDuration for a row that another transaction
	// held: the task's, or its FIFO group's.
	CompletionsBlocked int64

	// PoolWaits counts the times that a statement of the worker found every
	// connection of the pool in use, and waited for one. Those statements
	// are fetches, completions in Run, the records of other outcomes, such
	// as failures and snoozes, renewals of leases, check-ins, and the takes
	// of the worker's reserve. A connection that is still opening is not in
	// use. Handlers that share the pool hold the connections, or the pool
	// is too small, or statements wait for an answer, as behind a pooler
	// whose own pool is full. A renewal or a check-in waits a thirtieth of
	// the lease at most, and then uses the reserve, if the worker keeps
	// one: see [Worker]. Without one, it counts twice.
	PoolWaits int64

	// Slots is the sum of the Concurrency of the worker's queues.
	Slots int

	// SlotsBusy is how many slots running attempts and batches hold now.
	SlotsBusy int

	// SlotsStuck is how many of those are held by handlers that did not
	// return 5 seconds after their context ended. While they hold every
	// slot, [Worker.Health] fails.
	SlotsStuck int
}

// counters hold what WorkerStats counts. The worker adds to them as it
// works, without a lock.
type counters struct {
	started, succeeded, failed, leasesLost, abandoned, blocked, poolWaits atomic.Int64
}

// Stats returns what the worker did since [NewWorker] created it, and how
// busy its slots are now. It reads only memory, so a metrics system may call
// it at every collection. The otel module exports it with
// Instrumentation.ObserveWorker.
func (w *Worker) Stats() WorkerStats {
	s := WorkerStats{
		AttemptsStarted:    w.counts.started.Load(),
		AttemptsSucceeded:  w.counts.succeeded.Load(),
		AttemptsFailed:     w.counts.failed.Load(),
		LeasesLost:         w.counts.leasesLost.Load(),
		HandlersAbandoned:  w.counts.abandoned.Load(),
		CompletionsBlocked: w.counts.blocked.Load(),
		PoolWaits:          w.counts.poolWaits.Load(),
	}
	for _, q := range w.queues {
		s.Slots += cap(q.slots)
		s.SlotsBusy += len(q.slots)
		s.SlotsStuck += int(q.stuck.Load())
	}
	s.SlotsStuck = min(s.SlotsStuck, s.SlotsBusy) // a stuck handler that just returned may still count
	return s
}
