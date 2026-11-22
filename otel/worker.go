package otel

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/code19m/taskmill"
)

const slotStateKey = attribute.Key("taskmill.slot.state")

// workerCounters are the counters that ObserveWorker reports, each with the
// count of taskmill.WorkerStats that it reads.
var workerCounters = []struct {
	name, unit, description string
	value                   func(taskmill.WorkerStats) int64
}{
	{
		"taskmill.worker.attempts.started", "{attempt}", "Attempts whose handler the worker started.",
		func(s taskmill.WorkerStats) int64 { return s.AttemptsStarted },
	},
	{
		"taskmill.worker.attempts.succeeded", "{attempt}", "Attempts whose success the worker recorded.",
		func(s taskmill.WorkerStats) int64 { return s.AttemptsSucceeded },
	},
	{
		"taskmill.worker.attempts.failed", "{attempt}", "Attempts whose failure the worker recorded.",
		func(s taskmill.WorkerStats) int64 { return s.AttemptsFailed },
	},
	{
		"taskmill.worker.leases.lost", "{attempt}", "Attempts that lost their lease; their tasks may run twice.",
		func(s taskmill.WorkerStats) int64 { return s.LeasesLost },
	},
	{
		"taskmill.worker.handlers.abandoned", "{attempt}", "Attempts whose handler did not return 5 seconds after its context ended.",
		func(s taskmill.WorkerStats) int64 { return s.HandlersAbandoned },
	},
	{
		"taskmill.worker.completions.blocked", "{completion}", "Completions that waited longer than the lease for a row that another transaction held.",
		func(s taskmill.WorkerStats) int64 { return s.CompletionsBlocked },
	},
	{
		"taskmill.worker.pool.waits", "{wait}", "Times that a statement of the worker found every connection of the pool in use, and waited for one.",
		func(s taskmill.WorkerStats) int64 { return s.PoolWaits },
	},
}

// ObserveWorker reports counters of w, read with [taskmill.Worker.Stats] at
// each collection, from w's memory:
//
//   - taskmill.worker.attempts.started, taskmill.worker.attempts.succeeded,
//     and taskmill.worker.attempts.failed: the attempts whose handler the
//     worker started, and those whose success or failure it recorded.
//   - taskmill.worker.leases.lost: the attempts that lost their lease. Their
//     tasks may run twice.
//   - taskmill.worker.handlers.abandoned: the attempts whose handler did not
//     return 5 seconds after its context ended, which the worker abandoned.
//   - taskmill.worker.completions.blocked: the completions that waited
//     longer than the lease for a row that another transaction held.
//   - taskmill.worker.pool.waits: the times that a statement of the worker
//     found every connection of the pool in use, and waited for one, as
//     [taskmill.WorkerStats].PoolWaits says.
//   - taskmill.worker.slots: the slots of the worker's queues, by
//     taskmill.slot.state: free, running, or stuck, held by a handler that
//     did not return 5 seconds after its context ended.
//
// The docs say which of them to alert on:
// https://github.com/code19m/taskmill/blob/main/docs/production/monitoring.md.
//
// Each process reports its own workers, so aggregate them with sum. The
// values of several workers of one process add up, without an attribute
// that tells them apart. A worker counts from 0, so replacing one with a
// new one, as after Run returned, looks like a reset of the counters, which
// rate and increase allow for. Unregister the returned registration to stop
// observing. It returns an error that wraps [taskmill.ErrInvalidArgument]
// if w is nil.
func (in *Instrumentation) ObserveWorker(w *taskmill.Worker) (metric.Registration, error) {
	if w == nil {
		return nil, fmt.Errorf("%w: otel: worker is nil", taskmill.ErrInvalidArgument)
	}

	counters := make([]metric.Int64ObservableCounter, len(workerCounters))
	observed := make([]metric.Observable, 0, len(workerCounters)+1)
	for i, c := range workerCounters {
		var err error
		counters[i], err = in.meter.Int64ObservableCounter(c.name, metric.WithUnit(c.unit), metric.WithDescription(c.description))
		if err != nil {
			return nil, fmt.Errorf("taskmill/otel: %w", err)
		}
		observed = append(observed, counters[i])
	}
	slots, err := in.meter.Int64ObservableUpDownCounter("taskmill.worker.slots", metric.WithUnit("{slot}"),
		metric.WithDescription("The slots of the worker's queues, by state: free, running, or stuck."))
	if err != nil {
		return nil, fmt.Errorf("taskmill/otel: %w", err)
	}
	observed = append(observed, slots)

	free := metric.WithAttributes(slotStateKey.String("free"))
	running := metric.WithAttributes(slotStateKey.String("running"))
	stuck := metric.WithAttributes(slotStateKey.String("stuck"))
	return in.meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s := w.Stats()
		for i, c := range workerCounters {
			o.ObserveInt64(counters[i], c.value(s))
		}
		o.ObserveInt64(slots, int64(s.Slots-s.SlotsBusy), free)
		o.ObserveInt64(slots, int64(s.SlotsBusy-s.SlotsStuck), running)
		o.ObserveInt64(slots, int64(s.SlotsStuck), stuck)
		return nil
	}, observed...)
}
