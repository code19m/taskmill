package otel

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/code19m/taskmill"
)

// observeTimeout bounds the reading of the statistics at each collection, so
// that a slow database does not hold up the application's other metrics.
const observeTimeout = 5 * time.Second

// observedStates are the states that taskmill.queue.tasks reports, with zeros
// for the states a queue has no task in.
var observedStates = []taskmill.State{
	taskmill.StateScheduled, taskmill.StatePending, taskmill.StateAvailable, taskmill.StateRunning,
	taskmill.StateRetryable, taskmill.StateDead, taskmill.StateCanceled,
}

// ObserveQueues reports gauges of client's queues, read with
// [taskmill.Client.Queues] at each collection, from the statistics that the
// maintainer refreshes every 10 seconds:
//
//   - taskmill.queue.tasks: the tasks of each queue per state
//     (taskmill.task.state), except completed ones. Counting stops at 10,000
//     tasks per queue and state, and at 100,000 scheduled and retryable
//     tasks across all queues, so on a large backlog a count is a lower
//     bound.
//   - taskmill.queue.capped: 1 when a count of the queue stopped at its cap,
//     else 0.
//   - taskmill.queue.lag: in seconds, how long the queue's oldest due task
//     has waited, as [taskmill.QueueInfo.Lag] says: it may read up to one
//     refresh high. It stays right on a large backlog, and keeps growing
//     when no maintainer runs: alert on it, not on the counts.
//   - taskmill.stats.age: in seconds, by this process's clock, how long ago
//     the maintainer last counted the queues. Alert when it passes a minute:
//     no maintainer runs, or it cannot count, and a queue that was empty at
//     its last count cannot show lag. It is absent until the maintainer
//     first counts.
//
// The gauges describe the whole installation, read from one small table, so
// every process that observes it reports the same values: aggregate them with
// max, not sum. A read takes at most 5 seconds; one that fails is reported to
// otel.Handle. Unregister the returned registration to stop observing.
func (in *Instrumentation) ObserveQueues(client *taskmill.Client) (metric.Registration, error) {
	if client == nil {
		return nil, errors.New("taskmill/otel: client is nil")
	}
	tasks, err := in.meter.Int64ObservableGauge("taskmill.queue.tasks", metric.WithUnit("{task}"),
		metric.WithDescription("Tasks per queue and state, counted up to 10,000 per queue and state "+
			"and 100,000 scheduled and retryable tasks in all."))
	if err != nil {
		return nil, fmt.Errorf("taskmill/otel: %w", err)
	}
	capped, err := in.meter.Int64ObservableGauge("taskmill.queue.capped",
		metric.WithDescription("1 when a count of the queue stopped at its cap and is a lower bound."))
	if err != nil {
		return nil, fmt.Errorf("taskmill/otel: %w", err)
	}
	lag, err := in.meter.Float64ObservableGauge("taskmill.queue.lag", metric.WithUnit("s"),
		metric.WithDescription("How long the oldest due task of the queue has waited since its run time."))
	if err != nil {
		return nil, fmt.Errorf("taskmill/otel: %w", err)
	}
	age, err := in.meter.Float64ObservableGauge("taskmill.stats.age", metric.WithUnit("s"),
		metric.WithDescription("How long ago the maintainer last counted the queues; it grows when no maintainer runs."))
	if err != nil {
		return nil, fmt.Errorf("taskmill/otel: %w", err)
	}
	return in.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		ctx, cancel := context.WithTimeout(ctx, observeTimeout)
		defer cancel()
		queues, err := client.Queues(ctx)
		if err != nil {
			return fmt.Errorf("taskmill/otel: observe queues: %w", err)
		}
		var counted time.Time
		for _, q := range queues {
			if q.RefreshedAt.After(counted) {
				counted = q.RefreshedAt
			}
			queue := metric.WithAttributes(semconv.MessagingDestinationName(q.Name))
			for _, s := range observedStates {
				o.ObserveInt64(tasks, q.Counts[s],
					metric.WithAttributes(semconv.MessagingDestinationName(q.Name), stateKey.String(string(s))))
			}
			o.ObserveInt64(capped, boolInt(q.Capped), queue)
			o.ObserveFloat64(lag, q.Lag.Seconds(), queue)
		}
		if !counted.IsZero() {
			o.ObserveFloat64(age, time.Since(counted).Seconds())
		}
		return nil
	}, tasks, capped, lag, age)
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
