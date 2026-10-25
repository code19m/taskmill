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

// observeTimeout bounds the reading of the queues at each collection, so that
// a slow database does not hold up the application's other metrics.
const observeTimeout = 5 * time.Second

// observedStates are the states that taskmill.queue.tasks reports, with zeros
// for the states a queue has no task in.
var observedStates = []taskmill.State{
	taskmill.StateScheduled, taskmill.StatePending, taskmill.StateAvailable, taskmill.StateRunning,
	taskmill.StateRetryable, taskmill.StateDead, taskmill.StateCanceled,
}

// ObserveQueues reports gauges of client's queues, read with
// [taskmill.Client.Queues] at each collection:
//
//   - taskmill.queue.tasks, the tasks of each queue per state
//     (messaging.taskmill.state), as of the maintainer's latest count, which
//     it refreshes every 10 seconds; completed tasks are not counted.
//   - taskmill.queue.lag, in seconds, how long the oldest available task of
//     each queue has waited since its run time, measured at the collection.
//
// The gauges describe the whole installation, so every process that observes
// it reports the same values: aggregate them with max, not sum, or observe
// in one deployment only. Each collection runs two queries, bounded to 5
// seconds; one that fails is reported to otel.Handle. Unregister the returned
// registration to stop observing.
func (in *Instrumentation) ObserveQueues(client *taskmill.Client) (metric.Registration, error) {
	if client == nil {
		return nil, errors.New("taskmill/otel: client is nil")
	}
	tasks, err := in.meter.Int64ObservableGauge("taskmill.queue.tasks",
		metric.WithDescription("Tasks per queue and state, as of the maintainer's latest count."),
		metric.WithUnit("{task}"))
	if err != nil {
		return nil, fmt.Errorf("taskmill/otel: %w", err)
	}
	lag, err := in.meter.Float64ObservableGauge("taskmill.queue.lag",
		metric.WithDescription("How long the oldest available task of the queue has waited since its run time."),
		metric.WithUnit("s"))
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
		for _, q := range queues {
			queue := semconv.MessagingDestinationName(q.Name)
			for _, s := range observedStates {
				o.ObserveInt64(tasks, q.Counts[s], metric.WithAttributes(queue, stateKey.String(string(s))))
			}
			o.ObserveFloat64(lag, q.Lag.Seconds(), metric.WithAttributes(queue))
		}
		return nil
	}, tasks, lag)
}
