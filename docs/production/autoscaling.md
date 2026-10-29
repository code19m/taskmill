# Autoscaling

This page shows how to add and remove workers with [KEDA](https://keda.sh),
based on how much work waits in a queue.

## Which should I use?

| Situation | What to use | Why |
|---|---|---|
| Most apps, with the `otel` module and Prometheus | **Scale on the queue's available plus running tasks** | It measures the work directly. Counting running tasks keeps busy workers alive |
| A backlog of more than 10,000 tasks is normal, or you promise a wait time | Scale on the queue's [lag](../concepts/glossary.md#lag) | Lag has no cap. Counts stop at 10,000 |
| No OpenTelemetry | Scale on `Client.Queues`, behind a small JSON endpoint that KEDA's Metrics API scaler reads | The same numbers, without a metrics system |

Never point KEDA's PostgreSQL scaler at taskmill's tables. They are internal
and change between releases. `Client.Queues` and the gauges are the stable
way to read them ([ADR 0006](../adr/0006-health-and-scaling-read-paths.md)).

## Scale on tasks with Prometheus

This `ScaledObject` runs one worker for every 10 available or running tasks
of the `default` queue. 10 is a worker's `Concurrency`.

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: worker
spec:
  scaleTargetRef: {name: worker}
  minReplicaCount: 1 # keep one worker: see "Scale to zero"
  maxReplicaCount: 10
  advanced:
    horizontalPodAutoscalerConfig:
      behavior:
        scaleDown:
          stabilizationWindowSeconds: 300 # a short dip does not stop workers
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://prometheus.monitoring.svc:9090
        query: sum(max by (taskmill_task_state) (taskmill_queue_tasks{messaging_destination_name="default", taskmill_task_state=~"available|running"}))
        threshold: "10"
```

- The gauges come from `ObserveQueues` in the `otel` module
  ([Observability](../guides/observability.md)). Every process reports the
  same numbers for the whole schema, so take their `max`, never their `sum`.
- Scaling down stops workers. Each one drains for `DrainTimeout` first, and a
  task still running after that starts over on another worker. Keep
  `DrainTimeout` above your tasks' usual length.
- While a queue is paused, its tasks still count, so KEDA adds workers that
  have nothing to do. Expect that during an incident.
- `maxReplicaCount` sets your peak of database connections: see
  [Count the database connections](kubernetes.md#count-the-database-connections).

## Scale on lag

Lag is how long the oldest due task of a queue has waited. To scale on it,
use this trigger instead, with a threshold in seconds:

```yaml
  triggers:
    - type: prometheus
      metricType: Value
      metadata:
        serverAddress: http://prometheus.monitoring.svc:9090
        query: max(taskmill_queue_lag_seconds{messaging_destination_name="default"})
        threshold: "30"
```

Lag can read up to 10 seconds high, so don't set the threshold lower than
that.

## Scale to zero

The [maintainer](../concepts/glossary.md#maintainer) runs inside your workers. If KEDA
stops every worker, nothing makes retries and delayed tasks available,
schedules don't run, and the counts stop updating. So nothing would scale
the workers up again.

To scale to zero, run the maintainer in a Deployment of its own:

```go
// Deployment "maintainer", always 1 replica: no handlers, every schedule.
keeper, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Schedules: schedules})

// Deployment "worker", scaled by KEDA down to zero.
worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
    Handlers:     &handlers,
    NoMaintainer: true,
})
```

1. Give the maintainer every `Schedule`: the maintainer runs only the
   schedules that it defines.
2. Report the gauges from a process that stays up, such as that maintainer
   or your web server. A stopped worker reports nothing.
3. Set `minReplicaCount: 0` in the `ScaledObject`.

## Scale without OpenTelemetry

KEDA's Metrics API scaler reads a number from JSON. Serve it from
`Client.Queues`, in a process that is always up, such as your web server:

```go
mux.HandleFunc("GET /queues", func(w http.ResponseWriter, r *http.Request) {
    queues, err := client.Queues(r.Context())
    if err != nil {
        http.Error(w, "taskmill: list queues failed", http.StatusServiceUnavailable)
        return
    }
    tasks := map[string]int64{} // per queue: available plus running
    for _, q := range queues {
        tasks[q.Name] = q.Counts[taskmill.StateAvailable].N + q.Counts[taskmill.StateRunning].N
    }
    _ = json.NewEncoder(w).Encode(tasks)
})
```

Then give the `ScaledObject` this trigger:

```yaml
  triggers:
    - type: metrics-api
      metadata:
        url: http://web.shop.svc/queues # your web server's Service
        valueLocation: default
        targetValue: "10"
```

## Size each worker

- **Prefer fewer, larger workers on a queue.** A wake-up for a queue reaches
  every worker that reads it. They all look for tasks, and all but one find
  nothing. Many small workers on one queue waste the database's CPU.
- `Concurrency` sets how many tasks a worker runs at the same time. The pool
  follows it: `Concurrency` plus 5.
- A worker itself needs little memory: about 20 MiB, plus a few KiB per
  running task. Your handlers need the rest.

## See also

- [Kubernetes](kubernetes.md): the worker Deployment that KEDA scales
- [Monitoring](monitoring.md): the same gauges, for alerts
- [Performance](../reference/performance.md): how fast, and how light
- [`Client.Queues`](https://pkg.go.dev/github.com/code19m/taskmill#Client.Queues) on pkg.go.dev

**Next:** [Monitoring](monitoring.md)
