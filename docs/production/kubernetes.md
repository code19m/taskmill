# Kubernetes

This page gives you manifests to copy: workers, their probe, the dashboard,
and a migration Job. It also shows the numbers that tie them together: the
grace period and the database connections.

## Which should I use?

| Situation | What to use | Why |
|---|---|---|
| Production | **Workers in a Deployment of their own, from your app's image.** Web pods only enqueue. | Workers scale on the queue, not on web traffic, and handlers never take the web pods' connections |
| A small app with little load | The worker inside the web process | One Deployment is simpler. Give it the grace period below, and size one pool for both |

Run one `Worker` per process, and list all its queues in it.

## Run workers in their own Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: worker
spec:
  replicas: 2 # leave it out when an autoscaler sets it
  selector:
    matchLabels: {app: worker}
  template:
    metadata:
      labels: {app: worker}
    spec:
      terminationGracePeriodSeconds: 30 # DrainTimeout (20 s) plus 10 s
      containers:
        - name: worker
          image: registry.example.com/shop:1.4.0 # your app's image
          args: [worker]
          env:
            - name: DATABASE_URL # ends in ?pool_max_conns=15: Concurrency 10 plus 5
              valueFrom: {secretKeyRef: {name: shop-db, key: url}}
          ports:
            - {name: health, containerPort: 8081}
          readinessProbe:
            httpGet: {path: /healthz, port: health}
            periodSeconds: 10
          resources:
            requests: {cpu: 250m, memory: 128Mi}
            limits: {memory: 256Mi}
```

- `args: [worker]` assumes that your binary picks its role from its first
  argument, such as `shop worker` or `shop migrate`.
- Workers get no traffic, so they need no Service and no `preStop` hook.
- A worker itself uses about 20 MiB of memory, plus a few KiB per running
  task. Size the limits for what your handlers use
  ([performance](../reference/performance.md)).

## Tell Kubernetes when a worker is ready

Serve `Worker.Health` on the port of the readiness probe:

```go
go func() {
    mux := http.NewServeMux()
    mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
        if err := worker.Health(); err != nil {
            http.Error(w, err.Error(), http.StatusServiceUnavailable)
            return
        }
        w.WriteHeader(http.StatusOK)
    })
    server := &http.Server{Addr: ":8081", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
    log.Print(server.ListenAndServe())
}()
```

`Health` fails while `Run` does not run, and when the worker has not reached
the database for longer than `LeaseDuration` (30 s). It reads only memory, so
the probe costs the database nothing. It does not see a stuck handler.

With the probe above, a worker turns unready within about a minute of losing
the database, and ready again within about 20 seconds of its return. An
unready worker pauses a rolling deploy, which is right while the database is
out of reach.

> [!WARNING]
> Don't base a liveness probe on `Health`. It fails while the database is
> down, and a restart doesn't fix that: a new pod cannot even start until
> the database is back. If you want a liveness probe, point it at a path
> that does not call `Health`.

## Give workers time to drain

Set `terminationGracePeriodSeconds` to at least `DrainTimeout` plus 10
seconds, because Kubernetes kills the pod when the grace period ends. The
defaults fit: a `DrainTimeout` of 20 seconds, and Kubernetes' 30 seconds. If
you raise `DrainTimeout` to 2 minutes, set 130.

Why 10 more seconds: after `DrainTimeout`, the worker cancels the tasks still
running, waits up to 5 seconds for them, and then gives them back. See
[Stop workers without losing work](deploys.md#stop-workers-without-losing-work).

## Count the database connections

Add up the connections of all your pods. Each worker pod uses its pool plus
one connection for notifications:

```text
  worker pods      × (pool_max_conns + 1)
+ web pods         × pool_max_conns
+ taskmill-ui pods × pool_max_conns
≤ max_connections, less the connections PostgreSQL keeps for administrators
```

Count the pods at their peak. During a rollout, old pods keep their
connections while they drain, and new pods already open theirs. In the worst
case, a Deployment then has twice its replicas, or twice the autoscaler's
maximum. With short tasks, old pods stop within seconds, and the peak is
closer to the replicas plus `maxSurge`.

| Pods | Normal | Rollout peak |
|---|---|---|
| 4 workers, `Concurrency` 10, so `pool_max_conns=15` | 4 × 16 = 64 | 128 |
| 3 web pods with `pool_max_conns=10` | 30 | 60 |
| 1 `taskmill-ui` with `pool_max_conns=4` | 4 | 8 |
| **Total** | **98** | **196** |

So this app needs `max_connections` of about 200, or a pooler in front: see
[Connection poolers](connection-poolers.md).

## Run the dashboard

The preferred way is to mount the dashboard in your app with `ui.New`,
behind your own login ([Dashboard](../guides/dashboard.md)). If you have no
Go HTTP server, run the `taskmill-ui` image in a Deployment of its own:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: taskmill-ui
spec:
  replicas: 1
  selector:
    matchLabels: {app: taskmill-ui}
  template:
    metadata:
      labels: {app: taskmill-ui}
    spec:
      containers:
        - name: taskmill-ui
          image: ghcr.io/code19m/taskmill-ui:0.3.0 # the taskmill version your apps run
          env:
            - {name: TASKMILL_UI_ADDR, value: ":8080"}
            - name: TASKMILL_DATABASE_URL # ends in ?pool_max_conns=4
              valueFrom: {secretKeyRef: {name: shop-db, key: ui-url}}
            - name: TASKMILL_UI_PASSWORD
              valueFrom: {secretKeyRef: {name: taskmill-ui, key: password}}
          ports:
            - {name: http, containerPort: 8080}
          readinessProbe:
            tcpSocket: {port: http}
          securityContext:
            runAsNonRoot: true
            readOnlyRootFilesystem: true
            allowPrivilegeEscalation: false
            capabilities: {drop: [ALL]}
          resources:
            requests: {cpu: 50m, memory: 32Mi}
            limits: {memory: 128Mi}
---
apiVersion: v1
kind: Service
metadata:
  name: taskmill-ui
spec:
  selector: {app: taskmill-ui}
  ports:
    - {name: http, port: 80, targetPort: http}
```

- **Pin the tag** to the taskmill version your apps run: a newer dashboard
  exits on an older schema.
- **`TASKMILL_UI_ADDR=:8080`** listens on every address. The default,
  `localhost:8080`, is out of reach from outside the container.
- **The probe only checks the port**: with a password, every path asks for
  it, and `taskmill-ui` has no health path.
- **Reach it** through your Ingress with TLS, or with
  `kubectl port-forward service/taskmill-ui 8080:80`.

The image holds only the binary, CA certificates, and time zones, and runs as
a non-root user. For `TASKMILL_SCHEMA`, `TASKMILL_UI_READ_ONLY`, and the other
settings, see the
[`taskmill-ui` docs](https://pkg.go.dev/github.com/code19m/taskmill/cmd/taskmill-ui).

## Migrate in a Job

You need this only when your deploys apply schema changes before new code
starts. Otherwise, `Migrate` at the start of each process is enough
([Run migrations](deploys.md#run-migrations)).

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: shop-migrate-1-4-0
spec:
  backoffLimit: 3 # a migration that gave up waiting for a lock tries again
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: migrate
          image: registry.example.com/shop:1.4.0
          args: [migrate] # calls taskmill.Migrate, then exits
          env:
            - name: DATABASE_URL
              valueFrom: {secretKeyRef: {name: shop-db, key: url}}
```

Run it as a Helm `pre-upgrade` hook, or in an earlier Argo CD sync wave.

## See also

- [Deploys](deploys.md): shutdown, migrations, and two versions side by side
- [Autoscaling](autoscaling.md): KEDA for workers
- [`Worker.Health`](https://pkg.go.dev/github.com/code19m/taskmill#Worker.Health) on pkg.go.dev

**Next:** [Autoscaling](autoscaling.md)
