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
- A worker itself needs about 20 MiB of memory, plus about 8 KiB per running
  task ([Performance](../reference/performance.md)). Size the limits for
  your handlers.

## Tell Kubernetes when a worker is ready

Serve `Worker.Health` on the port of the readiness probe:

```go
mux := http.NewServeMux()
mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
    if err := worker.Health(); err != nil {
        http.Error(w, err.Error(), http.StatusServiceUnavailable)
    }
})
server := &http.Server{Addr: ":8081", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
go func() { log.Print(server.ListenAndServe()) }()
```

`Health` fails while `Run` does not run, and when the worker has not reached
the database for longer than `LeaseDuration` (30 s). It reads only memory, so
the probe costs the database nothing. It does not see a stuck handler.

With this probe, a worker turns unready within about a minute of losing the
database, and ready again about 20 seconds after it returns. An unready
worker pauses a rolling deploy, which is right while the database is down.

> [!WARNING]
> Don't base a liveness probe on `Health`. It fails while the database is
> down, and a restart doesn't fix that: a new pod cannot even start until
> the database is back. If you want a liveness probe, point it at a path
> that does not call `Health`.

## Give workers time to drain

Kubernetes kills the pod when the grace period ends. So set
`terminationGracePeriodSeconds` to at least `DrainTimeout` plus 10 seconds:
after `DrainTimeout`, the worker cancels the tasks that still run, and waits
up to 5 seconds before it gives them back. The defaults fit: 20 seconds, and Kubernetes' 30.
For a `DrainTimeout` of 2 minutes, set 130. See
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
connections while they drain, and new pods open theirs. At worst, a
Deployment then has twice its replicas, or twice the autoscaler's maximum.
With short tasks, the peak is closer to the replicas plus `maxSurge`.

| Pods | Normal | Rollout peak |
|---|---|---|
| 4 workers, `Concurrency` 10, so `pool_max_conns=15` | 4 × 16 = 64 | 128 |
| 3 web pods with `pool_max_conns=10` | 30 | 60 |
| 1 `taskmill-ui` with `pool_max_conns=4` | 4 | 8 |
| **Total** | **98** | **196** |

So this app needs `max_connections` of about 200, or a pooler in front: see
[Connection poolers](connection-poolers.md).

## Run the dashboard

We recommend that you mount the dashboard in your app with `ui.New`
([Dashboard](../guides/dashboard.md)). Without a Go HTTP server, run the
`taskmill-ui` image in a Deployment of its own:

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
          image: ghcr.io/code19m/taskmill-ui:0.4.0 # the taskmill version your apps run
          env:
            - {name: TASKMILL_UI_ADDR, value: ":8080"}
            - {name: TASKMILL_UI_LOG_FORMAT, value: json}
            - name: TASKMILL_DATABASE_URL # ends in ?pool_max_conns=4
              valueFrom: {secretKeyRef: {name: shop-db, key: ui-url}}
            - name: TASKMILL_UI_PASSWORD
              valueFrom: {secretKeyRef: {name: taskmill-ui, key: password}}
          ports:
            - {name: http, containerPort: 8080}
          readinessProbe:
            httpGet: {path: /readyz, port: http}
            timeoutSeconds: 3 # /readyz waits up to 2 s for the database
          livenessProbe:
            httpGet: {path: /livez, port: http}
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
  `localhost:8080`, cannot be reached from outside the container.
- **The probes** need no login, and their paths stay at the root, even with
  `TASKMILL_UI_PREFIX`. `/readyz` fails while the database does not answer,
  so Kubernetes stops sending people to the pod. `/livez` does not check the
  database, so the pod is not restarted: a restart does not bring the
  database back.
- **`TASKMILL_UI_LOG_FORMAT=json`** writes JSON lines, for your log
  collector.
- **Reach it** through your Ingress with TLS, or with
  `kubectl port-forward service/taskmill-ui 8080:80`.
- **The other settings**, such as `TASKMILL_UI_READ_ONLY`, are in
  [Dashboard](../guides/dashboard.md).

## Migrate in a Job

You need this only when your deploys change the schema before new code
starts ([Run migrations](deploys.md#run-migrations)).

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
