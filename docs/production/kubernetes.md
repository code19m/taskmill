# Kubernetes

This page gives you manifests to copy: workers, their probe, the dashboard,
and a migration Job. It also shows how to set two numbers: the
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
            - {name: HEALTH_ADDR, value: ":8081"}
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
  argument, such as `shop worker` or `shop migrate`:
  [One binary, two roles](deploys.md#one-binary-two-roles) shows the code.
- `HEALTH_ADDR=:8081` is read by the binary of
  [Ready to ship](../tutorial/7-ready-to-ship.md), not by taskmill. Its
  default, `localhost:8081`, cannot be reached by the kubelet.
- Workers get no traffic, so they need no Service and no `preStop` hook.
- A worker needs little memory, but a running batch, as the outbox
  relay's, holds its tasks' payloads ([Performance](../reference/performance.md#resource-use)).
  Size the limits for those, and for your handlers.
- Run at least 2 replicas. With one, a lost node stops all work, and the
  [maintainer](../concepts/glossary.md#maintainer) with it, until Kubernetes
  starts the pod elsewhere: about 5 minutes by default. A
  PodDisruptionBudget makes a node drain stop one worker at a time:

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: worker
spec:
  maxUnavailable: 1
  selector:
    matchLabels: {app: worker}
```

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

Listen on every address, as `:8081` does. The kubelet calls the pod's IP
address, so a server on `localhost` never answers it.

`Health` fails while `Run` does not run, and when the worker has not reached
the database for longer than `LeaseDuration` (30 s). It also fails until a
new worker has read the pauses and limits of its queues: before that, the
worker takes no task. It also fails while handlers that did not return 5
seconds after their context ended hold every slot: such a worker can run
nothing. It reads only memory, so the probe costs the database nothing.

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
`terminationGracePeriodSeconds` to the grace period that
[Stop workers without losing work](deploys.md#stop-workers-without-losing-work)
asks for. The defaults fit: 20 seconds, and Kubernetes' 30. For a
`DrainTimeout` of 2 minutes, set 130.

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

So this app needs `max_connections` of about 200, or a pooler in front
whose [pool you size](connection-poolers.md#size-the-poolers-pool) too.

## Run the dashboard

We recommend that you mount the dashboard in your app with `ui.New`
([The dashboard](../guides/dashboard.md)). Without a Go HTTP server, run the
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
          image: ghcr.io/code19m/taskmill-ui:0.15.0 # the taskmill version your apps run
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
  [the `taskmill-ui` reference](../reference/taskmill-ui.md).

## Migrate in a Job

In production, run `Migrate` in this Job before the rollout
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
