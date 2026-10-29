# Production checklist

Check these points before you go live. Each line links to the page that
explains it.

## Database

- [ ] PostgreSQL is 15 or newer, and every process connects to the primary. ([Database](database.md#use-postgresql-15-or-newer-and-its-primary))
- [ ] The role that runs your workers owns taskmill's tables. ([Database](database.md#run-workers-as-the-owner-of-taskmills-tables))
- [ ] Each worker's `pool_max_conns` is its total `Concurrency` plus 5. ([Database](database.md#size-the-connection-pool))
- [ ] `max_connections` covers every pod at the peak of a rollout. ([Kubernetes](kubernetes.md#count-the-database-connections))
- [ ] `idle_in_transaction_session_timeout` is set, and `autovacuum_naptime` is 10 s or less. ([Database](database.md#keep-long-transactions-from-slowing-the-queue))
- [ ] Behind a pooler in transaction mode, `ListenConfig` is a direct connection. ([Connection poolers](connection-poolers.md))
- [ ] Every server's clock is synced with NTP. ([Database](database.md#keep-clocks-in-sync))

## Workers and deploys

- [ ] Every process calls `Migrate` before `NewClient`, or a Job does it first. ([Deploys](deploys.md#run-migrations))
- [ ] `Run`'s context ends on `SIGTERM`. ([Deploys](deploys.md#stop-workers-without-losing-work))
- [ ] The grace period is at least `DrainTimeout` plus 10 seconds. ([Kubernetes](kubernetes.md#give-workers-time-to-drain))
- [ ] The readiness probe calls `Worker.Health`, and no liveness probe does. ([Kubernetes](kubernetes.md#tell-kubernetes-when-a-worker-is-ready))
- [ ] At least one worker that can become the [maintainer](../concepts/glossary.md#maintainer) always runs. ([Autoscaling](autoscaling.md#scale-to-zero))
- [ ] Every worker that can become the maintainer defines the same `Schedules`. ([Deploys](deploys.md#run-two-versions-side-by-side))
- [ ] Every worker that reads a queue has a handler for every kind in it. ([Deploys](deploys.md#add-a-new-kind-during-a-rollout))
- [ ] Your handlers are safe to run twice. ([Do work once](../recipes/do-work-once.md))

## Watching and access

- [ ] You pass a `*slog.Logger` in `ClientConfig.Logger`. ([Monitoring](monitoring.md#logs-which-levels-matter))
- [ ] You have set up the alerts in Monitoring. ([Monitoring](monitoring.md#what-to-alert-on))
- [ ] The dashboard is behind a login and HTTPS, and people who only look get read-only access. ([Security](security.md#protect-the-dashboard))
- [ ] The `taskmill-ui` image tag matches the taskmill version your apps run. ([Kubernetes](kubernetes.md#run-the-dashboard))

## See also

- [Troubleshooting](troubleshooting.md): what to do when something goes wrong
- [Guarantees](../concepts/guarantees.md): what taskmill promises, and what it does not
- [Defaults and limits](../reference/defaults-and-limits.md)

**Next:** [Database](database.md)
