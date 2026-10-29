# Run in production

This section is for you if you deploy taskmill, watch it, and fix it when
something goes wrong. Start with the checklist. Come back to the other pages
when you need them.

| Page | Read this when… |
|---|---|
| [Production checklist](checklist.md) | you prepare to go live |
| [Database](database.md) | you set up PostgreSQL: roles, isolation level, connections, vacuum, failover |
| [Connection poolers](connection-poolers.md) | you use PgBouncer, RDS Proxy, Supabase, or Neon |
| [Deploys](deploys.md) | you ship a new version of your app, or change a task |
| [Kubernetes](kubernetes.md) | you want manifests to copy |
| [Autoscaling](autoscaling.md) | you want KEDA to add and remove workers |
| [Monitoring](monitoring.md) | you set up alerts |
| [Security](security.md) | you protect the dashboard and the data in your tasks |
| [Troubleshooting](troubleshooting.md) | something is wrong right now |
| [Upgrading](upgrading.md) | a new taskmill release is out |

> [!TIP]
> Something is broken right now? Go straight to
> [Troubleshooting](troubleshooting.md).

**Next:** [Production checklist](checklist.md)
