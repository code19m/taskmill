# 0006. Health and scaling read paths

- Status: Proposed
- Date: 2026-10-26

## Context

On Kubernetes, operators need two things from a worker that taskmill did not
offer as such:

- **A health signal** for probes. A worker can be up but cut off from the
  database, and then it cannot keep its tasks' leases.
- **A scaling signal**, such as queue depth or lag, for an autoscaler like
  KEDA. `Client.Queues` and the `otel` module's gauges already report both.
  KEDA can also run SQL against PostgreSQL itself, which invites pointing it
  at taskmill's tables.

## Decision

**Health is a method, not an HTTP handler.** `Worker.Health` returns nil
while `Run` runs and its last heartbeat succeeded within `LeaseDuration`, and
an error that says why otherwise. It reads only memory, so probes cost the
database nothing. The core does not serve it over HTTP: applications already
run an HTTP server or a health aggregator, and choose its port, path, status
codes, and the other checks it combines. A handler in the core would fix
those choices for them and add a transport to a package that has none. The
guide shows the few lines that serve it.

**Health is for readiness, not liveness.** It fails while the database is out
of reach, which a restart does not fix: a liveness probe on it would restart
every worker during an outage.

**Scalers read `Client.Queues` or the `otel` gauges, never the tables.**
taskmill's tables, their columns, and the statistics in them are internal:
migrations change them in minor releases. We considered a stable SQL view for
scalers, such as KEDA's PostgreSQL scaler. It would make a part of the schema
a public interface, which every later migration must keep, for a need that
two existing paths meet: the gauges through Prometheus, and `Client.Queues`
behind a small JSON endpoint for KEDA's Metrics API scaler.

## Consequences

- Probes and scalers add no load beyond what the maintainer already does:
  health reads memory, and the queue figures come from statistics that the
  maintainer refreshes every 10 seconds.
- An application without an HTTP server must add one, or a few lines to an
  existing one, to expose health.
- An operator who wants SQL for scaling must write a small endpoint instead.
  In return, upgrades never break their scaler.
