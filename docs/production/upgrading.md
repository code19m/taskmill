# Upgrading

This page shows how to move to a new taskmill release without downtime. What
each kind of release may change is in
[Compatibility](../reference/compatibility.md).

## Upgrade step by step

1. **Read the [changelog](../../CHANGELOG.md)** for every release between
   yours and the new one. A change that needs work from you is marked
   **Breaking:**, and says how to migrate.
2. **Update the modules** to the next release, and fix what the changelog
   says. If you are one release behind, that is `@latest`:

   ```sh
   go get github.com/code19m/taskmill@latest
   go get github.com/code19m/taskmill/otel@latest # if you use it
   ```

3. **Migrate the schema.** If your processes call `Migrate` at start, the
   first new pod does it. If you migrate in a Job or with your own tool, do
   it now, before the rollout.
4. **Roll out your apps and workers.** Old and new pods share the queues
   while the rollout runs. Old workers finish or give back their tasks as
   they stop.
5. **Upgrade `taskmill-ui` last**, if you run it: set its image tag to the
   new version.
6. **Check** that Dashboard › Workers shows the new version for every
   worker, and that the logs show no Error lines.

## Upgrade one release at a time

Each release's schema works with the previous release's code, and CI tests
exactly that. So a rolling deploy is safe across one release. See the table
in [Compatibility](../reference/compatibility.md#old-code-on-a-newer-schema).

To skip a release, do one of these:

- Roll out each release in between, one after the other.
- Stop every old process that uses taskmill, web pods included, before the
  new ones start.

## What a migration repairs

A migration can add a rule to a table, such as "a retention period is at
least 1 hour". It first repairs the rows that break the rule. taskmill never
writes such a row: only SQL by hand does, or a tool of your own. So most
upgrades repair nothing.

| A row with | Becomes |
|---|---|
| A worker whose start or check-in time is infinite, whose queues or kinds hold a `NULL` or have two dimensions, or whose lease is below zero, above 300 years, or written with a negative field | Deleted. A worker that still runs registers again at its next check-in |
| A schedule whose next run time is infinite | A paused schedule. When you resume it, it runs right away: once for the time of the upgrade, and once more if it missed a regular run since |
| A schedule whose last run time is infinite | A schedule without a last run |
| A retention period below 1 hour, or above 300 years | 1 hour, or 300 years, which keeps tasks for good |
| A retention period written with a negative field, such as `7 days -1 hour` | The same period without one: `6 days 23:00:00` |

To see what an upgrade repaired, run this right after it, with your
schema's name. A repaired row has the time of the migration as its
`updated_at`, until something changes the row again. After an hour, the
maintainer does that to each schedule that it defines:

```sql
SELECT 'schedule ' || s.name AS repaired
FROM taskmill.schedules s, taskmill.migrations m
WHERE m.version = 6 AND s.updated_at = m.applied_at
UNION ALL
SELECT 'settings'
FROM taskmill.settings s, taskmill.migrations m
WHERE m.version = 6 AND s.updated_at = m.applied_at;
```

- **Resume a schedule that the upgrade paused,** on Dashboard › Schedules.
- **A deleted worker row leaves no trace in the database.** You see it only
  in the log of a worker that still runs, as the Warn line `worker was
  removed as stale; registering again`.
- **SQL that writes such a value now fails** with a check violation
  (SQLSTATE `23514`), which names the rule. taskmill's tables are internal:
  change settings, schedules, and queues through `Client`
  ([Compatibility](../reference/compatibility.md#what-may-change-in-a-minor-release)).

## Roll back

Rolling back one release is safe: the old code runs on the new schema. Deploy
the previous version of your apps, and of `taskmill-ui`.

Don't undo the migration. There are no down migrations, and you don't need
one.

> [!WARNING]
> Only the latest minor release gets fixes, security fixes included. If you
> stay one minor release behind, you get no patches. Plan to upgrade soon
> after each release.

## See also

- [Compatibility](../reference/compatibility.md): what never changes, and the supported Go and PostgreSQL versions
- [Deploys](deploys.md): migrations and rolling deploys
- [Changelog](../../CHANGELOG.md)

**Next:** [Reference](../reference/README.md)
