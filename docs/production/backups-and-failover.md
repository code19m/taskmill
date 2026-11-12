# Backups, failover, and replicas

Tasks are rows in your database, so your backups and your failover cover
them too. This page shows what a restore and a failover do to tasks, and the
steps that keep them safe.

## Which should I use?

| Situation | What to use | Why |
|---|---|---|
| Most apps | **A physical backup with point-in-time recovery**: your provider's snapshots, pgBackRest, or `pg_basebackup` | It opens no long transaction on the primary |
| You need a logical copy | `pg_dump`, on a replica or at a quiet hour | It is one long transaction, with the costs below |

While `pg_dump` runs on the primary, three things happen. All three end with
the dump:

- **Vacuum cannot clean the `tasks` table,** so fetches slow down
  ([Database](database.md#keep-long-transactions-from-slowing-the-queue)).
- **`Migrate` cannot alter a table.** A pending migration that does gives up
  after 5 seconds with `canceling statement due to lock timeout`. Run it
  again after the dump.
- **The [maintainer](../concepts/glossary.md#maintainer) cannot drop old
  history.** It logs `operation failed` for `apply retention`, and a minute
  later `operation still failing`, at Error level
  ([Troubleshooting](troubleshooting.md#the-log-says-apply-retention-keeps-failing)).

## What a restore does to your tasks

A restore takes every taskmill table back to the time of the backup. So does
a point-in-time recovery.

| What | After the restore |
|---|---|
| Tasks that finished after the backup | They are unfinished again, and run again |
| Tasks enqueued after the backup | They are gone, like every other row from that time |
| Tasks that were running at the backup | Their [leases](../concepts/glossary.md#lease) have run out. The maintainer [rescues](../concepts/glossary.md#rescue) them: each one uses an attempt, with the code `lease_lost`, and retries. On its last attempt, it becomes dead |
| Delayed tasks and retries whose time has passed | They all become due together |
| Schedules | Each one runs once for all the runs it missed |
| Dedupe keys | Keys that were taken after the backup are free again |
| Task IDs | They repeat: new tasks get the IDs of the tasks that are gone |

> [!WARNING]
> Task IDs repeat after a restore. If you build idempotency keys from the
> task ID ([Do work once](../recipes/do-work-once.md)), a new task can get
> the key of a lost one. The other system then skips the new task's work.
> Move the ID sequence forward before any process enqueues: step 3 below.

## Restore a backup

1. **Stop every process that uses taskmill,** web processes included.
2. **Restore the database.**
3. **Move the task ID sequence forward.** Run this as the owner of
   taskmill's tables. It skips a billion IDs, more than you can have used
   since the backup:

   ```sql
   SELECT setval('taskmill.tasks_id_seq', last_value + 1000000000) FROM taskmill.tasks_id_seq;
   ```

4. **Pause the queues whose tasks need a check first,** such as the queue
   that sends emails. Use the dashboard or `Client.PauseQueue`. Neither
   needs a running worker, so you can start your web processes for this.
5. **Start the workers.** The maintainer rescues the tasks that were
   running, makes due tasks available, and runs each schedule once. A paused
   queue keeps its tasks.
6. **Resume the queues one by one.** Watch
   [lag](../concepts/glossary.md#lag) and the dead count while the old work
   runs.

Tasks run again after a restore, so
[make handlers safe to run twice](../recipes/do-work-once.md) before you
need one.

## Fail over to a new primary

[When the database fails](database.md#when-the-database-fails) says what
taskmill does during a failover. This section is about your setup. First,
know what taskmill's fault tests cover
([Performance](../reference/performance.md#reliability-under-faults)):

| Situation | Tested? |
|---|---|
| PostgreSQL restarts, cleanly or after a crash of 15 seconds | Yes: workers continue, and no task is lost |
| A worker loses its connection to the database for longer than its lease | Yes: its late results do not count, and every task completes once |
| The database moves to another server: a promoted standby, a changed DNS name, or a managed service's failover | No. Test a failover of your own setup before you rely on it |

After a failover, the old primary can come back as a replica. So make pgx
refuse a read-only server. Add `target_session_attrs=read-write` to the
pool's URL, and to the URL of `ListenConfig` if you set one. pgx checks each
new connection, and tries the next host if you list several:

```text
postgres://shop@db-1.internal,db-2.internal/shop?target_session_attrs=read-write
```

### Check these points after a failover

- [ ] The log has `operation recovered` for each operation that logged
  `operation failed`. A maintainer that lost its role logs
  `no longer maintainer` instead.
- [ ] One worker logged `elected maintainer`, and Dashboard › Workers shows
  one maintainer.
- [ ] `taskmill_stats_age_seconds` is under 60 again, the worker pods are
  ready, and lag falls.
- [ ] No process logs `read-only transaction` (SQLSTATE 25006). One that
  does still uses the old primary: restart it.
- [ ] You know which tasks ran twice. The lines `attempt lost its lease;
  canceling it` and `rescued tasks whose workers lost their leases` name
  them. They are expected after an outage of more than about 20 seconds.
- [ ] With asynchronous replication, the last commits can be lost. Treat
  that as a small restore: tasks enqueued in those commits are gone, and
  their IDs can repeat. Move the ID sequence forward, as in step 3 above.

## Keep taskmill away from read replicas

A replica cannot run taskmill, because every fetch writes. A process that
connects to one starts, then fails:

| Call | On a read-only server |
|---|---|
| `Migrate` on a schema that is up to date, and `NewClient` | Succeed: they only read |
| `Worker.Run` | Returns `register worker: … cannot execute INSERT in a read-only transaction (SQLSTATE 25006)` |
| `Enqueue` and the other calls that write | Fail with the same SQLSTATE |

`target_session_attrs=read-write`, as above, makes the connection fail
instead, with `read only connection`.

> [!NOTE]
> A read-only dashboard only reads, so it can work on a replica. taskmill
> does not test that. Its numbers are then as old as the replica's delay.

## See also

- [Database](database.md): roles, pool size, and what taskmill does when the database fails
- [Recover from an incident](../recipes/recover-from-an-incident.md): retry many tasks safely
- [Guarantees](../concepts/guarantees.md): why a task can run twice
- [`Client.PauseQueue`](https://pkg.go.dev/github.com/code19m/taskmill#Client.PauseQueue) on pkg.go.dev

**Next:** [Deploys](deploys.md)
