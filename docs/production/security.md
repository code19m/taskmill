# Security

This page shows how to protect the dashboard, the database, and the data in
your tasks. It also shows how to check a download, and how to report a
vulnerability.

## Which should I use?

| Situation | What to use | Why |
|---|---|---|
| The dashboard is mounted in your Go app | **Your app's own login around the `ui` handler** | Recommended. Each person logs in as themselves, so the logs say who did what |
| `taskmill-ui` behind a proxy that logs people in, such as oauth2-proxy | The proxy, and no `TASKMILL_UI_PASSWORD` | Single sign-on. `taskmill-ui` warns at start that it has no password: that is expected here |
| `taskmill-ui` on its own | `TASKMILL_UI_PASSWORD`, over HTTPS only | Simple, but everyone shares one user |

## Protect the dashboard

The dashboard can retry, cancel, and delete every task, and it shows every
payload. `ui.New` has **no login** of its own.

- Put it behind your login, such as
  `mux.Handle("/taskmill/", requireAdmin(dashboard))`, and serve it over HTTPS.
- Give people who only look a read-only dashboard: `ui.Config.ReadOnly`, or
  `TASKMILL_UI_READ_ONLY=true`.
- Pass a `ui.Config.Logger` that adds the signed-in user. Each action is
  logged with the message `dashboard action`, so you see who did what.
- Basic authentication (`TASKMILL_UI_PASSWORD`) has one user for everyone,
  and no lockout or rate limit. Use a long, random password.

The dashboard already protects its forms against cross-site request forgery,
and other sites cannot show it in a frame.
[Protect it](../guides/dashboard.md#protect-it) lists every setting.

## Know who can reach the database

taskmill relies on PostgreSQL for access control: who can connect, and what
each role may do.

- **Table privileges protect the tasks.** A role that cannot read taskmill's
  tables sees no payload. A role that cannot write them cannot enqueue,
  change, cancel, or delete a task.
- **Table privileges do not cover notifications.** PostgreSQL has no
  privilege for `NOTIFY`. So any role that can connect to the database can
  send [notifications](../concepts/glossary.md#notification) on taskmill's
  channel, whatever its rights on the tables.
- **taskmill treats a notification as a hint.** Before a worker acts on one,
  it reads the truth from taskmill's tables. So a forged notification cannot
  run, change, or cancel a task. It only changes when workers query the
  database, which costs extra queries. That cost has a limit: however many
  notifications arrive, a worker reads the cancel requests, the queue
  settings, and each idle queue at most about 50 times a second.
- **taskmill waits only for locks on its own tables.** A role needs rights
  on a table to lock it. taskmill takes no advisory locks, which PostgreSQL
  lets every role of a database take and hold.
- **Let only code that you trust connect to the database.** Services that
  must not affect each other's load belong in separate databases:
  notifications never leave their database.
- **Encrypt the connection.** Payloads travel over it. Add
  `sslmode=verify-full` to every connection URL, so that pgx uses TLS and
  checks the server's certificate.

## Give each process the smallest role

Workers must own taskmill's tables
([Database](database.md#run-workers-as-the-owner-of-taskmills-tables)).
Other processes can use a role with fewer rights. In this example, `shop` is
the owner, and the role that runs `Migrate`:

```sql
-- Reads and writes, but owns nothing: for web processes that enqueue,
-- and for a dashboard with actions.
CREATE ROLE shop_web LOGIN PASSWORD '...';
GRANT USAGE ON SCHEMA taskmill TO shop_web;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA taskmill TO shop_web;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA taskmill TO shop_web;
-- The tables and sequences that later releases add.
ALTER DEFAULT PRIVILEGES FOR ROLE shop IN SCHEMA taskmill
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO shop_web;
ALTER DEFAULT PRIVILEGES FOR ROLE shop IN SCHEMA taskmill
    GRANT USAGE ON SEQUENCES TO shop_web;

-- Reads only: for a read-only dashboard.
CREATE ROLE taskmill_viewer LOGIN PASSWORD '...';
GRANT USAGE ON SCHEMA taskmill TO taskmill_viewer;
GRANT SELECT ON ALL TABLES IN SCHEMA taskmill TO taskmill_viewer;
ALTER DEFAULT PRIVILEGES FOR ROLE shop IN SCHEMA taskmill
    GRANT SELECT ON TABLES TO taskmill_viewer;
```

| Role | What it can do | What it cannot do |
|---|---|---|
| `shop`, the owner | Everything: `Migrate`, workers, enqueue calls, the dashboard | — |
| `shop_web` | Every enqueue call, `outbox.Store`, the operations API, and every action in the dashboard | Migrate an outdated schema. A worker with this role runs tasks, but loses the history of completed ones |
| `taskmill_viewer` | Show every page of a dashboard with `ReadOnly`, payloads included | Enqueue, change, cancel, or delete a task |

- **Use the viewer role only with `ReadOnly`.** Otherwise the dashboard
  shows its action buttons, and each action fails.
- **Grant on all tables, not on single ones.** taskmill's tables are
  internal, and a minor release can add one. A service that only enqueues
  into another service's schema can use a shorter list, at that risk:
  [Split work across services](../recipes/split-across-services.md#check-versions-and-privileges).

## Keep secrets out of tasks and errors

| Data | Who can see it |
|---|---|
| Payloads | Everyone with dashboard access: the task page shows them |
| A handler's error messages | The dashboard (each task's last 20 errors), the logs, and the spans' status |
| Metadata | The dashboard, but never the logs |
| Connection errors | The logs show the host, user, and database, but never the password |
| Metrics | Queues, kinds, states, and error classes, but no task IDs or payloads |

So keep secrets out of payloads and out of your handlers' errors. Store an
ID in the payload, and look the secret up in the handler.

## Know how long task data stays

Payloads often hold personal data, such as an email address. This is where
taskmill keeps a task's data, and for how long:

| Data | Kept |
|---|---|
| A task that waits or runs: payload, metadata, and errors | Until it finishes |
| A completed task | 7 days, plus up to 1 day (`HistoryRetention`). Not kept for a kind with `DiscardCompleted`, or for published outbox messages |
| A dead or canceled task | 30 days (`DeadRetention`). A dead task that halts its FIFO group stays until someone retries or deletes it |
| A task's errors | The last 20, with up to 4 KiB of each message |
| A FIFO group's key | As plain text, with each task of the group |
| A dedupe key | Only as a SHA-256 hash, until its window ends at the latest |
| The charts' activity | Counts per queue only, for 24 hours |

- **To keep data for a shorter time,** change the retention:
  [Choose how long finished tasks are kept](../guides/operations-api.md#choose-how-long-finished-tasks-are-kept).
- **To erase one person's tasks,** call `Client.Delete` for each task, or
  `Client.DeleteMatching` for a queue, kind, state, or FIFO group. Both also
  delete completed tasks from the history. Neither deletes a running task.
- **taskmill cannot search by payload.** So store the task's ID with the
  person's data when you enqueue, or keep personal data out of the payload:
  store an ID, and look the data up in the handler.
- **Copies outside taskmill stay.** Your backups, the database's
  write-ahead log, your logs, and your traces keep what they got, for as
  long as you keep them.

## Verify a download

The release workflow builds each release's archives and the `taskmill-ui`
image, and signs an attestation for them. Check one with GitHub's CLI:

```sh
gh attestation verify oci://ghcr.io/code19m/taskmill-ui:0.6.0 \
  --repo code19m/taskmill \
  --signer-workflow code19m/taskmill/.github/workflows/release.yml
```

For an archive, pass its file name instead of the `oci://` address.

## Report a vulnerability

Report it privately, through
[GitHub private vulnerability reporting](https://github.com/code19m/taskmill/security/advisories/new),
never in a public issue. [SECURITY.md](../../SECURITY.md) says what happens
next, and which releases get fixes.

## See also

- [SECURITY.md](../../SECURITY.md): the full policy
- [The dashboard](../guides/dashboard.md): the ways to run it
- [Database](database.md): the owner role, and the connection settings
- [`ui.Config`](https://pkg.go.dev/github.com/code19m/taskmill/ui#Config) on pkg.go.dev

**Next:** [Troubleshooting](troubleshooting.md)
