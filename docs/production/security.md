# Security

This page shows how to protect the dashboard and the data in your tasks, how
to check a download, and how to report a vulnerability.

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

## Give the dashboard a read-only role

A read-only dashboard can use a database role that may only read taskmill's
tables:

```sql
CREATE ROLE taskmill_viewer LOGIN PASSWORD '...';
GRANT USAGE ON SCHEMA taskmill TO taskmill_viewer;
GRANT SELECT ON ALL TABLES IN SCHEMA taskmill TO taskmill_viewer;
-- Tables that later releases add; shop is the role that runs Migrate.
ALTER DEFAULT PRIVILEGES FOR ROLE shop IN SCHEMA taskmill GRANT SELECT ON TABLES TO taskmill_viewer;
```

Use this role only with `ReadOnly`. Otherwise the dashboard shows its action
buttons, and each action fails.

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

## Verify a download

The release workflow builds each release's archives and the `taskmill-ui`
image, and signs an attestation for them. Check one with GitHub's CLI:

```sh
gh attestation verify oci://ghcr.io/code19m/taskmill-ui:0.4.0 \
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
- [Dashboard](../guides/dashboard.md): the ways to run it
- [`ui.Config`](https://pkg.go.dev/github.com/code19m/taskmill/ui#Config) on pkg.go.dev

**Next:** [Troubleshooting](troubleshooting.md)
