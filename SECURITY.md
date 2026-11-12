# Security policy

This page says which versions get security fixes, what counts as a
vulnerability, and how to report one.

## Supported versions

Only the latest minor release gets fixes, including security fixes. This is
true before and after v1.0.0. [Compatibility](docs/reference/compatibility.md)
explains how versions work.

## Scope

Flaws in taskmill's own code are in scope, for example:

- SQL injection through any argument of taskmill's API;
- cross-site request forgery (CSRF) or cross-site scripting (XSS) in the
  dashboard (the `ui` package);
- a way around `ui.Config.ReadOnly` or `ui.Config.BasicAuth`;
- a way for a database role to read, run, change, or cancel a task without
  the table privileges for it.

How you expose the dashboard is a deployment choice, and not in scope. The
`ui` handler asks for a login only when `ui.Config.BasicAuth` is set.
Otherwise, your app must put its own authentication in front of it. A
dashboard exposed without authentication, or with basic authentication over
plain HTTP, is a deployment issue, not a vulnerability in taskmill.

Who can connect to your database is a deployment choice too. PostgreSQL has
no privilege for `NOTIFY`, so any role that can connect to the database can
send notifications on taskmill's channel. taskmill treats them as hints, and
reads the truth from its tables before it acts.

So a forged notification only changes when workers query the database,
which costs extra queries. That load is not a vulnerability in taskmill. A
notification that makes taskmill run, change, or cancel a task is one.

## Reporting a vulnerability

Please do not report security issues in public issues, discussions, or pull
requests.

Report them privately through
[GitHub private vulnerability reporting](https://github.com/code19m/taskmill/security/advisories/new).
Include as much as you can of:

- the affected version or commit;
- what the issue is, and its impact;
- the steps or a proof of concept to reproduce it.

What happens next:

| When | What we do |
|---|---|
| Within 5 working days | We aim to confirm that we got your report. |
| While we work on a fix | We keep you informed. |
| Before we publish | We agree on a disclosure date with you. |
| In the advisory | We credit you, unless you prefer otherwise. |

## How fixes ship

A fix ships as a patch release, with:

- a GitHub Security Advisory;
- a CVE;
- an entry in the Go vulnerability database, so that `govulncheck` reports
  it.

## See also

- [Security](docs/production/security.md): how to protect the dashboard, the
  database, and the data in your tasks
- [Compatibility](docs/reference/compatibility.md): versions and support
- [CONTRIBUTING.md](CONTRIBUTING.md): how changes get merged

**Next:** [Security](docs/production/security.md)
