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
- a way around `ui.Config.ReadOnly` or `ui.Config.BasicAuth`.

How you expose the dashboard is a deployment choice, and not in scope. The
`ui` handler asks for a login only when `ui.Config.BasicAuth` is set.
Otherwise, your application must put its own authentication in front of it.
A dashboard exposed without authentication, or with basic authentication
over plain HTTP, is a deployment issue, not a vulnerability in taskmill.

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

- [Security in production](docs/production/security.md): how to protect the
  dashboard and the database
- [Compatibility](docs/reference/compatibility.md): versions and support
- [CONTRIBUTING.md](CONTRIBUTING.md): how changes get merged

**Next:** [Security in production](docs/production/security.md)
