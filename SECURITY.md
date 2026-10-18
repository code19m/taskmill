# Security policy

## Supported versions

Before v1.0.0, only the latest minor release gets fixes, including security
fixes. See [Upgrading](docs/guide.md#upgrading) for the versioning policy.

## Scope

In scope are flaws in taskmill's own code, for example:

- SQL injection through any argument of taskmill's API;
- cross-site request forgery (CSRF) or cross-site scripting (XSS) in the web
  interface of the `ui` package;
- a way around `ui.Config.ReadOnly`.

The `ui` handler has no authentication, by design: applications mount it
behind their own. Exposing it without authentication is a deployment issue,
not a vulnerability in taskmill.

## Reporting a vulnerability

Please do not report security issues in public issues, discussions, or pull
requests.

Report them privately through
[GitHub private vulnerability reporting](https://github.com/code19m/taskmill/security/advisories/new).
Include as much as you can of:

- the affected version or commit,
- a description of the issue and its impact,
- the steps or a proof of concept to reproduce it.

We aim to acknowledge reports within five working days. We will keep you
informed while we work on a fix, agree on a disclosure date with you, and
credit you in the advisory unless you prefer otherwise.

## How fixes ship

A fix ships as a patch release, with a GitHub Security Advisory, a CVE, and
an entry in the Go vulnerability database, so that `govulncheck` reports it.
