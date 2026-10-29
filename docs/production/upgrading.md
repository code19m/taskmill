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
in [Run two versions side by side](deploys.md#run-two-versions-side-by-side).

To skip a release, do one of these:

- Roll out each release in between, one after the other.
- Stop every old process that uses taskmill, web pods included, before the
  new ones start.

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
