# Use your own migration tool

**Situation.** "Every schema change in our company goes through goose. Apps
must not change the schema when they start."

If your apps may change their own schema, run `Migrate` instead: in
production, in a Job before the rollout
([Run migrations](../production/deploys.md#run-migrations)).

## The recommended way

Write taskmill's migrations as files for your tool, with a small program in
your repository, such as `tools/taskmill-goose/main.go`. After each taskmill
upgrade, run it with `go run ./tools/taskmill-goose ./migrations`, and commit
the files it writes. This one writes goose files:

```go
const schema = "taskmill" // your taskmill schema

func main() {
    dir := os.Args[1]
    ms, err := taskmill.Migrations(schema)
    if err != nil {
        log.Fatal(err)
    }
    version := time.Now().UTC()
    for _, m := range ms {
        suffix := fmt.Sprintf("_taskmill_%04d_%s.sql", m.Version, m.Name)
        if old, _ := filepath.Glob(filepath.Join(dir, "*"+suffix)); len(old) > 0 {
            continue // written by an earlier run
        }
        sql := "-- +goose Up\n" + m.SQL
        if m.Online { // CREATE INDEX CONCURRENTLY runs only outside a transaction
            sql = "-- +goose NO TRANSACTION\n" + sql
        }
        if m.Version > 0 { // record it, so that NewClient finds the schema up to date
            sql += fmt.Sprintf("\nINSERT INTO %s.migrations (version, name) VALUES (%d, '%s');\n", schema, m.Version, m.Name)
        }
        file := filepath.Join(dir, version.Format("20060102150405")+suffix)
        if err := os.WriteFile(file, []byte(sql), 0o644); err != nil {
            log.Fatal(err)
        }
        version = version.Add(time.Second) // one version per file, in order
    }
}
```

For Atlas or Flyway, change the file name, the first line, and what marks
an online migration, one that builds an index outside a transaction:

| Tool | File name | First line | An online migration |
|---|---|---|---|
| goose | `20261029120000_taskmill_0001_initial.sql` | `-- +goose Up` | `-- +goose NO TRANSACTION` before it |
| Atlas | `20261029120000_taskmill_0001_initial.sql`, then run `atlas migrate hash` | none | `-- atlas:txmode none` |
| Flyway | `V20261029120000__taskmill_0001_initial.sql` | none | A file beside it, named after it with `.conf` added, that holds `executeInTransaction=false` |

Then apply the files with your tool, **before** you deploy the new taskmill
version. Your old code keeps working on the newer schema. If you skip this
step, the new version's `NewClient` fails with `ErrSchemaOutdated`.

## Why

- **`Migrations` returns every change, in order.** Version 0 creates the
  schema and its `migrations` table. It refuses the roles that `Migrate`
  refuses: run your tool connected as the role that owns taskmill's
  tables. Each later file records its version there, and `NewClient`
  checks it.
- **The program writes each migration once,** and names new files after the
  current time. So your tool applies them after the files you already have.
- **Each file runs in its own transaction,** which some migrations need.
  goose, Atlas, and Flyway do this by default. An online migration runs
  outside one: `CREATE INDEX CONCURRENTLY` builds an index while writes
  continue, and PostgreSQL refuses it in a transaction.

## Other options

| Option | When it fits | The cost |
|---|---|---|
| `Migrate` at the start of every process | Development, or a small schema | An index build delays the start of every process |
| `Migrate` in a Job before each deploy | Production | One more step ([Kubernetes](../production/kubernetes.md)) |
| Copy the SQL into your tool's files by hand | You upgrade taskmill rarely | It's easy to miss a version, or the `INSERT` that records it |

## Watch out

- **Don't also call `Migrate`.** If it runs before your tool, it applies the
  change itself. Your tool then fails on the same change.
- **The role that runs workers must own taskmill's tables.** Every day, the
  [maintainer](../concepts/glossary.md#maintainer) adds and drops a
  partition of the history table, and PostgreSQL allows that only to the
  owner. Run your tool as that role, or change the owner afterwards.
- **A failed online migration leaves an invalid index.** Drop it with
  `DROP INDEX CONCURRENTLY`, then apply the migration again.
- **Some tools send a file as one query,** such as golang-migrate and
  dbmate. PostgreSQL runs a query that holds several statements in one
  transaction, and refuses `CONCURRENTLY` there. For such a tool, or a
  runner of your own, write each of an online migration's `Statements` in
  a file of its own, and the `INSERT` that records its version in one more.
- **Workers create the history partitions, not your tool.** Each worker
  creates the partitions of the next 7 days when it starts, before it runs
  a task. So the first completed tasks keep their history too. If the
  worker's role may not create them, it logs `prepare history partitions
  failed` at Warn level, and completed tasks skip history.

## See also

- [Set up the database and client](../guides/setup.md): `Migrate` and the schema
- [Upgrading](../production/upgrading.md): the order of an upgrade
- [`taskmill.Migrations`](https://pkg.go.dev/github.com/code19m/taskmill#Migrations) and [its example](https://pkg.go.dev/github.com/code19m/taskmill#example-Migrations) on pkg.go.dev

**Next:** [Run in production](../production/README.md)
