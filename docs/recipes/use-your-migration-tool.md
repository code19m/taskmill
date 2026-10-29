# Use your own migration tool

This recipe shows how to apply taskmill's schema changes with goose, Atlas,
or Flyway, instead of calling `Migrate` from your app.

**Your situation:** "Every schema change in our company goes through goose.
Apps must not change the schema when they start."

## Which should I use?

| Your team | Use | Why |
|---|---|---|
| Lets each app change its own schema | `Migrate` at the start of every process ([Deploys](../production/deploys.md)) | Nothing more to deploy. This is our default |
| Changes the schema in a step before each deploy | `Migrate` in a Job that runs your app's image ([Kubernetes](../production/kubernetes.md)) | The same code, run earlier |
| Sends every schema change through one tool | `taskmill.Migrations`, applied by your tool (this page) | One tool and one history for all changes |

## The preferred way

Write taskmill's migrations as files for your tool, with a small program in
your repository, such as `tools/taskmill-goose/main.go`. Run it after each
taskmill upgrade, with `go run ./tools/taskmill-goose ./migrations`, and
commit the files it writes. This one writes goose files:

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

Then apply the files with your tool, as you apply your own migrations.

## Why

- **`Migrations` returns every change, in order.** First version 0 creates the
  schema and its `migrations` table. Each later version changes the tables.
- **Each file records its version** in the schema's `migrations` table.
  `NewClient` reads that table. It fails with `ErrSchemaOutdated` until the
  schema has every version that the code needs.
- **The program writes each migration once.** It skips the files it wrote
  before, and gives each new file a version after your newest file. So your
  tool applies them in order, after your own changes.
- **Each file runs in its own transaction,** which some migrations need.
  goose, Atlas, and Flyway all do this by default.

## Use Atlas or Flyway

Change two lines of the program: the file name, and the first line of each
file.

| Tool | File name | First line |
|---|---|---|
| goose | `20261029120000_taskmill_0001_initial.sql` | `-- +goose Up` |
| Atlas | `20261029120000_taskmill_0001_initial.sql`, then run `atlas migrate hash` | none |
| Flyway | `V20261029120000__taskmill_0001_initial.sql` | none |

## Upgrade taskmill

1. Upgrade the Go module, run the program, and commit the new files.
2. Apply them with your tool **before** the new version rolls out. Your old
   code keeps working on the newer schema.
3. Deploy the new version.

If you skip step 2, the new version's `NewClient` fails with
`ErrSchemaOutdated`, and its processes don't start.

## Other options

| Option | Choose it when | The cost |
|---|---|---|
| `Migrate` at the start of every process | Apps may change their own schema | None. It is safe from many processes at the same time |
| Copy the SQL into your tool's files by hand | You upgrade taskmill rarely | It's easy to miss a version, or the `INSERT` that records it |

## Watch out

- **Don't also call `Migrate`.** If it runs before your tool, it applies the
  change itself, and your tool then fails on the same change: for example,
  on a column that already exists.
- **The role that runs workers must own taskmill's tables,** or be a member
  of the owner role. Every day, the maintainer adds and drops a partition of
  the history table, and PostgreSQL allows that only to the owner. So run
  your tool as that role, or make that role the owner afterwards.
- **A new schema has no history partitions until a worker starts.** The
  first tasks that complete may skip history, and the worker logs `task
  history has no partition for today` once. The maintainer creates the
  partitions within seconds.

## See also

- [Set up the database and client](../guides/setup.md): `Migrate` and the schema
- [Upgrading](../production/upgrading.md): the order of an upgrade
- [`taskmill.Migrations`](https://pkg.go.dev/github.com/code19m/taskmill#Migrations) and [its example](https://pkg.go.dev/github.com/code19m/taskmill#example-Migrations) on pkg.go.dev

**Next:** [Run in production](../production/README.md)
