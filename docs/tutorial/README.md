# Tutorial

Build a small shop, step by step. This section is for you if you learn best
by writing code. The shop grows in seven parts: each part adds one idea to
the same program, and ends with a program that runs.

| Part | You add | Time |
|---|---|---|
| [1. Your first task](1-first-task.md) | A program of your own: a task, a handler, and a worker | 15 min |
| [2. Tasks with your data](2-tasks-with-your-data.md) | Orders that are saved together with their tasks | 10 min |
| [3. When a task fails](3-when-a-task-fails.md) | Retries, a dead task, and the dashboard | 10 min |
| [4. Queues, delays, and schedules](4-queues-delays-schedules.md) | A second queue, a reminder that runs later, and a report every minute | 15 min |
| [5. The outbox](5-outbox.md) | Events for a message broker. This part is optional | 10 min |
| [6. Test it](6-test-it.md) | Tests against a real PostgreSQL | 10 min |
| [7. Ready to ship](7-ready-to-ship.md) | One binary with a web role and a worker role | 15 min |

Want to see taskmill run before you write code? Do the
[quickstart](../start/quickstart.md) first. It runs the program that you
have after part 3.

## What you need

- **Go 1.26** or newer, and `curl`.
- **A PostgreSQL that you may fill with test data.**
  [Step 1 of the quickstart](../start/quickstart.md#step-1-start-postgresql)
  starts one in Docker, and the shop connects to it by default.
- **Two terminals:** one for the shop, and one for your commands.

To use another database, set `DATABASE_URL` in the shop's terminal:

```sh
export DATABASE_URL='postgres://user:password@localhost:5432/shop?sslmode=disable'
```

## How each part works

- **Each step shows the code to add, and what you should see.** Your IDs,
  times, and durations differ from the ones on these pages.
- **Stop after any part.** Your program runs at the end of each one.
- **Compare with the finished programs** when yours does not work. After
  part 3, your program is
  [examples/quickstart/main.go](../../examples/quickstart/main.go). After
  part 7, it is [examples/tutorial/main.go](../../examples/tutorial/main.go).
- **The tutorial shows the steps, not the reasons.** Each part ends with
  "Go deeper": links to the pages that explain what you built.

**Next:** [Part 1: Your first task](1-first-task.md)
