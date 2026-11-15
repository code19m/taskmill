# Writing doc comments

The doc comments are taskmill's reference. A reader looks an identifier up
on pkg.go.dev, in an editor, or with `go doc`, and must find the whole
answer there. This page says what a doc comment holds, and how the comments
and the pages in `docs/` share the work. The rules for words and sentences
in [Writing docs](writing-docs.md) bind doc comments too.

## Who holds what

| The reader asks | The answer lives in |
|---|---|
| What does it do? What is its default, its limit, its zero value? Which errors does it return? What about this edge case? | The doc comment of the identifier, and nowhere else |
| How do I use it? Which one do I choose? What do I do in production? | A page in `docs/`, which links to the identifier |
| What does this log line, metric, or timing mean? How do I run `taskmill-ui`? | A page in `docs/reference/`: these things have no identifier |
| Which identifier, or which page, do I need? | An index: names and links, with no facts of its own |

Three rules follow:

- **Docs never copy godoc.** A page says in a line what the reader needs at
  that point, with a rounded number, and links to the identifier, as
  `https://pkg.go.dev/github.com/code19m/taskmill#EnqueueOptions`.
- **Godoc links to pages, never to anchors.** A heading can change, and a
  released comment cannot follow it.
- **A fact changes in one place.** When you change a default, you change
  one comment. If a page must change too, the page copied the fact: replace
  the copy with a link.

So no page has a table of a struct's fields with their types, defaults, or
limits. A guide names the two or three options that it teaches, in its
prose or its example, and links to the type for the rest.

A doc comment may name a fixed timing that its contract depends on, as
`WorkerConfig.PollInterval` names the maintainer's heartbeat. The home of
the timings that no identifier owns is
[Fixed timings and limits](../reference/defaults-and-limits.md).

## The shape of a doc comment

Write the parts in this order, and leave out a part that has nothing to say.

| Part | What it holds | Example |
|---|---|---|
| First sentence | The name, and what the thing is or does. It must make sense alone: lists, search results, and editors show only this sentence | "Cancel cancels a task." |
| Contract | What it guarantees, and what the caller must do | "A running task's context is canceled, and the task becomes canceled when its attempt ends." |
| Default, limits, zero value | The exact values | "At most 128 characters. Default: [DefaultQueue]." |
| Errors | Each error that callers test for, and when it comes | "It returns [ErrNotFound] if no task has the ID." |
| Edge cases | One sentence each | "A delay of zero or less makes the task available right away." |
| Way on | For a package or a main type: one link to the page that teaches it | "The [enqueue guide] shows each option at work." |

- **Lead with the short answer.** In a long comment, the first paragraph
  holds the contract and the default, and the mechanics come after it.
  `WorkerConfig.PollInterval` is the model.
- **A field of a config struct names its default.** It says
  `Default: <value>.`, or `Required.`, or what the zero value means, as in
  "Empty means all queues." A `bool` needs none: false means off.
- **Say "may be nil"** for every pointer and interface that may be.
- **Say which call reports a wrong value,** and that the error wraps
  `ErrInvalidArgument`.
- **Link identifiers** as `[Name]` or `[Type.Method]`, so that pkg.go.dev
  links them. Read the result with `go doc`: a link that still shows its
  brackets is broken.
- **Godoc keeps two words of its own:** "installation" for a schema, and
  "the caller" or "the application" where a page says "you".

Link a page from a comment with a link definition, so that the sentence
stays clean:

```go
// The [enqueue guide] shows each option at work.
//
// [enqueue guide]: https://github.com/code19m/taskmill/blob/main/docs/guides/enqueue.md
type EnqueueOptions struct {
```

One link per type is enough: link from the type, not from each of its
methods and fields. pkg.go.dev shows the comment of a field as code, so a
field that needs a link has the URL itself in its comment.

## Package comments and large types

A package comment is a map, not a manual. It holds three things:

1. One sentence that says what the package is for. Put no doc link in it:
   lists show it as plain text.
2. The five or so identifiers to start from, each with a few words.
3. Where to read more: the guide, and the documentation home.

A type with many methods maps them the same way. `Client` groups its methods
by what the caller wants to do: enqueue, read, change tasks, change queues
and schedules, and change settings.

## Examples

- **Every main entry point has an `Example` function,** named after its
  function, type, or method, so that pkg.go.dev shows it there:
  `ExampleClient_Enqueue`. pkg.go.dev shows no example that is named after
  a constant or a variable. For those, write a package example with a
  suffix, such as `Example_errors`.
- **Show the output when the example needs no database.** End it with
  `// Output:`. Then `go test` runs it, and the reader sees the result.
  An example that needs a database only compiles.
- **Show one thing per example,** with names from a shop, as the pages do.

## What the tests check

`go test ./internal/docs/` reads the doc comments from the source. It fails
when:

| Check | It fails when |
|---|---|
| Defaults | A field of a config struct has no comment, or its comment names no default |
| Links from godoc | A comment links to a file that is not in the repository, or to an anchor. Or a package comment links to no page |
| Links to godoc | A page links to a package, an identifier, a field, a method, or an example on pkg.go.dev that the source does not have |
| Field tables | A page has a table of a struct's fields with a column for their type, default, or limits |
| Indexes | [Configuration](../reference/configuration.md) misses a config struct, or [Errors](../reference/errors.md) misses an error or a code |
| The command | [The `taskmill-ui` command](../reference/taskmill-ui.md) misses an environment variable that the command reads, or lists one that it does not read |

The linters check that every exported name has a comment that starts with
the name and ends with a period.

## See also

- [Go Doc Comments](https://go.dev/doc/comment): the syntax of links, lists, and headings
- [Writing docs](writing-docs.md): the style guide for words, sentences, and pages
- [CONTRIBUTING.md](../../CONTRIBUTING.md): the code standards

**Next:** [Documentation](../README.md)
