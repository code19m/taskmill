# Design

These documents describe how taskmill works and what it promises. They are
written before the code and kept in sync with it: a change that alters a
documented behavior updates the document in the same pull request.

| Document | Covers | Status |
|---|---|---|
| [Scope and use cases](scope.md) | What taskmill is for, the architectures it serves, and the requirements that follow | Draft |
| [Task lifecycle and guarantees](task-lifecycle.md) | Task states, transitions, and the promises taskmill makes | Draft |
| [Storage schema](schema.md) | Tables, indexes, and the SQL behind every transition, with measured query plans | Draft |
| [Public API](api.md) | The Go API: connecting, defining, enqueueing, and handling tasks, running workers, and operations | Draft |
| [Performance targets](performance.md) | The numbers taskmill promises, the reference hardware, how they are measured, and the baseline | Draft |

