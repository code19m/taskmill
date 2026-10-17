# Architecture Decision Records

This directory records the significant decisions behind taskmill: the context,
the choice, and its consequences. Together they explain why the project looks
the way it does.

| ADR | Title | Status |
|---|---|---|
| [0001](0001-dependency-policy.md) | Dependency policy | Accepted |
| [0002](0002-supported-versions.md) | Supported Go and PostgreSQL versions | Accepted |

## Writing an ADR

1. Copy the template below into `NNNN-short-title.md`, using the next free
   number, and add it to the table above.
2. Open a pull request with the status **Proposed**. It becomes **Accepted**
   when merged.
3. Do not rewrite the decision of an accepted ADR. Write a new ADR instead, and
   mark the old one **Superseded by NNNN**.

```markdown
# NNNN. Title

- Status: Proposed
- Date: YYYY-MM-DD

## Context

What problem or constraint makes this decision necessary?

## Decision

What we decided, stated plainly.

## Consequences

What becomes easier or harder as a result, including the costs we accept.
```
