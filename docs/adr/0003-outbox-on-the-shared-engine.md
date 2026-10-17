# 0003. Build the outbox on the shared engine

- Status: Accepted
- Date: 2026-10-18

## Context

Services that publish to a message broker need a transactional outbox: the
message must be stored in the same transaction as the business data, and then
published reliably and in order.

An outbox needs almost everything the task engine already provides: rows
written in the caller's transaction, safe concurrent fetching, leases and
fencing, retries and a dead-letter queue, ordering per key, batching,
notifications, leader election, migrations, and monitoring. These are the
hardest parts to get right. Outbox users, however, think in topics, keys, and
messages, not in tasks and handlers.

We considered three options:

1. A separate project with its own engine.
2. Outbox features mixed into the engine's API.
3. An outbox package built on top of the engine.

## Decision

We choose option 3. `github.com/code19m/taskmill/outbox` is a package built
only on the engine's public API. Its API speaks of topics, keys, and messages.
Users implement a small publisher interface with the broker client of their
choice; ready-made broker adapters are separate modules, as
[ADR 0001](0001-dependency-policy.md) requires.

The outbox is a package of the core module, not a module of its own, because it
has no dependencies of its own. It becomes a separate module only if that
changes.

## Consequences

- There is one engine to make correct. Services that need both background tasks
  and an outbox run one system, with one schema and one dashboard.
- The outbox tests the engine's API. If the outbox cannot be built cleanly on
  the public API, the API is wrong.
- The engine must provide what the outbox needs: FIFO groups, batch handlers,
  unique enqueue, deletion on completion, and stable task identities
  (requirements R6, R7, R9, R10, and R11 in the
  [scope document](../design/scope.md)).
- At very high volumes, around 100,000 messages per second, a different storage
  model is faster: rows that are only appended and read in order. The outbox
  API hides its storage, so such an engine can be added later without changing
  users' code.
