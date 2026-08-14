# Learning Log

A running record of concepts learned while building the Sentinel Engine.
Append one entry per concept, newest at the top.
Revisit this file periodically to revise.

## Entry Template

```
### <date> - <concept title>
- Context: where in the project this came up.
- What I learned: the core idea in my own words.
- Gotcha: the mistake or misconception this corrected.
- Reference: link to doc, PR, or external article.
```

---

### 2026-07-31 - Saga pattern vs. simple write-ordering plus reconciliation
- Context: designing how the Redis dispatch-hint index (see HLD-001 addendum) stays in sync with Postgres without a shared transaction across the two systems.
- What I learned: a Saga is specifically the technique of breaking one logical operation into a sequence of local transactions across systems, each paired with a compensating (undo) action, used when a later step can fail after an earlier step already took effect that must be rolled back. If nothing ever needs to be undone, a saga is the wrong tool even though "two systems, need consistency" sounds saga-shaped on the surface. Here, ordering the writes (write Postgres first, only attempt Redis after that commit succeeds) plus a reconciliation sweep for missed writes is sufficient and simpler, because Redis is a lookup hint, not authoritative data - a failed or missing Redis write never leaves anything in an invalid state that needs compensating.
- Gotcha: conflated "multiple stores need to agree" in general with the specific saga technique. The deciding question is whether a partially-completed sequence leaves something that must be actively undone (needs a saga) or just something that needs to be retried/caught up later (needs ordering plus reconciliation, not a saga). Real saga practice is still earmarked separately for the deferred MongoDB DAG-storage exercise (HLD-001 Section 9), where a failed second write genuinely would need to undo the first.
- Reference: docs/high-level-design/HLD-001-discussion-log.md, Redis dispatch-hint index section

### 2026-07-31 - Deciding what's safe to cache in a distributed system
- Context: evaluating whether to cache actual claimed tasks vs. just workflow IDs in Redis, in front of the Postgres/Citus claim path.
- What I learned: the question that actually matters when adding a cache in front of a source of truth is not "can this cache be wrong," since it always eventually can be - it's "when this cache is wrong, does it produce an incorrect answer, or just a slow one?" Caching the actual claim state failed this test (a stale entry could cause a duplicate claim, a correctness bug). Caching just "this workflow might have runnable work" passed it (a stale entry only ever wastes a cheap, harmless lookup, since the real claim still goes through Postgres's guarded `FOR UPDATE SKIP LOCKED`).
- Gotcha: assumed "add a cache" was a single decision with one risk profile. It isn't - what you choose to put in the cache changes the entire risk profile, even when the caching mechanism itself is identical.
- Reference: docs/high-level-design/HLD-001-discussion-log.md, Redis dispatch-hint index section

### 2026-07-31 - Recognizing the same structural pattern twice (outbox relay reused for Redis)
- Context: designing how workflow IDs get into the Redis dispatch-hint index reliably after a Postgres commit.
- What I learned: "an event committed in Postgres needs to reliably reach a second system asynchronously" is a single repeatable structural problem, not a new problem each time a new second system shows up. The transactional outbox plus `SKIP LOCKED` relay already built for the Cassandra history feed solves this generically; extending it with a second relay consumer for Redis reuses the same drain query, the same `relayed_at`-style bookkeeping, and the same leaderless-relay-pool shape, instead of hand-building a second reconciliation query from scratch.
- Gotcha: almost designed a bespoke "scan Postgres for rows missing from Redis" query, which would have needed its own way to track what had already been synced - exactly what the outbox's `relayed_at` column already exists to do.
- Reference: docs/high-level-design/HLD-001-discussion-log.md, Redis dispatch-hint index section

### 2026-07-31 - Router query vs. distributed query under Citus
- Context: figuring out why the Claim Broker's "give me any pending task" query is expensive under Citus sharding, and how to avoid that cost.
- What I learned: Citus routes a query one of two ways. A router query has an equality filter on the distribution column (`workflow_id`) in the `WHERE` clause, so the coordinator can hash that value, know the single shard that holds it, and forward the whole query there - it then runs like ordinary single-node Postgres. A distributed (multi-shard) query has no such filter, so the coordinator must fan the query out to every relevant shard, wait for all of them, and merge results itself - far more expensive, and only as fast as the slowest shard. The original claim query design ("any pending task, anywhere") was unavoidably the second kind, since it had nothing to filter on.
- Gotcha: assumed sharding would make the claim query slower in a generic, uniform way. It's not uniform - the exact same query can be nearly free (router) or expensive and fan-out shaped (distributed), and the difference is entirely about whether the `WHERE` clause pins the distribution column.
- Reference: docs/high-level-design/HLD-001-discussion-log.md, Redis dispatch-hint index section

### 2026-07-13 - Project kickoff
- Context: Sentinel Engine MVP scoped and phased.
- What I learned: a durable execution engine can be reduced to three primitives, which are a persistent state machine, an atomic task claim, and a lease with recovery.
- Gotcha: "exactly-once execution" is impossible over a network in the general case; the real goal is exactly-once state transition with at-least-once execution and idempotent side effects.
- Reference: docs/tasks/mvp-task-breakdown.md
