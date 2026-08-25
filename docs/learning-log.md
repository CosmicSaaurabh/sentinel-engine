# Learning Log

A running record of concepts learned while building the Sentinel Engine.
Append one entry per concept, newest at the top.
Revisit this file periodically to revise.

## Entry Template

```
### 2026-08-25 - A queue in front of an activity pool converts directly into duplicate execution
- Context: sizing the worker SDK's executor, where the obvious design is a bounded queue in front of a fixed thread pool.
- What I learned: the usual reasoning about queues does not apply when the queued item holds a lease. A task reaches the pool only after being claimed, and a claimed task is one the engine believes is being worked on. Sitting in a queue makes no progress towards anything the engine measures, so queue depth converts one-for-one into lease expiry, re-queue, and the same work running twice. A bounded queue does not fix this; it only chooses how much duplicate execution to permit. The correct shape is a `SynchronousQueue` plus a semaphore the poller must acquire before asking for work, so the worker never claims what it cannot start immediately.
- Gotcha: "bounded queue" is normally the safe, responsible answer to backpressure. Here it is the unsafe one, and the reason has nothing to do with memory.
- Reference: docs/low-level-design/LLD-002-worker-sdk.md section 3.3

### 2026-08-25 - Heartbeating from a worker thread fails precisely when the worker is busiest
- Context: deciding which thread renews task leases in the SDK.
- What I learned: putting the heartbeat on the threads that run handler code creates a failure mode that is triggered by load rather than by faults. Saturate the activity pool with CPU-bound work and those threads cannot be scheduled in time to heartbeat; the leases expire, the engine concludes the worker is dead, and it hands the work to somebody else while the original worker is still doing it. The worker was healthy, and it missed its heartbeats *because* it was working hard. A dedicated thread doing nothing but one small batched call on a timer has no such coupling.
- Gotcha: the bug is invisible in testing, because it only appears under sustained saturation, and it looks exactly like a crash in the logs.
- Reference: docs/low-level-design/LLD-002-worker-sdk.md section 3.1

### 2026-08-25 - Reporting a still-running task as failed is worse than letting its lease expire
- Context: what a worker should do at shutdown with handlers that outlive the drain timeout.
- What I learned: the helpful-sounding option, reporting them failed so the engine re-queues them immediately, is the dangerous one. Those tasks are still executing, so telling the engine they are available starts a second copy alongside the first rather than after it. At-least-once quietly becomes at-least-twice-simultaneously, which is a much harder thing for an idempotency key to absorb than a delay. Abandoning them and letting the lease lapse costs one lease duration and preserves the invariant that the engine never knowingly runs a task in two places at once. Ordering matters as much: heartbeats must stop *after* the drain, not before, or tasks seconds from finishing lose their leases.
- Gotcha: also learned this the hard way in code - the first implementation called `shutdownNow()`, whose interrupt made handlers throw, which the wrapper classified as a failure and reported. The test caught it.
- Reference: docs/low-level-design/LLD-002-worker-sdk.md section 6.1

### 2026-08-25 - Long polling gets streaming's latency without streaming's state
- Context: choosing between plain polling, bidirectional streaming, and long polling for the worker protocol.
- What I learned: streaming looked obviously right for low dispatch latency, but it does not remove polling, it relocates it. Postgres has no push channel in this design, so an engine holding streams for two hundred workers still has to ask Postgres whether anything is runnable; what changes is who asks. In exchange it adds per-worker engine state, flow control, and instance affinity. Long polling keeps the engine stateless because the in-flight request *is* the state, moves the retry cadence from a thousand independent workers to one number on the engine, and can absorb a future push channel as an improvement to the inside of the wait with no protocol change.
- Gotcha: the deciding question was not "which is fastest" but "what state does each leave behind when a worker disappears mid-call".
- Reference: docs/high-level-design/HLD-002-grpc-transport.md section 3

### 2026-08-25 - An interrupted poll can leave a task claimed but undelivered
- Context: an end-to-end test failed roughly one run in three, with one task stuck RUNNING and no error logged anywhere.
- What I learned: closing a worker interrupts its in-flight poll, but the engine may already have committed that claim. The task is then owned by a worker that never received it and is shutting down, so nothing will ever report it. This is not a bug to fix in the client; it is exactly the gap leases exist for, and it is why the reaper is not optional. The same window opens on any lost response, network partition included.
- Gotcha: the symptom was maximally misleading - a workflow that submitted fine, a worker that logged no errors, and a failure that only appeared when a particular earlier test had run first. Dumping the task table in the assertion message turned a long investigation into one glance, which is now the standing habit for any wait-for-state assertion.
- Reference: engine/src/test/java/dev/sentinel/engine/api/grpc/WorkerSdkEndToEndTest.java

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
