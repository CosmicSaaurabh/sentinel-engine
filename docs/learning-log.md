# Learning Log

A running record of concepts learned while building the Sentinel Engine.
Append one entry per concept, newest at the top.
Revisit this file periodically to revise.

## Entry Template

```
### 2026-08-25 - A gauge that runs a query runs it on the scrape path
- Context: exposing queue depth by status as a Prometheus metric.
- What I learned: Micrometer evaluates a gauge every time Prometheus scrapes it, so a gauge backed by `SELECT count(*) ... GROUP BY status` puts a full aggregate over the highest-churn table in the system on the scrape path, several times a minute, from every instance at once. Worse, the cost grows with the table, so monitoring gets more expensive exactly as the system gets busier, and a scrape storm during an incident adds load precisely when there is least to spare. Refreshing into an `AtomicLong` on a schedule you control decouples the two; the value is a few seconds stale, which for a depth trend is not a cost at all.
- Gotcha: also learned that a scheduled refresh which throws is silently cancelled, and frozen gauges are worse than missing ones, because a flat line reads as a healthy steady state.
- Reference: engine/src/main/java/dev/sentinel/engine/infra/QueueMetrics.java

### 2026-08-25 - Trace context belongs on the workflow, not in the poll's metadata
- Context: propagating trace context from the engine to a worker so a task's spans join the trace that submitted it.
- What I learned: the reflex is to put trace context in gRPC metadata, which is where it normally lives. That is wrong here, because it would propagate the trace of the *poll call*, and a poll is not a thing anyone wants to trace: it can return several tasks belonging to several unrelated workflows, and it may have spent twenty seconds waiting for work that has nothing to do with the worker that eventually received it. The context has to travel with the task, in the message. It also has to be stored rather than held in memory, because the workflow outlives the submitting request by minutes or hours.
- Gotcha: the SDK carries no tracing library, because it carries no framework, so it can expose the context and put the trace id in MDC but cannot emit spans. That boundary is a consequence of the dependency rule rather than an oversight, and writing it down stops someone hunting for spans that were never emitted.
- Reference: docs/high-level-design/HLD-002-grpc-transport.md, engine/src/main/java/dev/sentinel/engine/infra/TraceContext.java

### 2026-08-25 - MDC is thread-local and thread pools are reused
- Context: putting workflow and task ids on every log line, in both the engine and the SDK.
- What I learned: setting MDC without restoring it is worse than not setting it at all. Engine request threads and SDK activity threads are pooled, so a value left behind reappears on the next unrelated piece of work that lands on the same thread; the logs are then confidently wrong and every conclusion drawn from them is suspect. The fix is to scope it, restoring the previous value rather than clearing, since contexts nest. Making the only entry point a try-with-resources or a scoped helper means the restore cannot be forgotten.
- Gotcha: `-Xlint` rejects an unused try-with-resources variable, which pushed the SDK to an explicit try/finally - and that turned out to read better anyway, since the context needs to cover the catch blocks too so the SDK's own warnings carry the ids.
- Reference: engine/src/main/java/dev/sentinel/engine/infra/TaskLogContext.java, worker-sdk/src/main/java/dev/sentinel/worker/LogContext.java

### 2026-08-25 - Full jitter makes a schedule untestable by sampling, so test the bound instead
- Context: the requirement that retry attempt timestamps match the backoff schedule within tolerance.
- What I learned: with full jitter the delay is uniform over zero to the ceiling, so any single measurement is consistent with almost any schedule and asserting on one sample is asserting on luck. The property worth pinning is the one a runaway retry would actually violate: no delay ever exceeds its attempt's ceiling, and the ceiling doubles. The exact schedule is then pinned separately in a unit test by injecting a generator that always returns the top of the range.
- Gotcha: the same non-determinism silently broke an unrelated metrics test, where a retried task was sometimes immediately claimable because the jitter happened to pick nearly zero. A test about a gauge should pin the deadline rather than inherit the schedule's randomness.
- Reference: engine/src/test/java/dev/sentinel/engine/service/RetryAndDeadLetterTest.java

### 2026-08-25 - Recovery ordering: fail the exhausted before re-queueing the rest
- Context: writing the reaper pass that handles expired leases.
- What I learned: the two branches look independent and are not. Re-queueing first moves a task to `PENDING`, where the exhausted-attempts query can no longer see it; it is then claimed, immediately violates `attempt < max_attempts`, and becomes invisible in the queue forever with no error anywhere. Doing the terminal branch first means every task is considered by exactly one branch. The general lesson is that when two queries select from overlapping sets and both mutate, the order they run in is part of the design rather than an implementation detail.
- Gotcha: the re-queue delay has the same shape of trap. Jittering it per batch instead of per task removes the lock contention and then recreates the stampede one step later, which is the more damaging half of the problem.
- Reference: docs/low-level-design/LLD-003-retry-reaper-idempotency.md section 4.2

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
