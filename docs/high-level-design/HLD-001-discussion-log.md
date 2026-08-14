# HLD-001 Discussion Log: System Architecture of the Sentinel Engine

Status: **converged, 2026-07-30.** The final doc, `docs/high-level-design/HLD-001-system-architecture.md`, has been written from everything below.
This file remains as the full reasoning trail and rationale behind that doc - useful for understanding *why* each decision was made, including ideas that were proposed and rejected along the way.
Items in the "Discussion order" and "Open questions" lists below that were not actively resolved were carried into the final doc's Section 9 (Deferred / Major Upgrades Needed Later) as explicit, tracked future work, not dropped.

Companion to issue #2.

---

## Scope pivot (2026-07-29)

The original MVP-scoped brief for this project has been intentionally widened.
New direction, as stated by the engineer:

- Design for millions of daily tasks, not a learning-scale MVP.
- Study how Uber and Temporal solved durable execution at scale: which technologies, which trade-offs.
- Use this project as a vehicle to learn Kafka, ZooKeeper, Redis, and wide-column stores (Cassandra), in addition to Postgres.
- Deliberately practice modern distributed patterns: CQRS, SAGA, Strangler Fig, event sourcing, log aggregation, service discovery, async messaging.
- Go deep on core Java and Spring Boot internals throughout, not just at the API layer.
- Treat this as a long-lived project the engineer will maintain and contribute to over an extended period, not a 1-2 week exercise.
- Benchmark performance and publish results (README-level) as a resume-relevant artifact.
- **Added 2026-07-30**: build an analytical dashboard for Sentinel Engine, sourced from the Cassandra history/event log, as an explicit learning-scope item.

**Open decision, not yet resolved:** `CLAUDE.md`'s current Non-Goals section (no custom consensus, PostgreSQL as sole coordinator, no multi-language SDKs) was written for the narrower MVP scope and now conflicts with parts of this pivot (for example, deliberately exploring leader election).
This needs to be reconciled - either by updating `CLAUDE.md` to reflect the new scope, or by explicitly scoping which non-goals still hold.
Not changed yet; the engineer's call.

## Cross-project split: Postgres concurrency moves to `splitwise-app`

Decision: the deep-dive into Postgres concurrency, row-level locking, `SELECT ... FOR UPDATE SKIP LOCKED`, isolation levels, and capacity math (connection pool sizing, rows/sec under contention) is being scoped into a **separate** project, `Desktop/projects/splitwise-app`, using its ledger/expense-splitting domain (a strong-consistency, high-contention use case: concurrent balance updates) as the teaching vehicle.

State of that project as of this log: 4 commits already exist (boilerplate, security config, groups service, expense service), but it has **no `CLAUDE.md`** yet, so it is not currently under the same gated review process as Sentinel Engine.

**Not yet done:** a dedicated HLD/scoping session for `splitwise-app`, including whether it adopts the same three-gate process (HLD -> LLD -> feature) and its own `CLAUDE.md`.
Deferred as a separate conversation, not part of this log.

Consequence for Sentinel Engine's HLD-001: capacity math (Postgres pool size, rows/sec, contention behavior under `SKIP LOCKED`) is **out of scope for this HLD** and will not be demanded here the way it normally would; the exactly-once task-claim mechanism itself (`FOR UPDATE SKIP LOCKED`) still belongs to Sentinel Engine, since it is what the engine actually needs to ship - only the *deep* concurrency-internals learning moved.

## Scope clarification (2026-07-30)

The pivot recorded above is not "use every technology discussed."
It is "design for ~100M DAGs/day, Uber/Temporal-level scale, and let each technology earn its place at that scale."
Every infra choice still needs the ADR justification described above; the scope being open-ended is about ambition and learning depth, not about mandating every tool be used.

## Decisions made so far

- **Task dispatch is pull, not push, and involves no broker (2026-07-30 clarification)**: workers call the engine over gRPC to ask for work; the engine runs the atomic `SELECT ... FOR UPDATE SKIP LOCKED` claim query against Postgres on the worker's behalf and returns a claimed task if one exists, or nothing. The Postgres tasks table *is* the queue - there is no message broker moving tasks from engine to worker, and no push dispatch decision for the engine to make. This reverses the push-shaped arrow in the engineer's original first-draft diagram; noting it explicitly since it was a live point of confusion. One legitimate exception, not yet designed: task **cancellation** is a genuine push (the engine must signal a worker mid-execution to stop) and isn't solved by the pull-claim model - parked as an open item.
- **Task dispatch mechanism confirmed: gRPC only, no Kafka (2026-07-30).** Considered and rejected using Kafka as a real dispatch/notification layer for workers, after establishing: (a) gRPC streaming RPCs give the same near-instant "wake up, don't blind-poll" property as a broker would, without one; (b) the exactly-once safety property comes entirely from the atomic Postgres claim regardless of whether a broker sits in front of it, so Kafka would add operational cost (partition management, consumer-group rebalancing, a second zombie-consumer fencing problem) without adding a capability not already available more simply.
  **Kafka's scoped role going forward**: introduced later specifically to feed an **analytical dashboard** sourced from Cassandra's history/event log - a genuine fan-out/multi-consumer justification (dashboard, potentially other future consumers), unlike task dispatch which is a single-worker-gets-one-task relationship with no fan-out need.
  **Still open, not answered**: whether ingestion (accept/parse/persist DAG) and scheduling (dependency-flip) should be split into two separate deployable services. Established that neither needs to be a leader and both are already leaderless/stateless, so no structural requirement forces a split - it would be a deliberate choice (e.g. for service-decomposition learning practice), not a necessity. Engineer has not yet stated whether this split is wanted.

- **Task queue / claim mechanism stays Postgres-native** (`SELECT ... FOR UPDATE SKIP LOCKED` or an atomic conditional `UPDATE ... RETURNING`), not Kafka. Kafka is being kept in scope for this project as a **learning and comparison exercise** (how consumer groups, offsets, and rebalancing handle at-least-once delivery differently from Postgres's exactly-once claim), documented separately, not as a replacement for the actual claim path.
- **FR6 (dead worker detection) uses a lease/fencing-token model**, pull-based (engine reaper polls a heartbeat timestamp column in Postgres), not push-based notification from an external system. Agreed by the engineer.
- **Engine HA must not be solved by hand-rolled leader-election/WAL-shipping in the application layer.** Postgres's own streaming replication already solves data-layer HA. The open question is application-layer coordination between multiple engine instances, with two candidate directions under research (see Open Questions).
- **FR4 needs two distinct mechanisms, not one:** claim idempotency (exactly one worker per task instance, solved by an atomic claim, not check-then-act) and execution idempotency (an idempotency key persisted before the first side-effect attempt, so a legitimately re-claimed task after lease expiry does not re-execute a side effect like `charge-payment`). Previously conflated as one problem; now tracked as two.
  **Mechanism clarified (2026-07-30)**: the idempotency key is generated once, at task creation, and stored durably on the task row - never regenerated per attempt, or retries would look like new requests to the downstream system. The dedup responsibility lives with the downstream system (e.g. a payment gateway's native `Idempotency-Key` support), not with Sentinel Engine or Kafka - no message/task queue technology can make an external side effect exactly-once by itself, because the caller can always crash between "side effect happened" and "recorded that it happened." Also clarified: this is not a Kafka-specific gap - Sentinel Engine's own `SKIP LOCKED` claim has the identical failure shape (worker crashes after side effect succeeds, before marking `COMPLETED`; lease expires; a second worker legitimately re-claims and retries), so the same idempotency-key mechanism is required regardless of queue technology.
  **Platform/client responsibility boundary confirmed (2026-07-30)**: Sentinel Engine (via `worker-sdk`) never implements task business logic (e.g. "call Stripe") - that is always client-written Activity code, matching Temporal/Cadence's Workflow-vs-Activity split. The engine's sole responsibility is to expose a stable, durable `taskId` per task instance (unchanged across retries) through the SDK's execution context; the client's activity code is responsible for actually passing that identifier to whatever third-party system it calls as an idempotency key. The engine cannot do that wiring itself since it has no knowledge of what a given task type calls out to.
  **Formal guarantee confirmed (2026-07-30): Sentinel Engine guarantees at-least-once task execution, explicitly not at-most-once.** A dead worker's task is always re-queued and retried rather than silently dropped, which is why duplicate execution is possible and the idempotency-key mechanism above is required for any task with an external side effect. For purely internal state mutations (claim, complete, dependency-flip), the engine achieves exactly-once *effect* unilaterally via atomic Postgres transactions - the at-least-once caveat applies specifically to tasks with side effects outside Postgres.
- **Backpressure is a capacity-vs-demand control-loop problem, not a majority-vote/consensus problem.** Signal: live-worker capacity (from the same heartbeat table as FR6) vs. count of claimable-but-unclaimed tasks. Decision on shed-vs-queue behavior still open (see below).
- **No custom consensus protocol will be built in Sentinel Engine.** That learning goal (implementing consensus from scratch) is already covered by the separate `redis-from-scratch` project; duplicating it here would be redundant, not additive.
- **Postgres streaming replication is the DB-layer HA mechanism**, confirmed, no open objection.
- **Throughput target finalized, revision 1 (2026-07-30)**: derived from 100M DAGs/day, ~10 tasks/DAG, ~3 transitions/task, 5x peak multiplier -> ~34,700 transitions/sec average, ~173,600 transitions/sec peak. Grounded against real published figures before finalizing (Uber Cadence: hundreds of millions of open workflows, tens of thousands of updates/sec; Temporal's own blog names persistence backend as the throughput ceiling, Postgres for typical production, Cassandra for massive scale - see sources in conversation).
- **Throughput target finalized, revision 2 - supersedes revision 1 (2026-07-30)**: revision 1 omitted heartbeat/lease-renewal writes entirely. A naive fix (average task duration 10 min, heartbeat every 10s, applied uniformly to every task) produced ~729,000/sec average and ~3.65M/sec peak - roughly 100-200x Cadence's own stated real-world ceiling, a strong signal the model, not the infrastructure, was wrong. Root cause: task duration is bimodal (payment-gateway calls mostly resolve in seconds, a minority hang for minutes on 3DS/bank confirmation) and only a fraction of tasks per DAG are payment-like at all; a single blended "average duration" misrepresents both ends and wrongly applies heartbeat cost to fast tasks that would never need it.
  Corrected model (estimates, not measured data - revisit once real task-duration telemetry exists): ~10 tasks/DAG, 9 fast auxiliary tasks/DAG (3 lifecycle writes each, no heartbeats - duration under one heartbeat interval), 1 payment-like task/DAG (90% resolve fast like the auxiliary tasks, 10% hit an ~11-minute hangup path needing heartbeat renewal every 10s, ~66 heartbeats when it occurs). Expected ~36.6 mutations/DAG (~3.66/task average).
  **Final target: ~42,400 mutations/sec sustained average, ~211,800 mutations/sec at 5x peak.** `PRD-001` NFR2 updated to this revision 2 figure (supersedes the revision 1 number written there earlier).
  Design target (100M/day derived) vs. benchmark target (whatever is actually provable on real hardware) are explicitly two different numbers; the gap between them must be bridged by a documented scaling argument, not asserted.
- **Consequence surfaced, not yet resolved**: even this corrected, lower target very likely exceeds what a single Postgres primary can sustain (Temporal's own benchmarking shows Postgres as a materially lower ceiling than Cassandra for this exact workload shape). The previously "numbers-gated, deferred" decision on horizontally partitioning the persistence layer is therefore now effectively triggered. Not yet designed; next major open topic once the current thread of discussion concludes.
- **Heartbeat-vs-lease-renewal-frequency decoupling raised, not yet resolved**: NFR4 ties recovery bound to "3 missed heartbeat intervals," which constrains how far lease-renewal write frequency can be reduced below raw heartbeat frequency without slowing failure detection - a real trade-off (faster recovery costs more writes), not a free optimization; parked for a later pass once the persistence-partitioning topic concludes.
- **Persistence model decided (2026-07-30): CQRS split.** Postgres owns current/claimable state (status, claim, lease/heartbeat renewal) - this is the exactly-once claim path and stays Postgres for its transactional guarantees. Cassandra owns the append-only history/event log (Temporal-style replay and audit trail), matching the real precedent researched earlier.
  Important scope of what this decides and doesn't: it avoids doubling Postgres's write burden by keeping history out of the same table/store as active-state mutations, and gives Postgres a bounded, prunable "active tasks" working set instead of an ever-growing log. It does **not** reduce the ~42,400/sec average (~211,800/sec peak) claim/status mutation figure itself - that load belongs to Postgres either way. Whether a single Postgres primary can sustain that claim-path load, or whether the claim path itself also needs horizontal partitioning, remains open and is not resolved by this decision.
  **New problem surfaced by this decision, not yet resolved**: writing to two different database engines (Postgres for state, Cassandra for history) on the same logical event is a dual-write consistency risk - one write can succeed while the other fails, with no distributed transaction spanning both engines to prevent it. Standard mitigation named but not yet designed: the **transactional outbox pattern** - write the history event into an outbox table inside the same Postgres transaction that updates state, then relay outbox entries to Cassandra asynchronously (via a relay process or CDC), so "state changed" and "history recorded" stay atomic with respect to Postgres. Also a deliberate opportunity to practice event-sourcing/outbox patterns, one of the engineer's stated learning goals.
- **Advisory-lock leader election is the chosen engine-instance coordination mechanism** (replacing the earlier hand-rolled leader-follower/WAL proposal). Explicitly flagged by the engineer as a decision worth highlighting in project documentation and resume framing: chose a boring, Postgres-native primitive over building custom leader election, and can defend why. Trade-offs under active discussion (see below); final write-up pending resolution of the single-global-leader-vs-sharded-leadership question at 100M-DAGs/day scale.
- **Claim-path sharding confirmed as required, not optional (2026-07-30)**: real benchmarked single-Postgres-primary throughput under production-realistic conditions (durability on, multiple indexes, multi-statement transactions, real timeouts) is ~1,000-1,200 sustained TPS; even a dedicated high-end box (16-core, top-tier NVMe, 62GB RAM) tops out ~1,875/sec. Against the ~42,400/sec average (~211,800/sec peak) claim/status target, that's a 20-40x gap at average and 100-200x at peak - too large for vertical scaling or tuning to close. Horizontal partitioning of the claim/status path is therefore confirmed necessary, not merely likely.
  **Mechanism recommended, not yet confirmed by engineer**: Citus (open-source Postgres extension, transparent sharding + distributed query planner/executor), over hand-rolled multi-primary routing, since Citus already solves the routing/rebalancing problem a manual approach would have to reinvent.
  **Open, unresolved**: (a) shard/distribution key - workflow_id proposed (keeps a workflow's tasks co-located, avoids cross-shard dependency-flip checks), but no operation has yet been checked for a genuine cross-shard need; (b) whether `SELECT ... FOR UPDATE SKIP LOCKED` behaves cleanly when fanned out across Citus shards is an explicit knowledge gap, not verified either way - needs research before this is treated as settled.

### Transactional outbox - proposed design (drafted 2026-07-30, pending engineer review, not yet confirmed)

Engineer requested this be drafted directly due to end-of-session fatigue; treat everything below as a starting proposal to accept, amend, or reject next session, not a locked decision.

**Outbox table (conceptual - exact DDL is LLD-001 scope), building on the engineer's own starting fields (workflow, task, status)**:
- `event_id` - unique per event; becomes the idempotency key on the Cassandra side, so a retried relay overwrites rather than duplicates.
- `workflow_id`, `task_id` (nullable - some events are workflow-level, e.g. "workflow completed," not task-level).
- `event_type` - what happened (the engineer's "status" field, renamed to avoid confusion with the task's own current-state `status` column): `SCHEDULED`, `CLAIMED`, `COMPLETED`, `FAILED`, etc.
- `sequence_number` - monotonically increasing **per workflow_id**, assigned inside the same transaction as the state mutation. This is what resolves the ordering concern raised earlier: Cassandra clusters by `(workflow_id, sequence_number)`, so reads come back in correct logical order regardless of what order the relay actually shipped events in - relay order stops needing to match creation order.
- `payload` - event-specific context (JSONB is fine at this level of detail).
- `created_at` - for operator debugging only, not the ordering key.
- `relayed_at` (nullable) - null means pending; this is the column the relay's claim query filters on.

**Relay mechanism**: reuses the same primitive as task claiming, since "many workers draining a shared table of pending work items" is the same structural problem - `SELECT ... FROM outbox WHERE relayed_at IS NULL ORDER BY id LIMIT N FOR UPDATE SKIP LOCKED`, run by any number of relay-worker instances concurrently, no leader needed, consistent with the rest of this system's leaderless philosophy. After a successful Cassandra write, the relay sets `relayed_at`.

**Cleanup**: relayed rows must be pruned on a retention window (e.g. periodic `DELETE WHERE relayed_at < now() - <window>`), or the outbox table itself becomes a second unbounded table in Postgres - exactly the problem moving history to Cassandra was meant to avoid.

**Not yet addressed, flag for next session**: exact retention window; whether `event_id` or `(workflow_id, sequence_number)` is the better Cassandra idempotency/clustering key; whether every event type needs an outbox row or only certain transitions (e.g. does every heartbeat-driven lease renewal need a history event, or only the FSM-level transitions?).

### Advisory-lock trade-offs (in discussion, 2026-07-30)

- **Connection-pooling gotcha**: session-scoped advisory locks are tied to a specific DB connection, not the application process. Must be held on a dedicated, unpooled connection - never through the same HikariCP pool (or PgBouncer transaction-pooling) driving normal queries, or the lock can be silently lost or double-held. Flagged as a hard implementation constraint for the eventual LLD/implementation, not just a note.
- **Failover latency is bounded by connection-death detection**, and differs by failure type: process crash (socket closes, Postgres notices quickly) vs. network partition (engine alive but unreachable; Postgres eventually kills the session via keepalive/statement timeout, potentially slower). Open question, posed back to the engineer: in the partition case, since Postgres is the sole coordinator, can a partitioned "leader" actually do anything harmful before detection kicks in, or is it naturally inert? Not yet answered.
- **No graceful step-down on crash** (only on clean shutdown). Acceptable gap for now; revisit if faster failover is needed later.
- **Single global lock key = single active scheduler globally**, regardless of engine instance count. At 100M DAGs/day (~1,150/sec average, higher at peak), open question posed to the engineer: does a thin single leader (owning only a narrow coordination duty, e.g. "what's newly schedulable," while claim/dispatch stays distributed via the atomic claim query across all instances) suffice, or does scheduling itself need to be sharded across multiple advisory-lock keys/partitions to avoid a single-instance ceiling? Not yet answered - this determines whether the final HLD documents one global leader or partitioned leadership.

### Backpressure policy - decided (2026-07-30)

- **Rejection behavior**: reject outright with `503 Service Unavailable` plus a `Retry-After` header when at capacity - no accept-and-queue path. Clean choice because nothing was ever acknowledged, so there's no durability promise to honor (consistent with NFR5, which only covers *acknowledged* submissions).
- **Capacity unit**: per task-execution slot (`total_capacity - tasks_in_flight`), not per-worker-count, since workers may support different concurrency levels.
- **Signal maintained as a sharded counter, not a single row and not a live query.** Reasoning, in order of how it developed:
  1. A single global counter row updated on every claim (decrement) and completion (increment) would be a hot-row bottleneck at the ~42,400-211,800/sec claim/complete write rate - same failure shape as the original `SKIP LOCKED` hot-row lesson, recognized by the engineer before it was built.
  2. A live indexed `COUNT` query (reading only, at the much-lower DAG-submission rate of ~1,157-5,785/sec) was considered as a simpler alternative requiring no maintained counter at all - but rejected: its cost scales with the number of `PENDING` rows, meaning it gets *slower* exactly when backlog is large, which is precisely when admission control matters most and needs to stay fast (a stress-amplifying feedback loop). A sharded counter's read cost (`SUM` over a small fixed number of shard rows) stays constant regardless of backlog size. Also an explicit, named learning goal (sharding, in depth) independent of the technical argument.
  3. **Design**: N counter shard rows (starting estimate 16-32, to be benchmarked and tuned, not fixed blind). Each claim decrements a randomly- or hash-selected shard; each completion increments a randomly- or hash-selected shard - confirmed no requirement to route a completion back to "its" shard, since only the aggregate sum matters, not per-shard correctness.
  4. **Read must be a single `SELECT SUM(available_slots) FROM counter_shards` statement**, one query against one Postgres snapshot - not N separate per-shard reads summed in application code, which could observe a torn/inconsistent view since there'd be no shared snapshot across separate round-trips.
  Exact shard count, hashing scheme, and safety-margin threshold (reject at exactly zero vs. some buffer above zero) are LLD-level detail, not decided here.

## Failure modes - decided (2026-07-30)

- **Poison task**: max 10 attempts, exponential backoff between attempts (2, 4, 8, 16, 32s), then moved to a dead-letter table and the workflow's queryable status reflects the failure (ties to FR5/FR7). Retries apply only to failures classified as retryable (transient: timeout, downstream 5xx) - failures classified as permanent (validation errors, downstream 4xx-style rejections) skip straight to dead-letter after the first attempt rather than burning all 10, since a permanent failure will not change outcome on retry. Exact retryable/permanent classification mechanism is LLD-level detail.
- **Thundering herd**: distinguished two sub-cases. Dependency fan-out (many tasks becoming claimable at once) needs no new mitigation - already absorbed by `SKIP LOCKED`'s design (many claimers, no blocking). Mass reconnection (engine restart, worker fleet reconnecting, reaper finding a large stranded batch) gets three combined mitigations: jitter on every timer (reconnect backoff, heartbeat intervals, `Retry-After` values), batched/staggered reaper recovery instead of one giant requeue burst, and a bulkhead/rate-limit on the engine's poll/claim surface.
- **Cascading failure**: four combined mitigations - backoff+jitter applied to every retry loop, not just task-level retries (claim polling, gRPC transport per the PRD's own stated requirement); a circuit breaker on calls to Postgres and any downstream dependency (fail fast after N consecutive failures, cooldown, periodic recovery probe); bounded connection pools (already a standing `CLAUDE.md` rule - "no unbounded thread pools" - reframed here as also being a cascading-failure brake, not just resource hygiene); and the admission-control/backpressure mechanism already designed, which is itself a cascading-failure defense applied at the submission boundary.
- **Not formally written up, deferred as lower priority**: split-brain (largely designed away already via leaderless engine + fencing, likely just needs a short "why this can't happen" note rather than a mitigation) and clock skew (heartbeat/lease timing assumes reasonably synchronized clocks across processes).

## Redis dispatch-hint index and scheduled workflows (2026-07-31)

Resolves discussion-order item 12 (`SKIP LOCKED` behavior fanned out across Citus shards) by changing the shape of the claim query rather than answering the original question directly - see note at the end of this section.

**Problem**: the Claim Broker's query ("give me any N pending tasks, anywhere") has no `workflow_id` filter, so under Citus it is unavoidably a multi-shard distributed query - fanned out to every shard, merged at the coordinator, only as fast as the slowest shard. At 100M-DAGs/day shard counts, this risks both cost (every claim call touching every shard) and NFR3's 500ms p99 dispatch latency budget.

**Idea considered and rejected: cache actual claimed tasks in Redis.** Fails the test "when this cache is wrong, does it produce a wrong answer or just a slow one" - a stale or crash-lost cache entry here could cause a duplicate claim (two workers believing they hold the same task), the exact bug `SKIP LOCKED` exists to prevent. Also creates a second, informal source of truth for claim state, which conflicts with the CP position in HLD-001 Section 3 (Postgres alone decides task state).

**Accepted: cache `workflow_id` "has runnable work" hints in a Redis Sorted Set, not the tasks themselves.**
- A Sorted Set holds unique members each ranked by a numeric score; here the score is "earliest time this workflow should be checked for work" - `now()` for an immediately-runnable workflow, a future timestamp for a scheduled one. One structure, one query (`ZRANGEBYSCORE ... 0 now`) covers both immediate and scheduled workflows.
- The Claim Broker picks a due `workflow_id` from this set, then runs a Postgres query filtered on that `workflow_id` - a router query, single-shard, cheap, with well-understood `FOR UPDATE SKIP LOCKED` semantics, replacing the expensive multi-shard fan-out.
- Redis is the **primary lookup path**, explicitly not a source of truth - Postgres remains authoritative (corrected from an earlier draft of this idea that used "source of truth" loosely). A wrong or missing entry in the set can only ever cause a delay (workflow not noticed yet) or a wasted cheap router query (workflow noticed but has nothing pending) - never an incorrect claim, since the actual claim is still gated by Postgres's guarded update.
- If a workflow has fewer pending tasks than a worker's requested batch size, the Claim Broker tops up by pulling additional `workflow_id`s from the set - bounded by an explicit max-tries-per-poll limit (not yet numbered), consistent with the project's standing "no unbounded work per request" rule.

**Sync mechanism**: a fast, best-effort direct write to Redis is attempted immediately after the relevant Postgres transaction commits (task flips to `PENDING`, or a scheduled workflow is submitted) - never before or concurrently, which is what prevents Redis from ever pointing at a workflow Postgres doesn't know about. If that direct write fails, the outbox relay already built for the Cassandra history feed (Section 4.5 of the HLD) is extended with a second consumer that also writes into Redis, reusing the same `SKIP LOCKED` drain and `relayed_at`-style bookkeeping instead of a bespoke reconciliation query. This closes the gap without needing a saga: nothing Redis-side ever needs to be undone, only retried, because Redis holds no data that is itself authoritative.

**Scope addition: scheduled/delayed workflow start.** Not previously in FR1-FR7. A workflow may specify a future trigger time; its first (dependency-free) tasks become claimable no earlier than that time, using the same Sorted Set described above. Engineer accepted that scheduled start is a target, not a guarantee to the millisecond - proposed default: within a few seconds of the target time at p99, exact number not yet confirmed (see Open Questions).

**Redis availability**: run Redis replicated (not a single instance) as the first line of defense. Second line: a circuit breaker on the engine's calls to Redis (fail fast after N consecutive failures/timeouts, per the existing cascading-failure defenses in Section 8.3 of the HLD), falling back to a plain round-robin-over-shards claim path - slower, but still correct, and still cheap per-query since each shard hit in that fallback is itself a router-shaped query. Keeps NFR1's CP guarantee intact: Redis being down degrades latency, never correctness.

**Consequence for discussion-order item 12** (`SKIP LOCKED` under Citus fan-out): the original open question assumed the claim query would remain a global, unfiltered, multi-shard query. With the workflow_id hint index in front of it, the claim query becomes single-shard/router-shaped in the common case, and the original heavy question (does `FOR UPDATE SKIP LOCKED` work correctly as a genuine multi-shard distributed query) mostly stops being load-bearing for the hot path. A narrower verification still remains open: confirm `FOR UPDATE SKIP LOCKED` behaves correctly on a Citus **router** query (single shard, filtered on `workflow_id`) - lower risk than the original question, but still unverified, still needed before LLD-001 treats it as settled. The round-robin-over-shards fallback path still depends on the original, harder question, since it has no `workflow_id` filter to route by - worth deciding whether that fallback should instead round-robin by picking a random known `workflow_id` per shard (keeping every query router-shaped) rather than truly querying "all pending tasks on shard N" unfiltered.

## Citus distribution key confirmed: `workflow_id` (2026-07-31)

**Decision**: `workflow_id` is confirmed as the Citus distribution key for `workflows`, `tasks`, `task_dependencies`, and the `outbox` table - the tables that are naturally "about one workflow." Colocating a workflow's own rows on one shard is what keeps the dependency-flip check single-shard.

**Foundational rule confirmed, not just assumed**: a task dependency edge can never cross a workflow boundary - every workflow is a fully self-contained DAG. Without this rule, colocation by `workflow_id` would not guarantee a dependency-flip check stays on one shard.

**Grounded against real precedent (2026-07-31)**, matching this project's own practice of checking design choices against Temporal/Cadence rather than asserting them: neither system has a raw dependency edge spanning two independent workflow executions in its base model either. A Temporal Workflow Execution is the fundamental unit, with its own independent Workflow ID and event history (bounded at 50K events); cross-workflow coordination happens only through explicit, structured constructs layered on top - **Child Workflows** (a workflow started from within another, with its own separate ID and history, which the parent can await synchronously or asynchronously) and **Signals** (asynchronous message-passing into a running workflow, including from outside via an external-workflow stub, without an implicit dependency edge). Cadence's model is the same shape: child workflows return results to the parent, and signals (including to workflows the caller did not itself start) are the mechanism for cross-execution communication. In both systems, "one workflow's outcome affects another" is always a deliberate, structured feature, never an implicit graph edge crossing execution boundaries.

**Consequence for Sentinel Engine**: if a future need arises for one workflow to depend on or react to another, the precedent points toward adding a structured mechanism analogous to Child Workflows or Signals, not relaxing the "DAG edges never cross workflow boundaries" rule that the sharding strategy depends on. Not designed now; noted as a future extension point if the need arises, not a current requirement.

**Not yet resolved**: whether the sharded admission counters and the worker heartbeat/capacity table should also be distributed by `workflow_id`, or whether they fit Citus's reference-table concept (a small table replicated to every node) better, since neither is naturally organized around a single workflow. Deferred to LLD-001 schema work, not decided here.

## Discussion order (one topic at a time, per engineer's request)

Going forward, topics are discussed and resolved one at a time rather than all at once.
Current order:

1. ~~Engine HA coordination mechanism~~ - resolved, advisory-lock leader election (see Decisions above). Sub-question on global-vs-sharded leadership still open within that topic.
2. ~~Idempotency necessity question~~ - resolved (see Decisions above: at-least-once execution, idempotency key mechanism, claim vs. execution idempotency distinction).
3. ~~Backpressure policy~~ - resolved (see Backpressure policy section above: reject outright, sharded counter).
4. ~~Failure-mode formal write-up~~ - resolved for the 3 modes tackled (poison task, thundering herd, cascading failure - see Failure modes section below). Remaining modes from the original menu (split-brain, clock skew) not formally written up - deferred, lower priority.
5. DAG storage (Postgres-only vs. Postgres + MongoDB) - parked.
6. Client-facing vs. operator-facing log delivery - parked.
7. `CLAUDE.md` Non-Goals reconciliation - parked.
8. Entities/schema sketch - deferred to LLD-001.
9. Task cancellation (push signal to a running worker) - parked, raised 2026-07-30.
10. Ingestion-service vs. scheduler-service split - parked, raised 2026-07-30 (not structurally required, would be a deliberate choice).
11. Global-vs-sharded advisory-lock leadership at 100M-DAGs/day scale - parked sub-question within item 1.
12. ~~`SELECT ... FOR UPDATE SKIP LOCKED` behavior when fanned out across Citus shards~~ - narrowed, see "Redis dispatch-hint index and scheduled workflows" section above. Explicitly backlogged by the engineer (2026-07-31), not a blocker for starting LLD-001.
13. ~~Shard/distribution key for Citus~~ - confirmed 2026-07-31, see "Citus distribution key confirmed" section below.
14. Exact scheduling-accuracy tolerance for delayed/scheduled workflow start - backlogged (2026-07-31), engineer indicated a few seconds of drift is acceptable, exact number deferred.
15. Max `workflow_id` tries per claim poll when topping up a short batch from the Redis hint index - backlogged (2026-07-31), not yet numbered.

**Note (2026-07-31)**: items 12, 14, and 15 above, plus the whole "Deliberately parked" list (DAG storage/SAGA exercise, log delivery split, `CLAUDE.md` Non-Goals reconciliation, task cancellation, ingestion/scheduler split, global-vs-sharded leadership, clock skew, splitwise-app cross-project scoping) are confirmed by the engineer as backlog - not immediate, not blocking the move to LLD-001. Revisit opportunistically, not on a forced schedule.

## Open questions / not yet resolved (detail)

1. **DAG storage: Postgres-only vs. Postgres + MongoDB.** Two framings on the table:
   - Deliberately treat "submit workflow" as a SAGA exercise: DAG doc in Mongo, workflow row in Postgres, explicit compensating action if the second write fails. If chosen, the ADR reason is "SAGA-pattern practice," not "flexible schema."
   - Keep the DAG in Postgres (JSONB or normalized tables) for now, and introduce the Mongo+SAGA exercise as a later, separate milestone once the core claim/schedule loop is proven correct.
   Not decided; engineer to choose sequencing.
2. **Client-facing log delivery vs. operator-facing log aggregation** are two different requirements that got merged in discussion:
   - Operator-facing: centralized, searchable, cross-workflow structured logs (NFR6, the "log aggregation pattern," ELK/Loki-style).
   - Client-facing: a link to a single workflow's execution log/output, likely S3-backed, with its own retention and per-tenant access control.
   Not yet decided whether both are in scope for this phase or just one.
3. **`CLAUDE.md` Non-Goals reconciliation** (see Scope pivot above) - still open.
4. Failure-mode formal write-up (split-brain, thundering herd, mass lease expiry, slow-consumer backpressure, etc.) - explicitly deferred by the engineer to a later pass, not dropped.
5. Entities/schema sketch - explicitly deferred to LLD-001 by the engineer.
6. Task cancellation - the engine must push a stop signal to a worker mid-execution; not solved by the pull-claim model. Not yet designed.
7. Ingestion-service vs. scheduler-service split - not structurally required (neither role needs to be a leader), so this is a deliberate-choice question, not yet answered.

## Rejected / walked back

- **Kafka as the primary task queue** - rejected as the *production* claim mechanism (conflicts with the project's Postgres-as-coordinator direction and doesn't by itself satisfy FR3 under consumer-group rebalances); retained only as a comparative-learning exercise, documented separately from the actual scheduling path.
- **ZooKeeper for worker-liveness "global config"** - rejected as originally proposed (push-based failure detection creates a split-brain risk: a worker can appear dead to ZK while still alive and mid-side-effect). Superseded by the lease/fencing-token model above.
- **Check-then-act idempotency table lookup** - rejected as proposed (TOCTOU race between two workers); the underlying idea (track claim state in Postgres) survives, but must be one atomic conditional statement, not a separate read then write.
- **Application-layer WAL shipping / leader-follower for the engine** - rejected as proposed; it re-describes Postgres's own streaming replication applied to the wrong layer. Superseded by the two candidate application-coordination approaches above.
