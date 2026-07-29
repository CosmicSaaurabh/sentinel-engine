# Sentinel Engine MVP Task Breakdown

Phased, bite-sized tasks in learning order.
Each task lists a Definition of Done (DoD) and the architectural edge cases to watch while writing the code.
Every phase follows the gate order: HLD issue, then LLD issue, then feature issues.

Legend: `[HLD]` design discussion, `[LLD]` detailed design, `[FEAT]` implementation.

---

## Phase 0: Foundation

### T0.1 [FEAT] Project scaffolding

Set up the repo skeleton: Maven multi-module build (`engine`, `worker-sdk`, `proto`), Spring Boot 4.x on Java 21, Docker Compose with PostgreSQL 18, Flyway wired in, Testcontainers smoke test, and a GitHub Actions CI running build plus tests.

DoD:
- `./mvnw verify` passes locally and in CI from a clean clone.
- `docker compose up` brings up Postgres; the app connects and runs an empty Flyway migration.
- One Testcontainers integration test proves the app context boots against real Postgres.
- README documents the one-command local setup.

Edge cases to watch:
- Connection pool (Hikari) sizing left at defaults will mask concurrency bugs later; set explicit small limits so contention shows up early.
- CI and local Postgres versions must match; a version drift here invalidates locking behavior tests later.
- Flyway migration checksum drift when editing an applied migration; learn the discipline of never editing applied migrations now.

---

## Phase 1: DB & Queue (the heart of the engine)

### T1.1 [HLD] HLD-001: System architecture of the Sentinel Engine

Interview-style design of the whole system: components (engine, Postgres, workers, SDK), data flow for submit/schedule/claim/complete, CAP position, and throughput/latency targets.

DoD:
- `docs/high-level-design/HLD-001-system-architecture.md` exists with component diagram, sequence flow, trade-offs, rejected alternatives, and at least two failure modes.
- Explicit CAP decision recorded with justification.
- Numbers: target transitions/sec, dispatch p99, recovery bound.

Edge cases to watch:
- Who owns the scheduler loop when we later run two engine instances.
- What "submit accepted" means durably (transaction boundary of the submit API).

### T1.2 [LLD] LLD-001: Schema and Postgres-backed task queue

Detailed design of the relational schema (`workflows`, `tasks`, `task_dependencies`, transition history), state machine transitions as guarded SQL, and the claim query built on `FOR UPDATE SKIP LOCKED`, including the exact indexing strategy that keeps claims fast under high concurrency.

DoD:
- `docs/low-level-design/LLD-001-schema-and-queue.md` with full DDL, index rationale, the claim query, and its expected `EXPLAIN` shape.
- Documented analysis: why `SKIP LOCKED` beats advisory locks and beats optimistic version bumping for this queue, with rejected alternatives recorded.
- Decision recorded on transition history table (audit) vs status column only.

Edge cases to watch:
- A partial index on `status = 'PENDING'` rots if status values evolve; document index maintenance implications.
- Hot single index page under high claim concurrency (btree contention); consider claim batching.
- `VACUUM` pressure from high-churn status updates; HOT updates require the status column to avoid indexed-column rewrites where possible.

### T1.3 [FEAT] Workflow and task persistence layer

Implement Flyway migrations for the approved schema, entities or records, and repositories with explicit SQL for all state transitions.

DoD:
- Migrations apply cleanly on empty and existing databases.
- Every state transition is a guarded update (`UPDATE ... SET status = ? WHERE id = ? AND status = ?`) and the code asserts affected-row count.
- Integration tests cover every legal and illegal transition.

Edge cases to watch:
- Illegal transition attempts (e.g. `COMPLETED -> RUNNING`) must fail loudly, not silently update zero rows.
- Timestamps must come from the database clock, not JVM clocks, or drift breaks lease math later.
- Enum-to-string mapping mismatches between Java and the DB check constraint.

### T1.4 [FEAT] Atomic task claim with SKIP LOCKED

Implement the claim operation: fetch and lock the next N runnable tasks, mark them `RUNNING` with a claim owner and lease deadline, in one transaction.

DoD:
- Claim is a single transaction; crash at any point either claims fully or not at all.
- A concurrency test with at least 50 parallel claimers over 100k tasks proves zero duplicate claims and zero lost tasks.
- Measured claims/sec documented in the PR.

Edge cases to watch:
- Long-running claim transactions hold locks and starve `VACUUM`; keep the claim transaction tiny and never execute task work inside it.
- `LIMIT` plus `ORDER BY` plus `SKIP LOCKED` can under-deliver when contention is high; that is correct behavior, but callers must handle empty results without hot-spinning.
- Connection pool exhaustion when claimers outnumber pool size; a claim should fail fast, not queue forever.

### T1.5 [FEAT] DAG parser and workflow state machine

Implement DAG validation (cycle detection, unknown dependency detection) at submit time and the scheduler transition that flips a task to `PENDING` (runnable) only when all its dependencies are `COMPLETED`.

DoD:
- Cyclic and malformed DAGs are rejected at submission with precise errors.
- Dependency satisfaction is computed in SQL or one query round-trip, not by loading the whole graph into memory per tick.
- A diamond DAG (A -> B, A -> C, B and C -> D) integration test passes, including the failure branch (B fails, D never runs, workflow fails deterministically).

Edge cases to watch:
- Two tasks completing simultaneously must not race on marking the common child runnable twice.
- A workflow with zero tasks, a single task, and a disconnected node are all legal-input questions; decide and test each.
- Fan-in of hundreds of dependencies makes the naive "count completed parents" query hot; know its complexity.

---

## Phase 2: gRPC & SDK

### T2.1 [HLD] HLD-002: gRPC transport and worker protocol

Design the wire protocol: polling vs long-poll vs bidirectional streaming, message contracts, heartbeat channel, backpressure, and versioning strategy for the proto contract.

DoD:
- `docs/high-level-design/HLD-002-grpc-transport.md` with the chosen interaction model, rejected models, and the reasoning.
- Explicit deadline/timeout policy for every RPC.
- At least two failure modes documented (e.g. half-open connections, stampede after engine restart).

Edge cases to watch:
- Bidirectional streaming keeps state in the engine per worker; polling keeps the engine stateless; this decision shapes everything in Phase 3.
- gRPC deadlines vs application-level lease timeouts must not fight each other.

### T2.2 [LLD] LLD-002: Worker SDK design

Class-level design of the Java worker SDK: task handler registration API, poll loop lifecycle, thread pool ownership, retry/backoff with jitter, heartbeat scheduling, and graceful shutdown semantics.

DoD:
- `docs/low-level-design/LLD-002-worker-sdk.md` with class diagram, threading model (which thread runs what), and shutdown sequence.
- The public SDK API surface is minimal and documented; a user registers handlers and calls start/stop.
- Backoff strategy specified with formula and jitter type, plus cap.

Edge cases to watch:
- Handler code that blocks forever must not wedge the poll loop or block shutdown.
- Uncaught exceptions in handler threads must fail the task, not kill the thread silently.
- Shutdown while a task is mid-flight: decide between finish-and-ack vs abandon-and-let-lease-expire.

### T2.3 [FEAT] gRPC contract and engine-side services

Write the proto files and implement the engine-side gRPC services: claim tasks, report completion/failure, heartbeat, submit workflow, query status.

DoD:
- Proto contract reviewed and committed under `proto/` with versioned package naming.
- Engine services enforce authorization of ownership: only the claiming worker can complete or fail its task.
- Integration test drives a full workflow lifecycle over real gRPC (Testcontainers Postgres plus in-process gRPC server).

Edge cases to watch:
- A completion report arriving after the lease expired and the task was re-queued (zombie worker) must be rejected deterministically; this is the fencing token lesson.
- Duplicate completion reports (network retry) must be idempotent.
- Payload size limits; gRPC default 4 MB message cap and what you do at the boundary.

### T2.4 [FEAT] Worker SDK implementation

Implement the SDK per LLD-002: poll loop, handler dispatch on an owned executor, heartbeats, retry/backoff with jitter on transport errors, graceful shutdown.

DoD:
- A sample worker app runs a demo workflow end to end.
- Kill -9 on the worker mid-task leaves the system consistent (task recovered later by Phase 3 machinery or visibly stuck pending it).
- SDK has zero Spring dependency; plain Java library.
- Concurrency test: handler exceptions, slow handlers, and parallel tasks on one worker all behave per design.

Edge cases to watch:
- Thread pool sizing: unbounded executors hide backpressure; bounded ones need a rejection policy that does not lose the claimed task.
- Heartbeat thread starvation when handler threads saturate the CPU; heartbeats need a dedicated scheduler.
- Retry storms: all workers backing off and retrying in sync after an engine blip (thundering herd); jitter is mandatory.

---

## Phase 3: Resilience & Observability

### T3.1 [HLD] HLD-003: Failure detection and recovery

Design the lease and reaper architecture: heartbeat intervals, lease durations, who reaps (single engine vs multiple), and how re-queue avoids duplicate side effects.

DoD:
- `docs/high-level-design/HLD-003-failure-recovery.md` with the lease state diagram, reaper election choice (e.g. Postgres advisory lock), and recovery time budget matching NFR4.
- The "at-least-once execution, exactly-once transition" contract written down explicitly, including what the engine can and cannot guarantee about side effects.
- Two failure modes documented (e.g. reaper split-brain, mass lease expiry after engine pause).

Edge cases to watch:
- GC pause or laptop sleep makes a healthy worker look dead; the fencing mechanism from T2.3 must make its late writes harmless.
- Clock skew between engine instances; leases must be computed against the database clock only.

### T3.2 [LLD] LLD-003: Retry policy, reaper, and idempotency design

Class and schema design for retry counters, backoff schedule persistence, dead-letter state, the reaper query, and idempotency keys for task execution.

DoD:
- `docs/low-level-design/LLD-003-retry-reaper-idempotency.md` with schema deltas, reaper SQL, retry state machine, and class diagram.
- Max-attempts and backoff schedule are per-task-type configurable with sane defaults.
- Rejected alternatives recorded (e.g. delayed queue table vs `next_attempt_at` column).

Edge cases to watch:
- Retry backoff stored as absolute `next_attempt_at` interacts with the claim query index; the partial index condition must include it.
- A task that fails instantly in a tight loop must not saturate the transition-history table.

### T3.3 [FEAT] Heartbeat, reaper, and safe re-queue

Implement worker heartbeats extending leases, and the reaper that expires leases and re-queues tasks with incremented attempt count and a new fencing token.

DoD:
- Kill -9 a worker mid-task; the task re-queues within 3 heartbeat intervals and completes on another worker (demo plus automated test).
- Zombie test: the killed worker's process is resumed (SIGSTOP/SIGCONT) and its late completion is rejected by fencing.
- Reaper is safe to run on multiple engine instances simultaneously without double re-queue.

Edge cases to watch:
- Reaping and a live completion racing on the same row; both paths must be guarded updates on the fencing token.
- Mass expiry (100 workers die at once) must not create a re-queue stampede that locks the table; batch and pace the reaper.

### T3.4 [FEAT] Retry, backoff, and dead-letter handling

Implement per-task retry with exponential backoff plus jitter, terminal `FAILED` after max attempts, and a queryable dead-letter view with the failure cause chain.

DoD:
- Failure cause (exception class, message, worker id, attempt) is persisted per attempt.
- Backoff verified by test: attempt timestamps match the schedule within tolerance.
- Workflow-level outcome on task exhaustion follows the FR5 rule deterministically.

Edge cases to watch:
- Distinguish retryable failures (network, timeout) from permanent ones (validation); retrying a permanent failure wastes the budget and delays the terminal state.
- Poison-pill payload that crashes every worker it lands on; max attempts must bound the blast radius.

### T3.5 [FEAT] Observability: traces, metrics, structured logs

Integrate OpenTelemetry: trace context injected at submit, propagated through gRPC metadata via interceptors, spans for schedule/claim/execute/complete.
Expose Prometheus metrics and build a minimal Grafana dashboard.

DoD:
- One trace in Jaeger (or OTLP viewer) shows the full life of a task across engine and worker processes.
- Metrics exposed: queue depth by status, claim rate, task latency histogram, reaper actions, heartbeat misses.
- Grafana dashboard JSON committed under `infra/`.
- Logs are structured (JSON) and every log line inside a task execution carries workflow id, task id, and trace id.

Edge cases to watch:
- Context propagation across the SDK's executor thread hop; OTel context does not follow threads automatically.
- High-cardinality metric labels (task id as a label) will melt Prometheus; labels must be bounded (task type, not task id).
- Sampling: 100% tracing at target load may be fine for MVP, but write down the decision.

---

## Learning Checkpoints

At the end of each phase, update `docs/learning-log.md` with:
- The three most surprising things learned.
- One production incident this phase's work would have prevented.
- One thing that would be designed differently at 100x scale.
