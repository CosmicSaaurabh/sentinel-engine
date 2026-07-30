# PRD-001: Sentinel Engine MVP

- Status: Accepted
- Author: Saurabh Mishra
- Reviewer: Principal Architect (Claude)
- Date: 2026-07-13

## 1. Problem Statement

Teams running multi-step business processes (payments, provisioning, data pipelines) need those steps to complete reliably even when a machine dies mid-step.
Ad-hoc solutions (cron plus retries plus manual cleanup) lose state, double-execute side effects, and are impossible to observe.
Heavyweight engines like Temporal solve this but hide the mechanics we want to master.

Sentinel Engine is a lightweight, developer-focused durable execution engine.
A developer defines a workflow as a DAG of tasks, submits it, and the engine guarantees every task runs to a terminal state with effectively-once semantics, surviving worker crashes and network failures.

## 2. Goals

- Deterministic orchestration of task DAGs with explicit states: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`.
- Durable state in PostgreSQL; a restart of any component never loses accepted work.
- A horizontally scalable worker fleet pulling tasks over gRPC.
- Automatic recovery of tasks stranded by dead workers, without duplicate side effects.
- First-class observability: distributed traces across engine and workers, Prometheus metrics, structured logs.

## 3. Non-Goals (MVP)

- No custom consensus protocol; PostgreSQL is the coordinator.
- No web dashboard.
- No event-sourcing replay engine.
- No non-Java SDKs.
- No cross-region deployment story.

## 4. Users and Use Cases

- Backend developer: defines a workflow DAG in Java, submits it via SDK, observes progress via logs and metrics.
- Platform operator: runs the engine and Postgres, scales workers up and down, gets alerted on stuck workflows.

Representative workflow: order processing with tasks `reserve-inventory -> charge-payment -> send-confirmation`, where `charge-payment` must never execute twice.

## 5. Functional Requirements

- FR1: Submit a workflow definition (DAG of named tasks with dependencies) and receive a workflow id.
- FR2: The engine schedules a task only when all its dependencies are `COMPLETED`.
- FR3: Workers claim tasks such that no task is ever claimed by two live workers at once.
- FR4: Task failures trigger configurable retries with exponential backoff, then mark the task `FAILED`.
- FR5: A `FAILED` task fails the workflow branch deterministically.
- FR6: Tasks held by a worker that stops heartbeating are re-queued within a bounded time.
- FR7: Workflow and task status are queryable at any time.

## 6. Non-Functional Requirements

- NFR1 (consistency): the engine chooses consistency over availability for state transitions (CP behavior); if Postgres is unreachable, the engine refuses work rather than guessing.
- NFR2 (throughput target): derived from a 100M-DAGs/day design target, modeled per task type rather than a single blended average, since task duration is bimodal (most tasks fast, a minority long-running) and a blended mean misrepresents both ends.
  Model: ~10 tasks/DAG, 9 fast auxiliary tasks/DAG (each ~3 lifecycle writes: schedulable-flip, claim, complete; duration well under one heartbeat interval, so zero heartbeat writes) plus 1 payment-like task/DAG (90% resolve fast like the auxiliary tasks, 10% hit an ~11-minute hangup path requiring heartbeat-driven lease renewal every 10s, ~66 heartbeats when it occurs).
  Expected mutations/DAG ~36.6 (~3.66/task average) -> **~42,400 mutations/sec sustained average, ~211,800 mutations/sec at a 5x peak multiplier.**
  This is a design target for the overall persistence layer, not a claim that a single Postgres primary sustains it unmodified; horizontal partitioning of the persistence layer is expected to be required and is tracked as an open design decision, not deferred indefinitely.
  Benchmarked/validated numbers on real hardware will necessarily be smaller than this design target; the gap between benchmarked and design numbers must be bridged by a documented, defensible scaling argument (e.g. sharding, wide-column store for history), not asserted without evidence.
  The 90/10 fast/slow split and the 1-in-10 payment-task ratio are estimates, not measured data; revisit once real task-duration telemetry exists.
- NFR3 (latency target): p99 task dispatch latency (dependency satisfied to worker claim) under 500 ms at target throughput.
- NFR4 (recovery target): stranded tasks re-queued within 3 missed heartbeat intervals.
- NFR5 (durability): an acknowledged workflow submission survives crash of any single component.
- NFR6 (observability): every task execution carries a trace spanning submit, schedule, claim, execute, and complete.

## 7. Success Criteria

- A demo workflow of 10 tasks with a diamond-shaped DAG completes correctly while a worker is killed mid-run.
- Concurrency test: 50 workers polling, zero duplicate claims across 100k tasks.
- Load test meets NFR2 and NFR3 with documented numbers.
- Grafana dashboard shows queue depth, claim rate, and task latency histograms.

## 8. Open Questions

- Task payload size limits and whether large payloads live in the row or a side table.
- Whether workflow definitions are versioned in the MVP or fixed at submission.

## 9. Failure Modes to Design Against

- Worker dies after executing a side effect but before acking completion; recovery must not blindly re-execute.
- Postgres failover causes a brief split where two engine instances both think they own reaping duties.
