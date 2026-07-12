# Sentinel Engine

A high-performance, fault-tolerant distributed workflow orchestration engine built from scratch in Java.
Sentinel is a lightweight durable execution engine (in the spirit of a streamlined Temporal or Cadence) that runs multi-step workflow DAGs with effectively-once semantics, surviving worker and network failures.

This is a deliberate learning project built entirely by hand to master production-grade systems design, Java concurrency, and distributed state management.

## Architecture at a Glance

- Orchestrator: a Spring Boot engine that parses task DAGs and drives a deterministic state machine (`PENDING`, `RUNNING`, `COMPLETED`, `FAILED`).
- Durable queue: PostgreSQL as the single source of truth, with `SELECT ... FOR UPDATE SKIP LOCKED` powering a high-concurrency task queue.
- Transport: gRPC between engine and workers, with heartbeats, retry/backoff with jitter, and lease-based recovery.
- Resilience: dead worker detection via missed heartbeats, fenced re-queue of stalled tasks, retries with exponential backoff, dead-letter handling.
- Observability: OpenTelemetry traces across processes, Prometheus metrics, structured logs.

## Repository Layout

- `docs/`: PRDs, high-level designs, low-level designs, ADRs, task breakdown, and the learning log.
- `docs/tasks/mvp-task-breakdown.md`: the phased plan with definitions of done and edge cases.
- `CLAUDE.md`: rules of engagement for the AI architect/reviewer guiding this project.

## Development Process

Every feature passes three gates, each tracked as a GitHub issue and closed in order: high-level design, low-level design, then implementation.
Designs are documented with trade-offs and rejected alternatives before code is written.
Concurrency claims are proven by stress tests, never asserted in comments.
