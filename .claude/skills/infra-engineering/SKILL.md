---
name: infra-engineering
description: Infrastructure and operations mentor for Sentinel Engine. Use for Docker Compose, CI pipelines, configuration management, environment parity, capacity, and operational readiness reviews. Exact solutions are allowed here for setup and tooling failures.
---

# Infra Engineering Mentor

This is the one area where exact solutions are given freely: fighting Docker, Gradle, or CI is not the learning goal.

## Local Environment

- One command brings up everything: `docker compose up` starts Postgres (and later Jaeger, Prometheus, Grafana) with pinned image versions.
- Dev, CI, and (imaginary) prod use the same Postgres major version; locking semantics are version-sensitive.
- Configuration via environment variables with a checked-in `.env.example`; secrets never committed.
- Healthchecks in compose so dependent services wait properly instead of crash-looping.

## CI Discipline

- CI runs on every push: compile, unit tests, integration tests (Testcontainers), lint.
- The build is the quality gate: a red build blocks everything, including unrelated work, per project rules.
- CI must be reproducible from a clean clone; "works on my machine" bugs are infra bugs and get fixed immediately.
- Cache Gradle and Docker layers deliberately; slow CI erodes discipline.

## Configuration Principles

- Every tunable that shapes behavior (pool sizes, lease duration, heartbeat interval, batch sizes) is external configuration with a documented default and unit.
- Fail fast on invalid config at startup, never at first use.
- Timeouts are configuration, and every remote call has one.

## Operational Readiness Review

Before a phase is called done, answer:
- How do I know it is healthy (liveness vs readiness, and what each actually checks).
- How do I deploy a schema change without downtime (expand-migrate-contract).
- What happens on `SIGTERM`: graceful shutdown order, in-flight task policy, drain timeout.
- What is the blast radius when Postgres is down: which operations refuse, which queue up, what the operator sees.
- Capacity: connection budget per component, since Postgres connections are the scarcest resource in this architecture.
