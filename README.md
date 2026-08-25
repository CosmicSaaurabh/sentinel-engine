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

- `proto/`: the gRPC wire contract (`sentinel.v1`). Depends on nothing else in the repo.
- `worker-sdk/`: the Java client library for running task handlers. Plain Java, no Spring. See its [README](worker-sdk/README.md).
- `engine/`: the orchestrator. Spring Boot, Postgres, gRPC server.
- `samples/`: a runnable demonstration. Depends on the SDK and the contract, never on the engine.
- `docs/`: PRDs, high-level designs, low-level designs, ADRs, task breakdown, and the learning log.
- `docs/tasks/mvp-task-breakdown.md`: the phased plan with definitions of done and edge cases.
- `CLAUDE.md`: rules of engagement for the AI architect/reviewer guiding this project.

The dependency direction is deliberate and enforced by the build: the contract depends on nothing, the SDK depends only on the contract, and a client application never links against the server.

## Development Process

Every feature passes three gates, each tracked as a GitHub issue and closed in order: high-level design, low-level design, then implementation.
Designs are documented with trade-offs and rejected alternatives before code is written.
Concurrency claims are proven by stress tests, never asserted in comments.

## Local Setup

1. Start Postgres: `docker compose -f infra/docker-compose.yml up -d`
2. Build everything: `./mvnw verify`
3. Run the engine: `./mvnw -pl engine spring-boot:run`
4. Confirm it is up: `curl localhost:8080/actuator/health`

The engine listens on two ports: `8080` for health and Prometheus metrics, `9090` for the gRPC worker protocol.
They are separate because they carry very different traffic, and one set of timeouts should not govern both.

The test suite needs a Docker daemon for Testcontainers; `./mvnw verify` starts and stops the containers itself.

## Run the Demo

With Postgres and the engine running:

```bash
./mvnw -pl samples -am exec:java -Dexec.mainClass=dev.sentinel.samples.OrderFulfilmentDemo
```

It submits a diamond-shaped order-fulfilment workflow, starts a worker that executes it, and prints the outcome:

```
                 reserve-stock
                /             \
      charge-card             notify-warehouse
                \             /
                  send-receipt
```

`charge-card` fails on its first attempt deliberately, so a run shows the retry path rather than only the happy one.
The two middle steps have no dependency on each other and run concurrently; `send-receipt` waits for both.

## Writing a Worker

See the [Worker SDK guide](worker-sdk/README.md) for the handler contract, the threading model, idempotency, failure classification, and shutdown semantics.

The short version:

```java
try (SentinelWorker worker = SentinelWorker.builder()
        .engineTarget("localhost:9090")
        .concurrency(8)
        .register("billing.charge", context -> {
            paymentApi.charge(context.input(), context.taskId());   // taskId is the idempotency key
            return TaskResult.completed("{\"charged\":true}");
        })
        .build()) {
    worker.start();
    Thread.currentThread().join();
}
```
