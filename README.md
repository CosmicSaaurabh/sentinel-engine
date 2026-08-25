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

## Observability

The engine exposes health and Prometheus metrics on `8080` and the worker protocol on `9090`.

```bash
curl localhost:8080/actuator/health
curl localhost:8080/actuator/prometheus | grep sentinel_
```

A Grafana dashboard is committed at [`infra/grafana/sentinel-engine-dashboard.json`](infra/grafana/sentinel-engine-dashboard.json), organised around three questions rather than around metric names: is work flowing, is work succeeding, and is the fleet healthy.

Three panels are worth knowing about before you need them:

- **Claimable versus pending.** A gap between them means tasks are waiting out a retry backoff rather than waiting for a worker. A queue that looks deep while nothing is claimable is a retry storm, and adding workers to it will not help.
- **Expired leases awaiting recovery.** The reaper's own backlog. Every reaper pass looks healthy while this climbs, so it is the only signal that recovery is losing ground. It should sit near zero.
- **Heartbeats renewed versus leases lost.** Lost leases are workers discovering their work was taken away. A steady trickle under load usually means the heartbeat interval is too close to the lease duration.

Logs are human-readable on the console and ECS-structured in `logs/sentinel-engine.log`, so a developer watching a terminal and a log aggregator indexing fields both get what they need. Every line carries `workflowId`, `taskId` and the trace id when there is one.

Tracing samples everything for the MVP, which is a deliberate choice rather than a default: a partly sampled trace is close to useless for following one task through submission, claim, execution and completion, and the trace an operator wants is never the one head sampling happened to keep. Tail sampling in a collector is the documented next step.

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
