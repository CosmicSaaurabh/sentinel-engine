---
name: observability
description: Observability mentor for Sentinel Engine. Use when designing or reviewing OpenTelemetry tracing, Prometheus metrics, structured logging, dashboards, or SLO definitions.
---

# Observability Mentor

Observability is designed, not sprinkled on. Every feature's LLD names its signals before implementation.

## The Three Signals

- Traces answer "where did this one request spend its time"; one trace must span submit, schedule, claim, execute, complete across processes.
- Metrics answer "how is the system doing in aggregate"; they are cheap, pre-aggregated, and alert-friendly.
- Logs answer "what exactly happened here"; structured JSON, one event per line, correlated by trace id.

## Tracing Rules (OpenTelemetry)

- Context propagates over gRPC via metadata interceptors; that is the engine-worker seam to get right.
- Context does not cross thread hops automatically; every executor handoff in the SDK must wrap tasks with context capture.
- Span names are low-cardinality (`task.execute`, not `task.execute.{id}`); ids go in attributes.
- Record the sampling decision explicitly, even if MVP samples 100%.

## Metrics Rules (Prometheus)

- Label cardinality is a budget: task type is a valid label, task id and workflow id never are.
- Counters for events (tasks_claimed_total), gauges for states (queue_depth), histograms for durations (task_dispatch_seconds); pick deliberately.
- Every histogram's buckets are chosen against the NFR target (dispatch p99 500 ms means buckets bracket 500 ms tightly).
- The four golden signals for each component: latency, traffic, errors, saturation. The queue adds depth and age-of-oldest-item, the two numbers that predict trouble earliest.

## Logging Rules

- Every log line inside task execution carries workflow id, task id, attempt, worker id, trace id (via MDC).
- Log levels mean something: ERROR pages someone, WARN is actionable later, INFO tells the story, DEBUG is for development.
- Never log payloads wholesale; they may be large and sensitive.
- No log-and-rethrow duplication; an error is logged once, at the layer that handles it.

## Review Checklist

- Can I answer "why is this workflow stuck" from signals alone, without a debugger.
- Does every alert-worthy condition have a metric, and does every metric that exists get looked at (delete the rest).
- Dashboard-as-code committed under `infra/`; hand-edited dashboards die with the container.
