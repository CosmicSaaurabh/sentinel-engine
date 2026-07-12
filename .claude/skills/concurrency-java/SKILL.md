---
name: concurrency-java
description: Java concurrency mentor for Sentinel Engine. Use when designing or reviewing threading models, executors, locks, memory visibility, backpressure, or tests for concurrent code.
---

# Java Concurrency Mentor

## Core Review Questions

Ask these of every piece of concurrent code before anything else:
- Which thread runs this line, and who created that thread.
- What is shared, what guards it, and is the guard the same object on every access path.
- What is the happens-before edge that makes this read see that write.
- What happens when this blocks forever; what is the timeout and who enforces it.

## Executor Discipline

- Every executor is owned: created in one place, named threads (`sentinel-poll-1`), bounded queue, explicit `RejectedExecutionHandler`, and a shutdown sequence (`shutdown`, await, `shutdownNow`, await, log leftovers).
- No `Executors.newCachedThreadPool` in production paths; unbounded threads hide backpressure until the process dies.
- CPU-bound and IO-bound work never share a pool.
- Virtual threads (Java 21) are appropriate for the SDK's blocking IO paths; pinning via `synchronized` blocks around IO is the gotcha to check.

## Shared State Rules

- Prefer immutability (records, final fields) and confinement (state owned by one thread) over locks.
- When locking: smallest scope possible, never hold a lock across network or database IO, document acquisition order when more than one lock exists.
- `volatile` is for single-writer flags, not compound actions; check-then-act on a volatile is still a race.
- Atomics for counters and CAS loops; explain the ABA risk when a CAS guards more than a counter.

## Failure and Cancellation

- Every `Future.get` has a timeout; every awaited latch has a timeout.
- `InterruptedException` is never swallowed: either propagate or restore the interrupt flag, and the choice is deliberate.
- Uncaught exception handlers on every pool; a silently dead worker thread is the worst bug class in this project.

## Testing Concurrent Code

- Race conditions are proven with stress tests: many threads, a synchronization barrier to maximize collision, and invariant assertions (e.g. no task claimed twice across 100k operations).
- Use deterministic seams: injectable clocks, injectable executors (direct executor in unit tests).
- A flaky concurrent test is a real bug until proven otherwise; never add retries to hide it.
- jcstress exists for memory-model-level questions; Testcontainers stress tests cover the DB claim paths.
