# HLD-002: gRPC Transport and Worker Protocol

- Status: proposed
- Date: 2026-08-24
- Governs: GitHub issue #7, and the implementation tasks T2.3 and T2.4
- Builds on: HLD-001 (system architecture), LLD-001 (schema and queue)

## 1. Scope

How a worker talks to the engine: the interaction model, the RPC surface, deadlines, backpressure, and what happens when the connection between them misbehaves.

This document decides one thing that shapes everything in Phase 3: **whether the engine holds per-worker state**.
Everything else here follows from that answer.

## 2. Requirements this transport has to meet

- FR3: a task is never handed to two live workers. Already guaranteed by the claim query; the transport must not undermine it.
- FR6: a task held by a worker that stops heartbeating is re-queued within a bounded time.
- NFR1 (CP): if Postgres is unreachable, the engine refuses work rather than guessing. The transport must surface that as a clear failure, not a hang.
- NFR3: p99 dispatch latency, from a task becoming runnable to a worker holding it, under 500ms.
- The standing rule from `CLAUDE.md`: no unbounded queues, no unbounded thread pools, every timeout explicit.

NFR3 is the one that rules out the simplest option, and it is worth being precise about why.

## 3. The interaction model

### 3.1 The three candidates

**Plain polling.** The worker asks "any work?", the engine answers immediately, the worker sleeps and asks again.
Dead simple, completely stateless, and the reason it fails NFR3 is arithmetic: dispatch latency averages half the poll interval.
A 200ms interval would meet the latency budget and produce, for a thousand idle workers, five thousand queries per second against Postgres asking a question whose answer is almost always "no".
The engine would spend most of its database capacity proving there is nothing to do.

**Bidirectional streaming.** The worker opens a long-lived stream and the engine pushes tasks down it.
Lowest possible latency, and it is what HLD-001 section 4.4 gestured at.
The problem is that it does not actually remove the polling, it relocates it. Postgres has no push channel in this design, so an engine instance holding streams for two hundred workers still has to ask Postgres whether anything is runnable. What changes is who asks, not whether anyone does.
In exchange for that non-improvement it introduces per-worker state in the engine, flow control on every stream, and a new failure mode where a worker attached to a healthy instance starves because that instance is the one that stopped polling.

**Long polling.** The worker makes an ordinary unary call carrying a maximum wait. The engine attempts the claim; if there is nothing, it holds the request open and retries on a paced interval until either work appears or the wait expires, then answers.

### 3.2 Decision: long polling

Long polling is chosen, and the reason is that it gets streaming's latency without streaming's state.

Latency: when work exists, the waiting request returns as soon as the engine's next internal attempt finds it, which is one paced interval rather than one worker poll interval.
Query load: the engine controls the retry cadence for every waiting worker centrally, so a thousand idle workers cost whatever the engine decides they cost, not whatever their poll intervals happen to multiply out to.
State: the in-flight request *is* the state. Nothing survives the call. A worker that disappears mid-poll costs one abandoned request, not a leaked session, and any worker can be answered by any engine instance with no affinity and no rebalancing.

The cost is real and worth naming: **a waiting request occupies a server thread.**
Section 4 is about not letting that become the unbounded queue the project has already banned twice.

### 3.3 What would change this decision

If a push channel appears, the calculation changes.
The Redis dispatch-hint index from HLD-001 section 4.7a, or Postgres `LISTEN/NOTIFY`, would let the engine wake a waiting request instead of retrying on a timer.
That is an improvement to the *inside* of the long poll and requires no protocol change, which is precisely why long polling is the right shape to commit to now: it can absorb that upgrade without the client noticing.

## 4. Backpressure on waiting requests

A long poll that blocks a platform thread for twenty seconds means the server's thread pool must be at least as large as the fleet, which is the connection-pool problem in a different costume.

Two mechanisms, together:

**Virtual threads carry the wait.**
A waiting poll is doing nothing but sleeping between attempts, which is exactly what virtual threads are for. Thousands of parked waiters cost memory rather than platform threads.

**An explicit semaphore bounds concurrent waiters.**
Virtual threads make waiting cheap, not free, and "cheap" is how unbounded queues get built. The engine caps how many polls may be waiting at once and answers the overflow with `RESOURCE_EXHAUSTED` and a retry hint.
A rejected worker backs off; it does not silently join a queue that nobody can see.

The database connection pool stays the harder bound, and it is unaffected: a waiting poll holds no connection between attempts, only during the brief claim itself.

## 5. RPC surface

One service, `sentinel.v1.TaskService`, plus `sentinel.v1.WorkflowService`.

| RPC | Shape | Deadline |
|---|---|---|
| `SubmitWorkflow` | unary | client-set, engine caps at 10s |
| `GetWorkflowStatus` | unary | client-set, engine caps at 10s |
| `PollForTasks` | unary, long poll | client-set, engine caps at 30s |
| `Heartbeat` | unary, batched over the worker's leases | 5s |
| `CompleteTask` | unary | 10s |
| `FailTask` | unary | 10s |

`Heartbeat` is batched deliberately.
A worker running sixteen tasks should renew sixteen leases in one call, not sixteen calls, because heartbeats are the highest-frequency traffic in the system and each one is otherwise a whole round trip to update a single timestamp.

## 6. Deadlines, and the one place they must not fight

Every RPC has an explicit deadline. That is table stakes.

The subtle part is the relationship between two timeouts that measure different things:

- The **gRPC deadline** bounds one network call.
- The **lease** bounds how long the engine will wait for a worker before giving its task to someone else.

These must never be confused, and specifically **a heartbeat's gRPC deadline must be far shorter than the lease**.
If a heartbeat's deadline were comparable to the lease duration, a heartbeat that hung would still be hanging when the lease expired, and the worker would learn it had been reaped only after continuing to work for the entire lease period.
With a 30s lease, a 10s heartbeat interval and a 5s heartbeat deadline, a worker gets two failed heartbeats and a clear signal before its lease is anywhere near expiry.

The lease is authoritative in all cases. A successful RPC never extends anything the database did not extend.

## 7. Ownership and the fencing rule at the transport boundary

The engine authorises by fencing token, not by connection identity.

This is worth stating at the transport layer because the intuitive alternative is wrong in an interesting way: it is tempting to trust the connection, on the grounds that a worker holding an open connection is obviously the worker that claimed the task.
It is not obvious at all. A worker can be alive and connected while stalled long enough to lose its lease, and its connection tells the engine nothing about whether its lease survived.

So every task-scoped RPC carries `task_id`, `worker_id` and `fencing_token`, and the engine's answer comes from the guarded update, never from who is calling.

## 8. Error mapping

| Condition | Status | Retryable by the client |
|---|---|---|
| Malformed DAG | `INVALID_ARGUMENT` | No |
| Unknown workflow or task | `NOT_FOUND` | No |
| At capacity | `RESOURCE_EXHAUSTED` + retry hint | Yes, after the hint |
| Too many waiting polls | `RESOURCE_EXHAUSTED` + retry hint | Yes, after the hint |
| Lease lost (fencing mismatch) | `FAILED_PRECONDITION` | **No, and this is the important one** |
| Postgres unreachable, pool exhausted | `UNAVAILABLE` | Yes, with backoff |
| Payload over the limit | `INVALID_ARGUMENT` | No |

`FAILED_PRECONDITION` on a lost lease must be non-retryable in the client's mind.
A worker that retries a rejected completion is a worker arguing with a decision that has already been made and acted on by someone else. The correct response is to abandon the task locally and poll for new work.

## 9. Payload limits

Task input and output are JSON documents carried as strings, capped at **256 KB** each.

gRPC's default message cap is 4 MB, and the cap here is deliberately far below it.
The reason is not the wire, it is the database: every payload is written to a `jsonb` column and every history event copies it into the outbox. A workflow moving megabyte payloads between steps turns the task table into a blob store and makes the queue slow for everybody else.
The documented answer for large data is the same one Temporal gives: put the object in storage the client owns, and pass a reference through the workflow.

## 10. Versioning

The proto package is `sentinel.v1`, and the version lives in the package name rather than in a field.

Within `v1`, changes must be wire-compatible: new fields get new tag numbers, existing tags are never reused or repurposed, and no field is ever removed.
A change that cannot be made compatibly becomes `sentinel.v2`, served alongside `v1` until clients migrate.
The engine and SDK ship together in this repo today, which makes it tempting to skip all of this. That temptation is exactly why it is written down now, while it costs nothing.

## 11. Failure modes

### 11.1 Half-open connections

A worker's connection dies in a way neither side notices: a NAT table drops the entry, a middlebox forgets it, a laptop lid closes.
The engine holds a poll that will never be collected, and the worker waits for a response that will never arrive.

Mitigation is on both sides.
gRPC keepalive pings detect a dead peer within a bounded time.
More fundamentally, the poll's own maximum wait means an abandoned poll expires on its own; the engine never waits on a worker indefinitely, whatever the socket believes.

For tasks already claimed, this is not a transport problem at all. The lease expires, the reaper re-queues, and the fencing token makes the ghost's eventual write harmless. That is the whole point of not making the transport authoritative.

### 11.2 Stampede after an engine restart

An engine instance restarts, or a deployment rolls, and every worker attached to it reconnects at once.
Every one of them immediately opens a long poll, and they arrive within milliseconds of each other because they were all disconnected by the same event.

Three mitigations, all of which the project already committed to elsewhere:
jittered reconnect backoff in the SDK, so the fleet does not return in a single wave;
the waiting-poll semaphore, so the engine sheds load rather than accepting an unbounded pile of waiters;
and `RESOURCE_EXHAUSTED` with a jittered retry hint, so the workers that are shed do not all come back together either.

The failure this prevents is subtle: without it, a restart is survivable but a *rolling* restart is not, because each instance's stampede lands on the instances that are still up.

## 12. Rejected alternatives

**Bidirectional streaming.** Rejected for the MVP, per section 3.1: it relocates polling rather than removing it, while adding per-worker engine state, flow control, and instance affinity. Reconsider once a push channel exists to make the stream genuinely push-driven.

**Plain short polling.** Rejected: meeting NFR3 requires a poll interval that generates enormous "is there anything?" query load from an idle fleet.

**A message broker between engine and workers.** Rejected in HLD-001 and not reopened here. Dispatch is a single-worker-gets-one-task relationship with no fan-out, which is the one shape a broker's strengths do not help with, and it would put a second source of truth next to Postgres.

**Trusting the connection for authorisation.** Rejected, per section 7. A live connection is not evidence of a live lease.

## 13. Open questions

1. Waiting-poll semaphore size, and whether it should be per-instance or fleet-wide. Per-instance for the MVP, because fleet-wide needs coordination the engine deliberately does not have.
2. Whether `PollForTasks` should return a partial batch as soon as anything is available, or wait briefly for the batch to fill. Returning immediately for the MVP; batching for throughput is a later optimisation with a latency cost.
3. TLS and authentication are out of scope for the MVP and must be settled before anything runs outside a trusted network.
