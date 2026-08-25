# HLD-003: Failure Detection and Recovery

- Status: proposed
- Date: 2026-08-25
- Governs: GitHub issue #11, and the implementation tasks T3.3 and T3.4
- Builds on: HLD-001 (system architecture), HLD-002 (transport), LLD-001 (schema and queue)

## 1. Scope

How the engine notices that a worker has stopped making progress, and what it does about it.

This document exists because of a gap Phase 2 made concrete rather than hypothetical.
Building the worker SDK produced a test that failed roughly one run in three: a workflow submitted cleanly, no worker logged an error, and one task sat `RUNNING` forever.
The cause was that closing a worker interrupts its in-flight poll, but the engine may already have committed that claim, leaving a task owned by a worker that never received it.

Nothing in the system currently recovers from that.
The lease column exists, the fencing token exists, and nothing reads them.
This document specifies the piece that does.

## 2. What failure actually looks like

The engine cannot tell these apart, and the central design point is that **it does not need to**:

- the worker process was killed;
- the machine lost power;
- the network partitioned;
- the JVM entered a long garbage-collection pause;
- the laptop lid closed;
- the worker is alive and healthy but its poll was cancelled before it received the task.

Every one presents identically: a task is `RUNNING`, and nobody is saying anything about it.
Designing around "is this worker dead?" would require answering an unanswerable question.
Designing around "has anyone spoken about this task recently?" is a local question with a definite answer.

That is what a lease is: not a claim about a worker's health, but a deadline on silence.

## 3. The lease and heartbeat contract

| Value | Setting | Why |
|---|---|---|
| Lease duration | 30s | The window of silence the engine tolerates |
| Heartbeat interval | 10s | A third of the lease |
| Reaper interval | 5s, jittered | Bounds detection latency well inside the lease |

The ratio is the load-bearing part.
Three heartbeats per lease means a worker can lose two of them to a blip and keep its task, while a genuinely silent worker is detected within one lease.
That satisfies NFR4, which is expressed as "re-queued within three missed heartbeat intervals" precisely because that phrasing is what the ratio guarantees.

**Recovery time budget**: a task held by a dead worker becomes claimable again within `lease duration + reaper interval + jitter`, so under 40s at the defaults, with detection dominated by the lease rather than by the reaper.
Shortening recovery means shortening the lease, which costs tolerance for blips. That trade is the only knob here, and it is deliberate that it is a single knob.

Every timestamp involved comes from the database clock.
Lease expiry compares "when does this lease end" against "what time is it now", and if those came from different machines, clock skew would show as tasks reaped while their worker is perfectly healthy.

## 4. The reaper is leaderless

Any engine instance may reap, at any time, with no election and no coordination.

That is not a simplification, it falls out of the same property that makes claiming leaderless: the re-queue is a single guarded `UPDATE`, so two instances reaping the same expired task simply means one of them affects zero rows.
The reaper claims its batch with `FOR UPDATE SKIP LOCKED`, exactly as workers claim tasks, so several reapers running at once **divide** the work rather than colliding over it.

There is deliberately no "reaper split-brain" failure mode to design against, because there is nothing for the reapers to disagree about. This is worth stating explicitly, because the instinct on seeing a background recovery process is to ask who owns it, and here the correct answer is "anyone, concurrently".

## 5. The guarantee, stated precisely

Sentinel guarantees **at-least-once execution and exactly-once state transition**.

Those are different claims about different things, and conflating them is how people end up expecting something the system cannot provide.

**Exactly-once state transition** is a claim about the database.
A task moves from `RUNNING` to `COMPLETED` exactly once, because that transition is a guarded update that matches exactly one row exactly once. A second report of the same outcome matches nothing and is recognised as a duplicate. The dependency counter is decremented exactly once, because it happens in the same transaction as the transition that authorises it.

**At-least-once execution** is a claim about the world, and it is weaker on purpose.
The engine cannot guarantee your handler's side effects happen once, because it does not control the systems those side effects reach. If a worker charges a card and dies before reporting, the engine has no way to learn that the charge happened. It must choose between re-running (risking a double charge) and dropping (risking a customer never charged), and it always re-runs, because a duplicate the client can prevent beats a loss nobody can detect.

**What the engine provides so the client can close that gap**: every attempt of a given task carries the same `task_id`. Used as an idempotency key against an external API, or as a natural key on an insert, it converts at-least-once delivery into exactly-once effect.

**What the engine cannot do**: make a non-idempotent side effect safe. That is stated plainly in the SDK guide rather than buried, because a client that does not know this will eventually be surprised by it in production.

## 6. Why fencing, not liveness

The obvious alternative is a liveness registry: workers report in, the engine tracks who is alive, and it only accepts writes from live workers.

It does not work, and the reason is instructive. Liveness is a distributed judgement, and two parties can disagree about it: a worker stalled by a garbage-collection pause looks dead to the registry while being very much alive and halfway through a side effect. Acting on the registry's judgement means the engine reassigns the task while the original worker is still working, and then accepts that worker's write afterwards because it has since checked in again.

Fencing sidesteps the judgement entirely.
The token increments on every claim, so "do you still own this?" is a comparison against a number rather than an opinion about a process. A stalled worker's write is rejected because its number is stale, regardless of whether anyone believes it is alive.

The `workers` table still exists, but only for operator visibility and capacity accounting. It is never consulted for correctness, and that separation is why a stale row there costs a dashboard its accuracy and can never cause a task to run twice.

## 7. Retry, and what the reaper does with an exhausted task

An expired lease is a failed attempt, and it is counted as one.
`attempt` is incremented at claim time rather than at failure time, precisely so that a task which kills its worker without ever reporting still burns budget. Otherwise a poison task would be retried forever and would take a worker with it each time.

The reaper therefore has two outcomes, and they are two separate statements rather than one with a `CASE`:

- **Attempts remain**: re-queue as `PENDING` with a backoff delay, so a mass expiry does not stampede straight back into the queue.
- **Attempts spent**: mark `FAILED` and let normal failure propagation run.

Keeping them separate means metrics and logs say which happened, which matters when diagnosing whether a fleet is flapping or a task is genuinely poisoned.

## 8. Failure modes

### 8.1 Mass lease expiry

A hundred workers die at once, or a network partition heals, and thousands of leases expire in the same second.

An unbounded reaper would try to re-queue the entire backlog in one transaction, holding locks over thousands of rows and blocking every claimer for the duration. Then all of those tasks would become claimable simultaneously and the fleet would stampede.

Four mitigations, and each addresses a different part of that sentence:

- **Bounded batch per pass**, so no transaction is ever large.
- **Jittered interval between passes**, so several engine instances do not reap in lockstep.
- **`SKIP LOCKED` on the reaper's own selection**, so concurrent reapers divide the work.
- **A backoff on re-queue**, so recovered tasks do not all become claimable at the same instant.

The last one is the least obvious and the most important: without it, the reaper solves the lock contention and then recreates the stampede one step later.

### 8.2 A zombie worker's late write

A worker stalls past its lease, the task is re-queued, another worker claims and completes it, and then the first worker wakes up and reports.

This is not prevented, it is made harmless. The fencing token has advanced, so the guarded update matches nothing and the report is rejected with `FAILED_PRECONDITION`. The SDK abandons the task locally rather than retrying, because retrying would mean arguing with a decision another worker has already acted on.

The uncomfortable part is honest: **the zombie's side effects already happened**. The engine cannot undo them. What it guarantees is that its own state is not corrupted by them, and that the task carried the same `task_id` on both attempts so the client had the means to make the second one a no-op.

### 8.3 Clock skew

Deliberately designed out rather than mitigated. Every timestamp in the lease calculation is produced by `now()` inside the SQL statement that uses it, so there is exactly one clock in the comparison and it belongs to Postgres.

The residual exposure is not clock skew at all but scheduling delay: a worker whose own process is descheduled long enough to miss heartbeats. That is section 8.2, and it is handled by fencing rather than by timing.

## 9. Rejected alternatives

**A liveness registry as the correctness mechanism.** Rejected per section 6: liveness is a judgement two parties can disagree about, and acting on it produces exactly the split-brain that fencing avoids.

**An elected single reaper.** Rejected: it buys nothing, because there is nothing for concurrent reapers to disagree about, and it adds an election, a failover gap, and a single point of stall.

**Push-based cancellation to a stalled worker.** Rejected for the MVP: it requires the engine to hold a channel per worker, which HLD-002 explicitly declined, and it cannot be relied on anyway, since a worker that is not responding is also not receiving.

**Reaping in one statement with a `CASE` for the exhausted branch.** Rejected: it makes the two outcomes indistinguishable in metrics and in the SQL, and those two outcomes mean very different things operationally.

**A dedicated `retry_queue` table for delayed tasks.** Rejected: `next_attempt_at` on the task row already expresses "not before this time", the claim query already honours it, and a second table would need its own claim mechanism and its own reconciliation with the first.

## 10. Open questions

1. Whether the reaper should pace itself against observed queue depth rather than running on a fixed jittered interval. Fixed for the MVP; adaptive pacing is an optimisation with its own failure modes.
2. Whether a task should be able to opt out of automatic re-queue, for handlers whose side effects are known to be unsafe to repeat. Not offered for now, because it would trade an at-least-once guarantee for a silent-loss one, and that should be a deliberate product decision rather than a flag.
