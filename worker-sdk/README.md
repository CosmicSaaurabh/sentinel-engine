# Sentinel Worker SDK

A plain Java library for running Sentinel tasks.
You write handlers; the SDK deals with claiming work, keeping leases alive, reporting outcomes, backing off, and shutting down cleanly.

No Spring dependency, and there never will be one.
A worker should run inside a Spring app, a Quarkus app, or a bare `main` method, and a library that drags a framework in takes that choice away from you.

---

## Contents

1. [Quick start](#quick-start)
2. [Writing a handler](#writing-a-handler)
3. [Idempotency: the one thing you must get right](#idempotency-the-one-thing-you-must-get-right)
4. [Classifying failures](#classifying-failures)
5. [What the worker starts](#what-the-worker-starts)
6. [Capacity and backpressure](#capacity-and-backpressure)
7. [Shutdown](#shutdown)
8. [Configuration reference](#configuration-reference)
9. [Testing your handlers](#testing-your-handlers)
10. [Common mistakes](#common-mistakes)
11. [Troubleshooting](#troubleshooting)

---

## Quick start

Add the dependency:

```xml
<dependency>
    <groupId>dev.sentinel</groupId>
    <artifactId>worker-sdk</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

Write a worker:

```java
import dev.sentinel.worker.SentinelWorker;
import dev.sentinel.worker.TaskResult;

public final class BillingWorker {

    public static void main(String[] args) throws InterruptedException {
        try (SentinelWorker worker = SentinelWorker.builder()
                .engineTarget("localhost:9090")
                .workerId(System.getenv("HOSTNAME"))     // stable across restarts, ideally
                .concurrency(8)
                .register("billing.charge", context -> {
                    ChargeRequest request = Json.parse(context.input(), ChargeRequest.class);

                    // context.taskId() is the same on every attempt of this task.
                    // That is what makes a retry safe.
                    paymentApi.charge(request, context.taskId());

                    return TaskResult.completed("{\"charged\":true}");
                })
                .build()) {

            worker.start();
            Thread.currentThread().join();     // or your framework's shutdown latch
        }
    }
}
```

That is the whole API surface most applications touch: `builder()`, `register()`, `start()`, and `close()`.

`SentinelWorker` is `AutoCloseable`, so a try-with-resources block gives you the correct shutdown sequence for free.

---

## Writing a handler

A handler is a function from an `ActivityContext` to a `TaskResult`:

```java
public interface ActivityHandler {
    TaskResult handle(ActivityContext context) throws Exception;
}
```

It is ordinary Java.
It does not talk to the engine, renew its own lease, or decide whether to retry.
The SDK owns all of that, which means you can unit test a handler by calling it.

### What you get

| `context.` | What it is |
|---|---|
| `taskId()` | Stable across every attempt of this task. **Use it as your idempotency key.** |
| `workflowId()` | The workflow this task belongs to |
| `taskName()` | The name from the DAG definition |
| `taskType()` | The routing key that brought it here |
| `input()` | The JSON document from the DAG definition |
| `attempt()` / `maxAttempts()` | Which attempt this is, and the budget |
| `isLeaseLost()` | Whether the engine has taken this task away while you were running it |
| `isFinalAttempt()` | Convenience for `attempt() >= maxAttempts()` |

### Throwing is fine

The signature declares `throws Exception` deliberately.
Forcing handlers to catch everything produces the worst possible outcome, which is a handler wrapped in `catch (Exception e) {}` written purely to satisfy the compiler.

An uncaught exception **fails the task, not the worker**.
The SDK catches it, records the exception class and message against the task, reports it as retryable, and the thread carries on.

---

## Idempotency: the one thing you must get right

Sentinel guarantees **at-least-once execution**, not exactly-once.

That is not a limitation waiting to be fixed; it is a consequence of the engine not controlling your downstream systems.
If a worker dies halfway through charging a card, the engine cannot know whether the charge happened.
It has two choices: re-run the task and risk a double charge, or drop it and risk a customer never being charged.
Sentinel always re-runs, because a duplicate you can prevent is better than a loss you cannot detect.

Preventing the duplicate is your side of the bargain, and the SDK hands you exactly what you need for it.

**`context.taskId()` is identical on every attempt of the same task.**
So:

```java
// An external API that supports idempotency keys
paymentApi.charge(amount, context.taskId());

// Or your own database
jdbc.update("""
    INSERT INTO charges (idempotency_key, amount)
    VALUES (?, ?)
    ON CONFLICT (idempotency_key) DO NOTHING
    """, context.taskId(), amount);
```

Both turn "this ran twice" into "this took effect once".

### When you can skip it

If a task only mutates state inside Postgres through the engine's own transaction, you do not need an idempotency key: the engine's atomicity already covers you.
Anything that reaches outside — an HTTP call, a message, an email, a file — needs one.

### The scenarios that actually cause a second run

- The worker process dies mid-task. The lease expires and the task is re-queued.
- The worker stalls past its lease: a long garbage-collection pause, a suspended laptop, a network partition. The engine gives the task to someone else while the first worker is still running it.
- The worker completes the task but the response is lost on the way back. The report is retried, and this one is harmless: the engine recognises it and answers "already recorded".

The second case is the uncomfortable one, and it is why `isLeaseLost()` exists.

---

## Classifying failures

Three outcomes, and the distinction between the last two matters more than it looks:

```java
return TaskResult.completed(outputJson);              // worked
return TaskResult.failed("upstream timed out");       // might work next time
return TaskResult.permanentlyFailed("no such user");  // will never work
```

| Result | Engine behaviour |
|---|---|
| `completed` | Task done, downstream tasks unblock |
| `failed` | Retried with exponential backoff until the attempt budget runs out |
| `permanentlyFailed` | Terminal immediately, workflow fails |
| Thrown exception | Same as `failed` |

**Classify honestly.**
Marking a genuine bug as retryable turns one failure into ten identical failures spread over minutes.
Marking a transient network blip as permanent fails a workflow that would have succeeded a second later.

A rule of thumb: if running the exact same input again could plausibly produce a different result, it is retryable.
A `404` is permanent; a `503` is retryable. A `NullPointerException` in your own code is permanent in spirit, though the SDK will report it as retryable because it cannot tell.

### Long-running handlers should check `isLeaseLost()`

```java
for (Record record : batch) {
    if (context.isLeaseLost()) {
        return TaskResult.failed("lease lost partway through the batch");
    }
    process(record);
}
```

This becomes true when a heartbeat is refused, which means another worker already has this task.
Nothing forces you to check it: the SDK will not interrupt your code, because interrupting arbitrary user code halfway through a side effect is not obviously safer than letting it finish.
But once it is true, everything you do afterwards is duplicated work, and the engine will reject whatever you report.

---

## What the worker starts

One worker owns exactly three kinds of thread:

| Thread | Count | Job | May block? |
|---|---|---|---|
| `sentinel-poller` | 1 | Asks the engine for work and hands it off | Yes, that is its job |
| `sentinel-activity-N` | `concurrency` | Runs your handler code | Yes, arbitrarily |
| `sentinel-heartbeat` | 1 | Renews every in-flight lease | Briefly, one call |

All are daemon threads, so a forgotten `close()` cannot keep your JVM alive.
That is a backstop, not the mechanism: closing properly is what drains work cleanly.

### Why the poller never runs handlers

If it did, one slow task would stop the worker claiming anything else, and a handler that blocked forever would wedge the worker permanently while every external signal still said it was healthy.

### Why heartbeats get their own thread

This is the subtle one.

Suppose heartbeats were sent from the activity threads.
Now saturate those threads with CPU-bound work.
The threads cannot get scheduled in time to heartbeat, the leases expire, the engine concludes the worker is dead, and it hands all that work to somebody else — while the original worker is still busily doing it.

The worker was healthy. It missed its heartbeats *because* it was working hard.
Load caused a failure that looks exactly like a crash, and it gets worse the busier you are.

A thread that does nothing but send one small batched call on a timer does not have that failure mode.

---

## Capacity and backpressure

`concurrency` is the number of tasks this worker runs at once.
It is the size of the activity pool **and** the number of permits the poller holds.

The pool has **no queue at all**.
That looks needlessly strict until you notice what a queued task would be: a task reaches the pool only after being claimed, and a claimed task holds a lease.
Sitting in a queue makes no progress towards anything the engine measures, so queue depth converts directly into lease expiry, re-queue, and the same work running twice.

So the worker never claims work it cannot start immediately:

```
free = concurrency - tasksInFlight
if free == 0 -> do not poll at all, wait for a task to finish
else         -> ask for min(free, maxBatchSize) tasks
```

A fully busy worker makes no requests.
This is why raising `concurrency` is the right knob for throughput and raising `maxBatchSize` is not: the batch size only ever reduces round trips within capacity you already have.

### Backing off

Two independent backoffs, easy to conflate and important not to:

- **Transport backoff**, when the engine is unreachable. Exponential from 100ms to 30s, with full jitter.
- **Rejection backoff**, when the engine is shedding load. It honours the engine's own `retry-after` hint, which the engine has already jittered.

An **empty poll is neither of these**.
It is the ordinary answer to an empty queue, and the correct response is to ask again immediately, because the engine already held the request open for the whole wait.
Sleeping after an empty poll means waiting twice for the same thing.

Jitter is not decoration.
When an engine instance restarts, every worker attached to it fails at the same instant, so a deterministic backoff has all of them return at the same instant too, in waves, each capable of knocking the recovering instance back down.

---

## Shutdown

`close()` runs a fixed sequence:

1. **Stop polling.** No new work enters the worker.
2. **Keep heartbeating.** Tasks still running still hold leases and must keep them.
3. **Wait for in-flight handlers**, up to `drainTimeout` (default 30s).
4. **Abandon anything still running.** Do not interrupt it, do not report it.
5. **Stop heartbeating**, stop the pool, close the channel.

Two of those are counter-intuitive and both are deliberate.

**Step 2 comes before step 3, not after.**
Stopping heartbeats first would expire the leases of tasks that were seconds away from finishing successfully.

**Step 4 abandons rather than reporting failure.**
Reporting undrained tasks as failed sounds more helpful: they would be re-queued immediately instead of after a lease expiry. It is worse.
Those tasks are *still executing*. Telling the engine they are available starts a second copy **alongside** the first rather than after it.
At-least-once quietly becomes at-least-twice-simultaneously, which is much harder for your idempotency key to absorb than one lease duration of delay.

If you see this in your logs, it means a handler outlived the drain timeout:

```
worker billing-1 still had 2 task(s) running after PT30S; abandoning them so their
leases expire, rather than reporting work that is still executing
```

The fix is a longer `drainTimeout`, or handlers that finish faster, or handlers with their own internal timeouts.

---

## Configuration reference

| Builder method | Default | Notes |
|---|---|---|
| `engineTarget(String)` | required | `host:port` of the engine, for example `localhost:9090` |
| `channel(ManagedChannel)` | none | Use instead of `engineTarget` for TLS, interceptors, or a custom resolver. The worker closes it |
| `workerId(String)` | generated | **Set this in production.** A stable id, such as the pod name |
| `concurrency(int)` | 4 | Tasks run at once. The throughput knob |
| `pollWait(Duration)` | 20s | How long the engine may hold a poll open. Longer means fewer round trips when idle |
| `heartbeatInterval(Duration)` | 10s | Must stay well below the engine's lease duration |
| `drainTimeout(Duration)` | 30s | How long `close()` waits for running handlers |
| `maxBatchSize(int)` | 8 | Ceiling on tasks per poll, further clamped by free capacity and by the engine |

### The one relationship that matters

`heartbeatInterval` must be comfortably shorter than the engine's lease duration, and by default it is exactly a third of it (10s against a 30s lease).

That ratio lets a worker miss **two** heartbeats to a network blip without losing work it is still doing, while still meeting the engine's promise to recover a genuinely dead worker's task within three missed intervals.

Raising it towards the lease duration removes that margin: one dropped packet then costs the worker its task.
Lowering it costs a little traffic and buys nothing, because the lease is what the engine actually measures.

### Setting `workerId` properly

If you do not set it, one is generated per process.
That works, but every restart looks like a brand new worker, so the engine's registry reads as a stream of strangers rather than a fleet.

In Kubernetes, `System.getenv("HOSTNAME")` gives you the pod name, which is exactly right.

---

## Testing your handlers

A handler is a plain function, so test it as one:

```java
@Test
void chargesTheCard() throws Exception {
    ActivityContext context = new ActivityContext(
            UUID.randomUUID(),          // taskId
            UUID.randomUUID(),          // workflowId
            "charge", "billing.charge",
            "{\"amount\":1000}",
            1, 10,
            () -> false);               // lease not lost

    TaskResult result = new ChargeHandler(paymentApi).handle(context);

    assertThat(result).isInstanceOf(TaskResult.Completed.class);
}
```

No engine, no server, no container.
If a handler is hard to test this way, it is doing something the SDK was meant to do for it.

To test the retry path, pass an `attempt()` greater than 1.
To test the lease-lost path, pass `() -> true` as the last argument.

---

## Common mistakes

**Claiming more than you can run.**
You cannot: the SDK will not let you. But if you find yourself wanting a bigger `maxBatchSize` to increase throughput, raise `concurrency` instead. Batch size only reduces round trips within capacity you already have.

**Handlers without their own timeout.**
The SDK will not interrupt a handler that never returns. It holds one of your concurrency permits until the process ends, and your worker silently runs at reduced capacity. Give any handler that makes a network call its own deadline.

**Treating `LeaseLostException` as retryable.**
It is not transient. It is a decision the engine has already made and another worker has already acted on. The SDK handles it for you by abandoning the task; if you catch it yourself somewhere, do not retry.

**Sleeping after an empty poll.**
The engine already waited. Sleeping again doubles your dispatch latency for no benefit. The SDK does not do this; do not add it.

**A random `workerId` per restart.**
Works, but makes the worker registry useless for operators trying to answer "is that pod alive?".

**Swallowing exceptions in the handler to look tidy.**
Let them out. The SDK records the class and message against the task, which is the difference between a debuggable failure and a mysterious one.

---

## Troubleshooting

**The worker starts but never runs anything.**
Check that the task types you registered match the `task_type` in the workflow definition exactly. The engine only offers a worker the types it advertised, so a typo produces a healthy, permanently idle worker. The startup log line lists what was registered:

```
worker billing-1 started with concurrency 8 for task types [billing.charge]
```

**`no handler registered for task type X` failures.**
The engine offered a type this worker did not register. That usually means two worker deployments share a `workerId`, or a handler registration was lost in a refactor.

**Tasks keep being re-queued while the worker looks healthy.**
Heartbeats are not getting through. Either the engine is unreachable (check for `engine unavailable` warnings) or handlers are saturating the machine so badly that even the dedicated heartbeat thread cannot get scheduled. The second is rare, and points at an over-large `concurrency` for the hardware.

**`lost the lease on task X` warnings.**
This worker stalled past its lease and the engine reassigned the task. Look for garbage-collection pauses, a suspended host, or a network partition. The work is not lost; it ran somewhere else. If it happens regularly, the machine is overloaded.

**Shutdown takes the full `drainTimeout` every time.**
A handler is not returning. Check for missing timeouts on network calls.
