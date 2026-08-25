package dev.sentinel.worker;

import java.util.Objects;
import java.util.UUID;
import java.util.function.BooleanSupplier;

/**
 * Everything a handler is told about the task it is running.
 *
 * <p>The two members worth reading carefully are {@link #taskId()} and {@link #isLeaseLost()}.
 *
 * @param taskId <strong>the idempotency key you should use.</strong> Sentinel guarantees
 *        at-least-once execution, not exactly-once: a worker that dies mid-task has its work
 *        re-queued, and a worker that stalls past its lease can find the task running elsewhere.
 *        The engine cannot make an external side effect happen only once, because it does not
 *        control the external system. What it does guarantee is that every attempt at a given task
 *        carries the same {@code taskId}, so passing it as the idempotency key to a payment API,
 *        or using it as a natural key on an insert, turns at-least-once delivery into
 *        exactly-once effect
 * @param attempt which attempt this is, counting from one. Useful for logging, and for handlers
 *        that want to behave differently on a retry
 */
public record ActivityContext(
        UUID taskId,
        UUID workflowId,
        String taskName,
        String taskType,
        String input,
        int attempt,
        int maxAttempts,
        BooleanSupplier leaseLost) {

    public ActivityContext {
        Objects.requireNonNull(taskId, "taskId");
        Objects.requireNonNull(workflowId, "workflowId");
        Objects.requireNonNull(taskName, "taskName");
        Objects.requireNonNull(taskType, "taskType");
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(leaseLost, "leaseLost");
    }

    /**
     * Whether this worker has lost ownership of the task while running it.
     *
     * <p>This becomes true when a heartbeat is refused, which happens if the worker stalled long
     * enough for its lease to expire and another worker took the task. The engine will reject
     * anything this worker reports about it from that point on.
     *
     * <p>Long-running handlers should check this and stop early. Nothing forces them to: the SDK
     * will not interrupt handler code, because interrupting arbitrary user code halfway through a
     * side effect is not obviously safer than letting it finish. But continuing is wasted work, and
     * for a handler with external side effects it is duplicated work happening knowingly.
     */
    public boolean isLeaseLost() {
        return leaseLost.getAsBoolean();
    }

    public boolean isFinalAttempt() {
        return attempt >= maxAttempts;
    }
}
