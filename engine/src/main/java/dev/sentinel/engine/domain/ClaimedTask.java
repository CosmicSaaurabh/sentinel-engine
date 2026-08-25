package dev.sentinel.engine.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * What a worker receives when it wins a claim: enough to execute, and the fencing token it must
 * echo on every subsequent call about this task.
 *
 * <p>This is deliberately narrower than {@link Task}. The claim path is the hottest query in the
 * system and returns only the columns the worker actually uses.
 */
public record ClaimedTask(
        UUID id,
        UUID workflowId,
        String name,
        String taskType,
        String input,
        int attempt,
        int maxAttempts,
        long fencingToken,
        Instant leaseExpiresAt,
        String traceContext) {

    public ClaimedTask {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(workflowId, "workflowId");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(taskType, "taskType");
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(leaseExpiresAt, "leaseExpiresAt");
    }
}
