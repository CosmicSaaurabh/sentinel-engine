package dev.sentinel.engine.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * A task instance as stored, including its queue state.
 *
 * <p>There is no separate queue table: this row is the queue entry. {@code status},
 * {@code nextAttemptAt} and {@code lease} together are what the claim query and the reaper read.
 *
 * @param lease present exactly when {@code status} is {@link TaskStatus#RUNNING}, an invariant the
 *        database enforces with the {@code tasks_lease_consistency} constraint
 * @param fencingToken incremented on every claim, and the only proof of current ownership. It
 *        persists after the lease is released so a returning zombie worker still fails the check
 * @param pendingDependencies parents not yet completed. Reaching zero is what flips BLOCKED to
 *        PENDING
 */
public record Task(
        UUID id,
        UUID workflowId,
        String name,
        String taskType,
        TaskStatus status,
        String input,
        String output,
        TaskFailure lastFailure,
        int attempt,
        int maxAttempts,
        int pendingDependencies,
        Instant nextAttemptAt,
        Lease lease,
        long fencingToken,
        Instant createdAt,
        Instant updatedAt) {

    public Task {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(workflowId, "workflowId");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(taskType, "taskType");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(nextAttemptAt, "nextAttemptAt");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");
        if ((status == TaskStatus.RUNNING) != (lease != null)) {
            throw new IllegalArgumentException(
                    "a lease must be present exactly when the task is RUNNING, but status was "
                            + status + " and lease was " + lease);
        }
    }

    public Optional<Lease> leaseIfPresent() {
        return Optional.ofNullable(lease);
    }

    public Optional<TaskFailure> lastFailureIfPresent() {
        return Optional.ofNullable(lastFailure);
    }

    public Optional<String> outputIfPresent() {
        return Optional.ofNullable(output);
    }

    /** Whether another attempt is permitted. A claim consumes an attempt, so this gates re-queueing. */
    public boolean hasAttemptsRemaining() {
        return attempt < maxAttempts;
    }
}
