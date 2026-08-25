package dev.sentinel.engine.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * One failed attempt, as stored in {@code task_failures}.
 *
 * <p>Distinct from {@link TaskFailure}, which is what a worker reports about the failure it just
 * had. This is the durable record of an attempt that already happened, including who was running
 * it and when.
 *
 * @param kind why it failed, including {@link FailureRecordKind#LEASE_EXPIRED}, which no worker can
 *        report because by definition no worker was there to report it
 * @param workerId who held the task. Null only when the engine never knew, which should not happen
 *        in practice and is left nullable rather than defaulted to a lie
 */
public record RecordedFailure(
        UUID taskId,
        UUID workflowId,
        int attempt,
        String workerId,
        FailureRecordKind kind,
        String errorClass,
        String errorMessage,
        Instant failedAt) {

    public RecordedFailure {
        Objects.requireNonNull(taskId, "taskId");
        Objects.requireNonNull(workflowId, "workflowId");
        Objects.requireNonNull(kind, "kind");
        errorMessage = errorMessage == null ? "" : errorMessage;
    }

    /** A failure the worker told us about. */
    public static RecordedFailure reported(
            UUID taskId, UUID workflowId, int attempt, String workerId, TaskFailure failure) {
        return new RecordedFailure(
                taskId, workflowId, attempt, workerId,
                FailureRecordKind.of(failure.kind()),
                null, failure.message(), null);
    }

    /** A failure nobody told us about, inferred from silence. */
    public static RecordedFailure leaseExpired(
            UUID taskId, UUID workflowId, int attempt, String workerId) {
        return new RecordedFailure(
                taskId, workflowId, attempt, workerId,
                FailureRecordKind.LEASE_EXPIRED,
                null,
                "the lease expired before the worker reported anything",
                null);
    }
}
