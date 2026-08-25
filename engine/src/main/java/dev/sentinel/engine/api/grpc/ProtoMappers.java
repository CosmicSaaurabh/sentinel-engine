package dev.sentinel.engine.api.grpc;

import com.google.protobuf.Timestamp;
import dev.sentinel.engine.domain.ClaimedTask;
import dev.sentinel.engine.domain.FailureKind;
import dev.sentinel.engine.domain.Task;
import dev.sentinel.engine.domain.TaskDefinition;
import dev.sentinel.engine.domain.TaskStatus;
import dev.sentinel.engine.domain.Workflow;
import dev.sentinel.engine.domain.WorkflowDefinition;
import dev.sentinel.engine.domain.WorkflowStatus;
import dev.sentinel.proto.v1.AssignedTask;
import dev.sentinel.proto.v1.GetWorkflowStatusResponse;
import dev.sentinel.proto.v1.SubmitWorkflowRequest;
import dev.sentinel.proto.v1.TaskSummary;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Conversion between the wire types and the domain.
 *
 * <p>This class is the entire reason the domain has no protobuf imports. Generated message classes
 * are mutable builders with default-valued fields and no notion of an invariant; letting them reach
 * the service layer would mean every service method re-checking things the domain records already
 * guarantee in their constructors.
 *
 * <p>Two conversions here are less mechanical than they look.
 *
 * <p><strong>Enums are mapped explicitly, never by ordinal or by name.</strong> Matching on
 * {@code name()} would couple the wire contract to a Java identifier, so renaming a domain constant
 * would silently break every deployed client. Matching on ordinal would be worse still. The switch
 * is exhaustive, so adding a status to either side stops compiling until both are considered.
 *
 * <p><strong>An absent optional means absent, not zero.</strong> proto3 gives no field presence for
 * scalars unless they are marked {@code optional}, which is exactly why {@code max_attempts} and
 * {@code submission_key} are declared that way: a client that omits them must not be understood as
 * having sent zero or an empty string.
 */
public final class ProtoMappers {

    private ProtoMappers() {
    }

    // ------------------------------------------------------------------
    // Inbound
    // ------------------------------------------------------------------

    public static WorkflowDefinition toDefinition(SubmitWorkflowRequest request) {
        List<TaskDefinition> tasks = request.getTasksList().stream()
                .map(ProtoMappers::toTaskDefinition)
                .toList();

        return new WorkflowDefinition(
                request.getName(),
                request.hasSubmissionKey() ? request.getSubmissionKey() : null,
                request.hasScheduledAt() ? toInstant(request.getScheduledAt()) : null,
                tasks);
    }

    private static TaskDefinition toTaskDefinition(dev.sentinel.proto.v1.TaskDefinition task) {
        return new TaskDefinition(
                task.getName(),
                task.getTaskType(),
                task.getInput(),
                task.hasMaxAttempts() ? task.getMaxAttempts() : null,
                task.getDependsOnList());
    }

    public static FailureKind toFailureKind(dev.sentinel.proto.v1.FailureKind kind) {
        return switch (kind) {
            case FAILURE_KIND_PERMANENT -> FailureKind.PERMANENT;
            case FAILURE_KIND_RETRYABLE -> FailureKind.RETRYABLE;
            // A client that did not say is treated as retryable. Defaulting the other way would
            // let an omitted field consume a task's entire remaining budget in one attempt.
            case FAILURE_KIND_UNSPECIFIED, UNRECOGNIZED -> FailureKind.RETRYABLE;
        };
    }

    public static UUID toUuid(String value, String field) {
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("%s is not a valid id: '%s'".formatted(field, value), e);
        }
    }

    // ------------------------------------------------------------------
    // Outbound
    // ------------------------------------------------------------------

    public static AssignedTask toAssignedTask(ClaimedTask task) {
        AssignedTask.Builder builder = AssignedTask.newBuilder()
                .setTaskId(task.id().toString())
                .setWorkflowId(task.workflowId().toString())
                .setName(task.name())
                .setTaskType(task.taskType())
                .setInput(task.input())
                .setAttempt(task.attempt())
                .setMaxAttempts(task.maxAttempts())
                .setFencingToken(task.fencingToken())
                .setLeaseExpiresAt(toTimestamp(task.leaseExpiresAt()));

        if (task.traceContext() != null) {
            builder.setTraceContext(task.traceContext());
        }
        return builder.build();
    }

    public static GetWorkflowStatusResponse toStatusResponse(Workflow workflow, List<Task> tasks) {
        GetWorkflowStatusResponse.Builder response = GetWorkflowStatusResponse.newBuilder()
                .setWorkflowId(workflow.id().toString())
                .setName(workflow.name())
                .setStatus(toProtoWorkflowStatus(workflow.status()))
                .setCreatedAt(toTimestamp(workflow.createdAt()));

        if (workflow.startedAt() != null) {
            response.setStartedAt(toTimestamp(workflow.startedAt()));
        }
        if (workflow.finishedAt() != null) {
            response.setFinishedAt(toTimestamp(workflow.finishedAt()));
        }
        tasks.forEach(task -> response.addTasks(toTaskSummary(task)));
        return response.build();
    }

    private static TaskSummary toTaskSummary(Task task) {
        TaskSummary.Builder summary = TaskSummary.newBuilder()
                .setTaskId(task.id().toString())
                .setName(task.name())
                .setTaskType(task.taskType())
                .setStatus(toProtoTaskStatus(task.status()))
                .setAttempt(task.attempt())
                .setMaxAttempts(task.maxAttempts());

        if (task.output() != null) {
            summary.setOutput(task.output());
        }
        task.lastFailureIfPresent().ifPresent(failure -> {
            summary.setLastError(failure.message());
            summary.setLastErrorKind(toProtoFailureKind(failure.kind()));
        });
        return summary.build();
    }

    private static dev.sentinel.proto.v1.TaskStatus toProtoTaskStatus(TaskStatus status) {
        return switch (status) {
            case BLOCKED -> dev.sentinel.proto.v1.TaskStatus.TASK_STATUS_BLOCKED;
            case PENDING -> dev.sentinel.proto.v1.TaskStatus.TASK_STATUS_PENDING;
            case RUNNING -> dev.sentinel.proto.v1.TaskStatus.TASK_STATUS_RUNNING;
            case COMPLETED -> dev.sentinel.proto.v1.TaskStatus.TASK_STATUS_COMPLETED;
            case FAILED -> dev.sentinel.proto.v1.TaskStatus.TASK_STATUS_FAILED;
            case CANCELLED -> dev.sentinel.proto.v1.TaskStatus.TASK_STATUS_CANCELLED;
        };
    }

    private static dev.sentinel.proto.v1.WorkflowStatus toProtoWorkflowStatus(WorkflowStatus status) {
        return switch (status) {
            case RUNNING -> dev.sentinel.proto.v1.WorkflowStatus.WORKFLOW_STATUS_RUNNING;
            case COMPLETED -> dev.sentinel.proto.v1.WorkflowStatus.WORKFLOW_STATUS_COMPLETED;
            case FAILED -> dev.sentinel.proto.v1.WorkflowStatus.WORKFLOW_STATUS_FAILED;
        };
    }

    private static dev.sentinel.proto.v1.FailureKind toProtoFailureKind(FailureKind kind) {
        return switch (kind) {
            case RETRYABLE -> dev.sentinel.proto.v1.FailureKind.FAILURE_KIND_RETRYABLE;
            case PERMANENT -> dev.sentinel.proto.v1.FailureKind.FAILURE_KIND_PERMANENT;
        };
    }

    public static dev.sentinel.proto.v1.TaskFailureRecord toFailureRecord(
            dev.sentinel.engine.domain.RecordedFailure failure) {

        dev.sentinel.proto.v1.TaskFailureRecord.Builder record =
                dev.sentinel.proto.v1.TaskFailureRecord.newBuilder()
                        .setAttempt(failure.attempt())
                        .setKind(toProtoFailureRecordKind(failure.kind()))
                        .setErrorMessage(failure.errorMessage());

        if (failure.workerId() != null) {
            record.setWorkerId(failure.workerId());
        }
        if (failure.errorClass() != null) {
            record.setErrorClass(failure.errorClass());
        }
        if (failure.failedAt() != null) {
            record.setFailedAt(toTimestamp(failure.failedAt()));
        }
        return record.build();
    }

    public static dev.sentinel.proto.v1.DeadLetterTask toDeadLetterTask(
            dev.sentinel.engine.repository.DeadLetterRepository.DeadLetterTask task) {

        dev.sentinel.proto.v1.DeadLetterTask.Builder builder =
                dev.sentinel.proto.v1.DeadLetterTask.newBuilder()
                        .setTaskId(task.taskId().toString())
                        .setWorkflowId(task.workflowId().toString())
                        .setWorkflowName(task.workflowName())
                        .setTaskName(task.taskName())
                        .setTaskType(task.taskType())
                        .setAttempt(task.attempt())
                        .setMaxAttempts(task.maxAttempts())
                        .setRecordedFailures(task.recordedFailures());

        if (task.lastError() != null) {
            builder.setLastError(task.lastError());
        }
        if (task.lastErrorKind() != null) {
            builder.setLastErrorKind(toProtoFailureKind(task.lastErrorKind()));
        }
        if (task.failedAt() != null) {
            builder.setFailedAt(toTimestamp(task.failedAt()));
        }
        return builder.build();
    }

    private static dev.sentinel.proto.v1.FailureRecordKind toProtoFailureRecordKind(
            dev.sentinel.engine.domain.FailureRecordKind kind) {
        return switch (kind) {
            case RETRYABLE -> dev.sentinel.proto.v1.FailureRecordKind.FAILURE_RECORD_KIND_RETRYABLE;
            case PERMANENT -> dev.sentinel.proto.v1.FailureRecordKind.FAILURE_RECORD_KIND_PERMANENT;
            case LEASE_EXPIRED ->
                    dev.sentinel.proto.v1.FailureRecordKind.FAILURE_RECORD_KIND_LEASE_EXPIRED;
        };
    }

    public static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    public static Instant toInstant(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}
