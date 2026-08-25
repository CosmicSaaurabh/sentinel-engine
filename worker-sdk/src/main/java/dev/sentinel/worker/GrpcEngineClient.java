package dev.sentinel.worker;

import dev.sentinel.proto.v1.CompleteTaskRequest;
import dev.sentinel.proto.v1.FailTaskRequest;
import dev.sentinel.proto.v1.FailureKind;
import dev.sentinel.proto.v1.HeartbeatRequest;
import dev.sentinel.proto.v1.HeartbeatResponse;
import dev.sentinel.proto.v1.PollForTasksRequest;
import dev.sentinel.proto.v1.PollForTasksResponse;
import dev.sentinel.proto.v1.TaskLease;
import dev.sentinel.proto.v1.TaskServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * The gRPC implementation of {@link EngineClient}.
 *
 * <p>Two responsibilities, and nothing else: convert between the SDK's types and the wire types,
 * and turn gRPC statuses into the SDK's exceptions. It holds no state about tasks and makes no
 * decisions about retries.
 *
 * <h2>Deadlines</h2>
 *
 * <p>Every call carries one, and the heartbeat's is deliberately much shorter than the others. A
 * heartbeat exists to say "still alive" before the lease expires, so a heartbeat that hangs for
 * longer than the lease is worse than useless: the worker would keep working, believing itself
 * healthy, while the engine gave its task away. Failing fast lets the worker notice and react.
 *
 * <p>The poll's deadline is its wait plus a margin. The engine already bounds its own wait and
 * answers before the deadline; the margin exists so an answer in flight is not cancelled at the
 * finish line, since a cancelled call is indistinguishable from a real failure.
 */
final class GrpcEngineClient implements EngineClient {

    private static final Metadata.Key<String> RETRY_AFTER_MILLIS =
            Metadata.Key.of("retry-after-millis", Metadata.ASCII_STRING_MARSHALLER);

    private static final Duration HEARTBEAT_DEADLINE = Duration.ofSeconds(5);
    private static final Duration REPORT_DEADLINE = Duration.ofSeconds(10);
    private static final Duration POLL_DEADLINE_MARGIN = Duration.ofSeconds(5);
    private static final Duration DEFAULT_RETRY_AFTER = Duration.ofSeconds(1);

    private final ManagedChannel channel;
    private final TaskServiceGrpc.TaskServiceBlockingStub stub;
    private final String workerId;

    GrpcEngineClient(ManagedChannel channel, String workerId) {
        this.channel = channel;
        this.stub = TaskServiceGrpc.newBlockingStub(channel);
        this.workerId = workerId;
    }

    @Override
    public List<EngineClient.AssignedTask> poll(
            List<String> taskTypes, int batchSize, Duration maxWait, int maxConcurrency) {

        PollForTasksRequest request = PollForTasksRequest.newBuilder()
                .setWorkerId(workerId)
                .addAllTaskTypes(taskTypes)
                .setBatchSize(batchSize)
                .setMaxConcurrency(maxConcurrency)
                .setMaxWait(com.google.protobuf.Duration.newBuilder()
                        .setSeconds(maxWait.getSeconds())
                        .setNanos(maxWait.getNano()))
                .build();

        Duration deadline = maxWait.plus(POLL_DEADLINE_MARGIN);
        try {
            PollForTasksResponse response = stub
                    .withDeadlineAfter(deadline.toMillis(), TimeUnit.MILLISECONDS)
                    .pollForTasks(request);
            return response.getTasksList().stream().map(GrpcEngineClient::toAssignedTask).toList();
        } catch (StatusRuntimeException e) {
            throw translate(e, null);
        }
    }

    @Override
    public HeartbeatOutcome heartbeat(Map<UUID, Long> fencingTokensByTaskId) {
        HeartbeatRequest.Builder request = HeartbeatRequest.newBuilder().setWorkerId(workerId);
        fencingTokensByTaskId.forEach((taskId, fencingToken) -> request.addLeases(TaskLease.newBuilder()
                .setTaskId(taskId.toString())
                .setFencingToken(fencingToken)
                .build()));

        try {
            HeartbeatResponse response = stub
                    .withDeadlineAfter(HEARTBEAT_DEADLINE.toMillis(), TimeUnit.MILLISECONDS)
                    .heartbeat(request.build());
            return new HeartbeatOutcome(
                    response.getRenewedTaskIdsList().stream().map(UUID::fromString).toList(),
                    response.getLostTaskIdsList().stream().map(UUID::fromString).toList());
        } catch (StatusRuntimeException e) {
            throw translate(e, null);
        }
    }

    @Override
    public void complete(UUID taskId, long fencingToken, String output) {
        try {
            stub.withDeadlineAfter(REPORT_DEADLINE.toMillis(), TimeUnit.MILLISECONDS)
                    .completeTask(CompleteTaskRequest.newBuilder()
                            .setTaskId(taskId.toString())
                            .setWorkerId(workerId)
                            .setFencingToken(fencingToken)
                            .setOutput(output)
                            .build());
        } catch (StatusRuntimeException e) {
            throw translate(e, taskId);
        }
    }

    @Override
    public void fail(UUID taskId, long fencingToken, String message, boolean permanent, String errorClass) {
        FailTaskRequest.Builder request = FailTaskRequest.newBuilder()
                .setTaskId(taskId.toString())
                .setWorkerId(workerId)
                .setFencingToken(fencingToken)
                .setKind(permanent
                        ? FailureKind.FAILURE_KIND_PERMANENT
                        : FailureKind.FAILURE_KIND_RETRYABLE)
                .setMessage(message);
        if (errorClass != null) {
            request.setErrorClass(errorClass);
        }
        try {
            stub.withDeadlineAfter(REPORT_DEADLINE.toMillis(), TimeUnit.MILLISECONDS)
                    .failTask(request.build());
        } catch (StatusRuntimeException e) {
            throw translate(e, taskId);
        }
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Turns a gRPC status into the SDK's vocabulary.
     *
     * <p>The mapping is what the SDK's retry behaviour is built on, so each case is a decision
     * rather than a translation.
     */
    private static RuntimeException translate(StatusRuntimeException e, UUID taskId) {
        Status.Code code = e.getStatus().getCode();
        return switch (code) {
            // The engine could not reach its database, or we could not reach the engine. It is
            // declining to guess rather than failing, so backing off and retrying is right.
            case UNAVAILABLE, DEADLINE_EXCEEDED -> new EngineUnavailableException(
                    "engine unavailable: " + e.getStatus(), e);

            // The engine is shedding load and told us when to come back. Honour the hint.
            case RESOURCE_EXHAUSTED -> new EngineOverloadedException(
                    "engine is shedding load: " + e.getStatus().getDescription(), retryAfter(e));

            // We no longer own this task. Final, not transient.
            case FAILED_PRECONDITION -> new LeaseLostException(taskId,
                    "lease lost: " + e.getStatus().getDescription());

            default -> e;
        };
    }

    private static Duration retryAfter(StatusRuntimeException e) {
        Metadata trailers = e.getTrailers();
        String hint = trailers == null ? null : trailers.get(RETRY_AFTER_MILLIS);
        if (hint == null) {
            return DEFAULT_RETRY_AFTER;
        }
        try {
            return Duration.ofMillis(Long.parseLong(hint));
        } catch (NumberFormatException ignored) {
            // A malformed hint is the engine's problem, not a reason to fail the worker.
            return DEFAULT_RETRY_AFTER;
        }
    }

    /**
     * Note the fully qualified parameter type. This class implements {@link EngineClient}, which
     * nests its own {@code AssignedTask}, and an inherited member type shadows a same-named import.
     * Spelling the wire type out is what keeps the two apart.
     */
    private static EngineClient.AssignedTask toAssignedTask(dev.sentinel.proto.v1.AssignedTask task) {
        return new EngineClient.AssignedTask(
                UUID.fromString(task.getTaskId()),
                UUID.fromString(task.getWorkflowId()),
                task.getName(),
                task.getTaskType(),
                task.getInput(),
                task.getAttempt(),
                task.getMaxAttempts(),
                task.getFencingToken(),
                task.hasTraceContext() ? task.getTraceContext() : null);
    }
}
