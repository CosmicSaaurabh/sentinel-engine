package dev.sentinel.engine.api.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.awaitility.Awaitility.await;

import dev.sentinel.proto.v1.FailureKind;
import dev.sentinel.proto.v1.FailureRecordKind;
import dev.sentinel.proto.v1.GetTaskFailuresRequest;
import dev.sentinel.proto.v1.GetTaskFailuresResponse;
import dev.sentinel.proto.v1.GetWorkflowStatusRequest;
import dev.sentinel.proto.v1.GetWorkflowStatusResponse;
import dev.sentinel.proto.v1.ListDeadLetterTasksRequest;
import dev.sentinel.proto.v1.ListDeadLetterTasksResponse;
import dev.sentinel.proto.v1.SubmitWorkflowRequest;
import dev.sentinel.proto.v1.TaskDefinition;
import dev.sentinel.proto.v1.WorkflowStatus;
import dev.sentinel.worker.SentinelWorker;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.grpc.client.GrpcChannelFactory;
import org.springframework.test.context.TestPropertySource;

/**
 * The operator-facing failure surface, over the wire.
 *
 * <p>The interesting case is driven by a real worker whose handler throws, because that is how a
 * failure normally reaches the engine: nobody writes {@code TaskResult.failed} by hand for a network
 * timeout, they let the exception out and the SDK classifies it.
 */
@TestPropertySource(properties = {
        "sentinel.retry.base-delay=1ms",
        "sentinel.retry.max-delay=10ms"
})
class FailureQueryGrpcTest extends AbstractGrpcIntegrationTest {

    private static final Duration PATIENCE = Duration.ofSeconds(30);

    @Autowired
    private GrpcChannelFactory channels;

    @Test
    @DisplayName("a handler that throws leaves a cause chain naming the exception type per attempt")
    void athrowingHandlerLeavesACauseChain() {
        String taskType = "failing.step";
        String workflowId = submit("doomed", taskType, 3);

        try (SentinelWorker worker = SentinelWorker.builder()
                .channel(channels.createChannel("engine"))
                .workerId("failing-worker")
                .concurrency(1)
                .pollWait(Duration.ofMillis(300))
                .heartbeatInterval(Duration.ofSeconds(2))
                .register(taskType, context -> {
                    throw new java.net.SocketTimeoutException("the downstream never answered");
                })
                .build()) {

            worker.start();
            await().atMost(PATIENCE).until(() -> statusOf(workflowId) == WorkflowStatus.WORKFLOW_STATUS_FAILED);
        }

        String taskId = status(workflowId).getTasks(0).getTaskId();
        GetTaskFailuresResponse chain = workflowStub.getTaskFailures(
                GetTaskFailuresRequest.newBuilder().setTaskId(taskId).build());

        assertThat(chain.getFailuresList()).hasSize(3);
        assertThat(chain.getFailuresList()).extracting(record -> record.getAttempt())
                .containsExactly(1, 2, 3);
        assertThat(chain.getFailures(0).getErrorClass())
                .as("the exception type travels as its own field, so counting a failure mode is a "
                        + "query rather than a text search over messages")
                .isEqualTo("java.net.SocketTimeoutException");
        assertThat(chain.getFailures(0).getErrorMessage()).isEqualTo("the downstream never answered");
        assertThat(chain.getFailures(0).getKind()).isEqualTo(FailureRecordKind.FAILURE_RECORD_KIND_RETRYABLE);
        assertThat(chain.getFailures(0).getWorkerId()).isEqualTo("failing-worker");
        assertThat(chain.getFailures(0).hasFailedAt()).isTrue();
    }

    @Test
    @DisplayName("the failed task appears in the dead-letter listing with its failure count")
    void theFailedTaskIsDeadLettered() {
        String taskType = "dead.step";
        String workflowId = submit("dead", taskType, 2);

        try (SentinelWorker worker = SentinelWorker.builder()
                .channel(channels.createChannel("engine"))
                .workerId("failing-worker")
                .concurrency(1)
                .pollWait(Duration.ofMillis(300))
                .heartbeatInterval(Duration.ofSeconds(2))
                .register(taskType, context -> {
                    throw new IllegalStateException("nope");
                })
                .build()) {

            worker.start();
            await().atMost(PATIENCE).until(() -> statusOf(workflowId) == WorkflowStatus.WORKFLOW_STATUS_FAILED);
        }

        ListDeadLetterTasksResponse deadLetters = workflowStub.listDeadLetterTasks(
                ListDeadLetterTasksRequest.newBuilder().setWorkflowId(workflowId).setLimit(10).build());

        assertThat(deadLetters.getTasksList()).hasSize(1);
        var entry = deadLetters.getTasks(0);
        assertThat(entry.getTaskName()).isEqualTo("only");
        assertThat(entry.getAttempt()).isEqualTo(2);
        assertThat(entry.getMaxAttempts()).isEqualTo(2);
        assertThat(entry.getRecordedFailures()).isEqualTo(2);
        assertThat(entry.getLastErrorKind()).isEqualTo(FailureKind.FAILURE_KIND_RETRYABLE);
        assertThat(entry.getWorkflowName()).isEqualTo("dead");
    }

    @Test
    @DisplayName("a task that never failed has an empty chain rather than an error")
    void aHealthyTaskHasAnEmptyChain() {
        String workflowId = submit("healthy", "never.run", 3);
        String taskId = status(workflowId).getTasks(0).getTaskId();

        GetTaskFailuresResponse chain = workflowStub.getTaskFailures(
                GetTaskFailuresRequest.newBuilder().setTaskId(taskId).build());

        assertThat(chain.getFailuresList()).isEmpty();
    }

    @Test
    @DisplayName("an unknown task id is NOT_FOUND, not an empty chain")
    void anUnknownTaskIsNotFound() {
        StatusRuntimeException thrown = catchThrowableOfType(StatusRuntimeException.class,
                () -> workflowStub.getTaskFailures(GetTaskFailuresRequest.newBuilder()
                        .setTaskId(UUID.randomUUID().toString())
                        .build()));

        assertThat(thrown.getStatus().getCode())
                .as("an empty chain would be indistinguishable from a wrong id, which sends an "
                        + "operator looking in the wrong place")
                .isEqualTo(Status.Code.NOT_FOUND);
    }

    // ------------------------------------------------------------------

    private String submit(String name, String taskType, int maxAttempts) {
        return workflowStub.submitWorkflow(SubmitWorkflowRequest.newBuilder()
                .setName(name)
                .addAllTasks(List.of(TaskDefinition.newBuilder()
                        .setName("only")
                        .setTaskType(taskType)
                        .setInput("{}")
                        .setMaxAttempts(maxAttempts)
                        .build()))
                .build())
                .getWorkflowId();
    }

    private WorkflowStatus statusOf(String workflowId) {
        return status(workflowId).getStatus();
    }

    private GetWorkflowStatusResponse status(String workflowId) {
        return workflowStub.getWorkflowStatus(
                GetWorkflowStatusRequest.newBuilder().setWorkflowId(workflowId).build());
    }
}
