package dev.sentinel.engine.api.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import com.google.protobuf.Duration;
import dev.sentinel.proto.v1.AssignedTask;
import dev.sentinel.proto.v1.CompleteTaskRequest;
import dev.sentinel.proto.v1.FailTaskRequest;
import dev.sentinel.proto.v1.FailureKind;
import dev.sentinel.proto.v1.GetWorkflowStatusRequest;
import dev.sentinel.proto.v1.GetWorkflowStatusResponse;
import dev.sentinel.proto.v1.HeartbeatRequest;
import dev.sentinel.proto.v1.HeartbeatResponse;
import dev.sentinel.proto.v1.PollForTasksRequest;
import dev.sentinel.proto.v1.PollForTasksResponse;
import dev.sentinel.proto.v1.ReportResponse;
import dev.sentinel.proto.v1.SubmitWorkflowRequest;
import dev.sentinel.proto.v1.SubmitWorkflowResponse;
import dev.sentinel.proto.v1.TaskDefinition;
import dev.sentinel.proto.v1.TaskLease;
import dev.sentinel.proto.v1.TaskStatus;
import dev.sentinel.proto.v1.WorkflowStatus;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A workflow's whole life, driven over real gRPC against a real Postgres.
 *
 * <p>Everything below this test is already covered by faster tests. What only this level can prove
 * is that the pieces are wired together: that the proto contract matches the domain, that the
 * exception mapping produces the statuses a client will actually branch on, and that a worker
 * following the documented protocol gets a workflow to completion.
 */
class GrpcLifecycleTest extends AbstractGrpcIntegrationTest {

    private static final String WORKER = "worker-1";
    private static final String TASK_TYPE = "test.echo";

    @Test
    @DisplayName("submit, poll, heartbeat, complete, and query: a chain runs to completion over the wire")
    void aWorkflowRunsToCompletionOverGrpc() {
        String workflowId = submitChain("first", "second").getWorkflowId();

        for (String expected : List.of("first", "second")) {
            List<AssignedTask> assigned = poll(1).getTasksList();
            assertThat(assigned).hasSize(1);
            AssignedTask task = assigned.getFirst();
            assertThat(task.getName()).isEqualTo(expected);
            assertThat(task.getFencingToken()).isPositive();
            assertThat(task.hasLeaseExpiresAt()).isTrue();

            HeartbeatResponse beat = taskStub.heartbeat(HeartbeatRequest.newBuilder()
                    .setWorkerId(WORKER)
                    .addLeases(leaseOf(task))
                    .build());
            assertThat(beat.getRenewedTaskIdsList()).containsExactly(task.getTaskId());
            assertThat(beat.getLostTaskIdsList()).isEmpty();

            ReportResponse report = complete(task, "{\"ok\":true}");
            assertThat(report.getAlreadyRecorded()).isFalse();
        }

        GetWorkflowStatusResponse status = status(workflowId);
        assertThat(status.getStatus()).isEqualTo(WorkflowStatus.WORKFLOW_STATUS_COMPLETED);
        assertThat(status.hasFinishedAt()).isTrue();
        assertThat(status.getTasksList()).extracting(task -> task.getStatus())
                .containsOnly(TaskStatus.TASK_STATUS_COMPLETED);
    }

    @Test
    @DisplayName("a batched heartbeat renews every lease a worker holds in one call")
    void heartbeatIsBatched() {
        submitParallel("a", "b", "c");
        List<AssignedTask> assigned = poll(10).getTasksList();
        assertThat(assigned).hasSize(3);

        HeartbeatRequest.Builder request = HeartbeatRequest.newBuilder().setWorkerId(WORKER);
        assigned.forEach(task -> request.addLeases(leaseOf(task)));

        HeartbeatResponse response = taskStub.heartbeat(request.build());

        assertThat(response.getRenewedTaskIdsList())
                .as("a worker running three tasks should renew three leases in one round trip")
                .hasSize(3);
    }

    @Test
    @DisplayName("a heartbeat reports lost leases per task rather than failing the whole call")
    void heartbeatReportsPartialLoss() {
        submitParallel("a", "b");
        List<AssignedTask> assigned = poll(10).getTasksList();
        AssignedTask stolen = assigned.getFirst();
        AssignedTask healthy = assigned.get(1);
        loseLease(stolen);

        HeartbeatResponse response = taskStub.heartbeat(HeartbeatRequest.newBuilder()
                .setWorkerId(WORKER)
                .addLeases(leaseOf(stolen))
                .addLeases(leaseOf(healthy))
                .build());

        assertThat(response.getLostTaskIdsList()).containsExactly(stolen.getTaskId());
        assertThat(response.getRenewedTaskIdsList())
                .as("failing the call would hide the fate of the healthy lease behind the dead one")
                .containsExactly(healthy.getTaskId());
    }

    @Test
    @DisplayName("a worker that lost its lease is refused with FAILED_PRECONDITION")
    void fencedWorkerGetsFailedPrecondition() {
        submitChain("only");
        AssignedTask task = poll(1).getTasks(0);
        loseLease(task);

        StatusRuntimeException thrown = catchThrowableOfType(
                StatusRuntimeException.class, () -> complete(task, "{}"));

        assertThat(thrown.getStatus().getCode())
                .as("the client must read this as non-retryable: another worker already owns the task")
                .isEqualTo(Status.Code.FAILED_PRECONDITION);
    }

    @Test
    @DisplayName("a duplicate completion is a success flagged as already recorded")
    void duplicateCompletionIsIdempotent() {
        submitChain("only");
        AssignedTask task = poll(1).getTasks(0);

        assertThat(complete(task, "{}").getAlreadyRecorded()).isFalse();
        assertThat(complete(task, "{}").getAlreadyRecorded())
                .as("a response lost on the way back makes an honest worker report twice")
                .isTrue();
    }

    @Test
    @DisplayName("failing a task permanently fails its workflow and cancels what cannot run")
    void permanentFailureFailsTheWorkflow() {
        String workflowId = submitChain("first", "second").getWorkflowId();
        AssignedTask task = poll(1).getTasks(0);

        taskStub.failTask(FailTaskRequest.newBuilder()
                .setTaskId(task.getTaskId())
                .setWorkerId(WORKER)
                .setFencingToken(task.getFencingToken())
                .setKind(FailureKind.FAILURE_KIND_PERMANENT)
                .setMessage("payload rejected")
                .build());

        GetWorkflowStatusResponse status = status(workflowId);
        assertThat(status.getStatus()).isEqualTo(WorkflowStatus.WORKFLOW_STATUS_FAILED);
        assertThat(taskNamed(status, "first").getStatus()).isEqualTo(TaskStatus.TASK_STATUS_FAILED);
        assertThat(taskNamed(status, "first").getLastErrorKind()).isEqualTo(FailureKind.FAILURE_KIND_PERMANENT);
        assertThat(taskNamed(status, "second").getStatus()).isEqualTo(TaskStatus.TASK_STATUS_CANCELLED);
    }

    @Test
    @DisplayName("resubmitting an idempotency key returns the same workflow, flagged as deduplicated")
    void submissionIsIdempotent() {
        SubmitWorkflowRequest request = SubmitWorkflowRequest.newBuilder()
                .setName("billing")
                .setSubmissionKey("invoice-2026-08")
                .addTasks(TaskDefinition.newBuilder().setName("charge").setTaskType(TASK_TYPE).setInput("{}"))
                .build();

        SubmitWorkflowResponse first = workflowStub.submitWorkflow(request);
        SubmitWorkflowResponse second = workflowStub.submitWorkflow(request);

        assertThat(first.getDeduplicated()).isFalse();
        assertThat(second.getDeduplicated()).isTrue();
        assertThat(second.getWorkflowId()).isEqualTo(first.getWorkflowId());
    }

    // ------------------------------------------------------------------
    // Error mapping
    // ------------------------------------------------------------------

    @Test
    @DisplayName("a cyclic DAG comes back as INVALID_ARGUMENT naming the cycle")
    void cyclicDagIsInvalidArgument() {
        SubmitWorkflowRequest request = SubmitWorkflowRequest.newBuilder()
                .setName("loop")
                .addTasks(TaskDefinition.newBuilder().setName("a").setTaskType(TASK_TYPE).addDependsOn("b"))
                .addTasks(TaskDefinition.newBuilder().setName("b").setTaskType(TASK_TYPE).addDependsOn("a"))
                .build();

        StatusRuntimeException thrown = catchThrowableOfType(
                StatusRuntimeException.class, () -> workflowStub.submitWorkflow(request));

        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(thrown.getStatus().getDescription()).contains("cycle");
    }

    @Test
    @DisplayName("an unknown workflow comes back as NOT_FOUND")
    void unknownWorkflowIsNotFound() {
        StatusRuntimeException thrown = catchThrowableOfType(
                StatusRuntimeException.class, () -> status(UUID.randomUUID().toString()));

        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    }

    @Test
    @DisplayName("a malformed id is rejected as INVALID_ARGUMENT rather than reported as missing")
    void malformedIdIsInvalidArgument() {
        StatusRuntimeException thrown = catchThrowableOfType(
                StatusRuntimeException.class, () -> status("not-a-uuid"));

        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    }

    @Test
    @DisplayName("an oversized payload is refused at the boundary, not at the database")
    void oversizedPayloadIsRefused() {
        String huge = "x".repeat(300_000);
        SubmitWorkflowRequest request = SubmitWorkflowRequest.newBuilder()
                .setName("bulky")
                .addTasks(TaskDefinition.newBuilder()
                        .setName("big")
                        .setTaskType(TASK_TYPE)
                        .setInput("{\"blob\":\"" + huge + "\"}"))
                .build();

        StatusRuntimeException thrown = catchThrowableOfType(
                StatusRuntimeException.class, () -> workflowStub.submitWorkflow(request));

        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(thrown.getStatus().getDescription()).contains("reference to external storage");
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private SubmitWorkflowResponse submitChain(String... names) {
        SubmitWorkflowRequest.Builder request = SubmitWorkflowRequest.newBuilder().setName("chain");
        for (int i = 0; i < names.length; i++) {
            TaskDefinition.Builder task = TaskDefinition.newBuilder()
                    .setName(names[i])
                    .setTaskType(TASK_TYPE)
                    .setInput("{}");
            if (i > 0) {
                task.addDependsOn(names[i - 1]);
            }
            request.addTasks(task);
        }
        return workflowStub.submitWorkflow(request.build());
    }

    private void submitParallel(String... names) {
        SubmitWorkflowRequest.Builder request = SubmitWorkflowRequest.newBuilder().setName("parallel");
        for (String name : names) {
            request.addTasks(TaskDefinition.newBuilder().setName(name).setTaskType(TASK_TYPE).setInput("{}"));
        }
        workflowStub.submitWorkflow(request.build());
    }

    private PollForTasksResponse poll(int batchSize) {
        return taskStub.pollForTasks(PollForTasksRequest.newBuilder()
                .setWorkerId(WORKER)
                .addTaskTypes(TASK_TYPE)
                .setBatchSize(batchSize)
                .setMaxConcurrency(batchSize)
                .setMaxWait(Duration.newBuilder().setSeconds(2))
                .build());
    }

    private ReportResponse complete(AssignedTask task, String output) {
        return taskStub.completeTask(CompleteTaskRequest.newBuilder()
                .setTaskId(task.getTaskId())
                .setWorkerId(WORKER)
                .setFencingToken(task.getFencingToken())
                .setOutput(output)
                .build());
    }

    private GetWorkflowStatusResponse status(String workflowId) {
        return workflowStub.getWorkflowStatus(
                GetWorkflowStatusRequest.newBuilder().setWorkflowId(workflowId).build());
    }

    private static TaskLease leaseOf(AssignedTask task) {
        return TaskLease.newBuilder()
                .setTaskId(task.getTaskId())
                .setFencingToken(task.getFencingToken())
                .build();
    }

    /**
     * Simulates the task being reaped and taken by someone else, without waiting out a lease.
     *
     * <p>Bumping the fencing token is exactly what a re-claim does, and the token is the only thing
     * ownership is checked against, so this reproduces the condition rather than approximating it.
     */
    private void loseLease(AssignedTask task) {
        jdbcClient.sql("UPDATE tasks SET fencing_token = fencing_token + 1 WHERE id = CAST(:id AS uuid)")
                .param("id", task.getTaskId())
                .update();
    }

    private static dev.sentinel.proto.v1.TaskSummary taskNamed(GetWorkflowStatusResponse status, String name) {
        return status.getTasksList().stream()
                .filter(task -> task.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no task named " + name));
    }
}
