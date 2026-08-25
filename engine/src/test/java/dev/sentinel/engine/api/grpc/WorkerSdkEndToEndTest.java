package dev.sentinel.engine.api.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.sentinel.proto.v1.GetWorkflowStatusRequest;
import dev.sentinel.proto.v1.GetWorkflowStatusResponse;
import dev.sentinel.proto.v1.SubmitWorkflowRequest;
import dev.sentinel.proto.v1.TaskDefinition;
import dev.sentinel.proto.v1.TaskStatus;
import dev.sentinel.proto.v1.WorkflowStatus;
import dev.sentinel.worker.SentinelWorker;
import dev.sentinel.worker.TaskResult;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.grpc.client.GrpcChannelFactory;

/**
 * The whole system, end to end: a real engine on a real Postgres, and a real worker running real
 * handler code over real gRPC.
 *
 * <p>Everything here is covered in pieces by faster tests. What only this level can show is that a
 * developer following the documented SDK usage gets a workflow executed, which is the actual
 * promise the project makes.
 */
class WorkerSdkEndToEndTest extends AbstractGrpcIntegrationTest {

    /**
     * Each test uses its own routing key, and that is not tidiness.
     *
     * <p>Closing a worker interrupts whatever poll is in flight. The engine may already have
     * committed that claim, so a task can end up owned by a worker that never receives it and is
     * about to shut down. In production the reaper re-queues it once the lease expires; until that
     * lands, such a task simply stays RUNNING.
     *
     * <p>Sharing one routing key across tests therefore let a departing worker from an earlier test
     * silently claim the next test's work and strand it, which showed up as an unrelated test
     * timing out roughly one run in three. Distinct types also mirror reality, since a worker is
     * only ever offered the types it advertised.
     */
    private static final Duration PATIENCE = Duration.ofSeconds(30);

    @Autowired
    private GrpcChannelFactory channels;

    @Test
    @DisplayName("a worker runs a diamond workflow to completion, in dependency order")
    void aWorkerRunsADiamondToCompletion() {
        String taskType = "demo.diamond";
        Set<String> executed = ConcurrentHashMap.newKeySet();
        AtomicInteger joinRanAfter = new AtomicInteger();

        String workflowId = submitDiamond(taskType);

        try (SentinelWorker worker = SentinelWorker.builder()
                .channel(channels.createChannel("engine"))
                .workerId("e2e-worker")
                .concurrency(4)
                .pollWait(Duration.ofMillis(500))
                .heartbeatInterval(Duration.ofSeconds(2))
                .register(taskType, context -> {
                    if (context.taskName().equals("join")) {
                        // The join must not start until both branches are done. Capturing what had
                        // already run when it started is what proves the ordering, rather than
                        // inferring it from the final state.
                        joinRanAfter.set(executed.size());
                    }
                    executed.add(context.taskName());
                    return TaskResult.completed("{\"ran\":\"" + context.taskName() + "\"}");
                })
                .build()) {

            worker.start();

            awaitTerminal(workflowId, WorkflowStatus.WORKFLOW_STATUS_COMPLETED);
        }

        assertThat(executed).containsExactlyInAnyOrder("root", "left", "right", "join");
        assertThat(joinRanAfter.get())
                .as("the join depends on both branches, so all three must have finished before it started")
                .isEqualTo(3);

        GetWorkflowStatusResponse status = status(workflowId);
        assertThat(status.getTasksList()).extracting(TaskSummaryStatus::of).containsOnly(
                TaskStatus.TASK_STATUS_COMPLETED);
        assertThat(status.hasFinishedAt()).isTrue();
    }

    @Test
    @DisplayName("a handler that throws is retried, and succeeds on a later attempt")
    void aTransientHandlerFailureIsRetried() {
        String taskType = "demo.flaky";
        AtomicInteger attempts = new AtomicInteger();

        String workflowId = submit("flaky", List.of(
                TaskDefinition.newBuilder().setName("only").setTaskType(taskType).setInput("{}").build()));

        try (SentinelWorker worker = SentinelWorker.builder()
                .channel(channels.createChannel("engine"))
                .workerId("flaky-worker")
                .concurrency(1)
                .pollWait(Duration.ofMillis(500))
                .heartbeatInterval(Duration.ofSeconds(2))
                .register(taskType, context -> {
                    if (attempts.incrementAndGet() == 1) {
                        throw new IllegalStateException("downstream was briefly unavailable");
                    }
                    return TaskResult.completed("{\"recovered\":true}");
                })
                .build()) {

            worker.start();
            awaitTerminal(workflowId, WorkflowStatus.WORKFLOW_STATUS_COMPLETED);
        }

        assertThat(attempts.get())
                .as("the first attempt threw, so the engine must have handed the task out again")
                .isEqualTo(2);
    }

    @Test
    @DisplayName("a permanent handler failure fails the workflow and cancels what cannot run")
    void aPermanentHandlerFailureFailsTheWorkflow() {
        String taskType = "demo.doomed";
        String workflowId = submit("doomed", List.of(
                TaskDefinition.newBuilder().setName("first").setTaskType(taskType).setInput("{}").build(),
                TaskDefinition.newBuilder().setName("second").setTaskType(taskType).setInput("{}")
                        .addDependsOn("first").build()));

        try (SentinelWorker worker = SentinelWorker.builder()
                .channel(channels.createChannel("engine"))
                .workerId("doomed-worker")
                .concurrency(1)
                .pollWait(Duration.ofMillis(500))
                .heartbeatInterval(Duration.ofSeconds(2))
                .register(taskType, context -> TaskResult.permanentlyFailed("the input is not valid"))
                .build()) {

            worker.start();
            awaitTerminal(workflowId, WorkflowStatus.WORKFLOW_STATUS_FAILED);
        }

        GetWorkflowStatusResponse status = status(workflowId);
        assertThat(taskNamed(status, "first").getStatus()).isEqualTo(TaskStatus.TASK_STATUS_FAILED);
        assertThat(taskNamed(status, "first").getAttempt())
                .as("a permanent failure must not burn the whole attempt budget")
                .isEqualTo(1);
        assertThat(taskNamed(status, "second").getStatus())
                .as("it depended on a task that failed, so it can never run")
                .isEqualTo(TaskStatus.TASK_STATUS_CANCELLED);
    }

    // ------------------------------------------------------------------

    private String submitDiamond(String taskType) {
        return submit("diamond", List.of(
                TaskDefinition.newBuilder().setName("root").setTaskType(taskType).setInput("{}").build(),
                TaskDefinition.newBuilder().setName("left").setTaskType(taskType).setInput("{}")
                        .addDependsOn("root").build(),
                TaskDefinition.newBuilder().setName("right").setTaskType(taskType).setInput("{}")
                        .addDependsOn("root").build(),
                TaskDefinition.newBuilder().setName("join").setTaskType(taskType).setInput("{}")
                        .addDependsOn("left").addDependsOn("right").build()));
    }

    private String submit(String name, List<TaskDefinition> tasks) {
        return workflowStub.submitWorkflow(SubmitWorkflowRequest.newBuilder()
                .setName(name)
                .addAllTasks(tasks)
                .build())
                .getWorkflowId();
    }

    /**
     * Waits for a workflow to reach the expected terminal state, and says what it actually saw if
     * it does not.
     *
     * <p>A bare timeout here reports only "condition not fulfilled", which says nothing about
     * whether the workflow was stuck, partly done, or failed for an unrelated reason. Dumping the
     * task table turns a fifteen-minute investigation into a glance.
     */
    private void awaitTerminal(String workflowId, WorkflowStatus expected) {
        try {
            await().atMost(PATIENCE).until(() -> status(workflowId).getStatus() == expected);
        } catch (org.awaitility.core.ConditionTimeoutException e) {
            GetWorkflowStatusResponse actual = status(workflowId);
            StringBuilder detail = new StringBuilder("workflow %s never reached %s; it is %s%n"
                    .formatted(workflowId, expected, actual.getStatus()));
            actual.getTasksList().forEach(task -> detail.append("  %-8s %-24s attempt %d/%d %s%n"
                    .formatted(task.getName(), task.getStatus(), task.getAttempt(), task.getMaxAttempts(),
                            task.hasLastError() ? task.getLastError() : "")));
            throw new AssertionError(detail.toString(), e);
        }
    }

    private GetWorkflowStatusResponse status(String workflowId) {
        return workflowStub.getWorkflowStatus(
                GetWorkflowStatusRequest.newBuilder().setWorkflowId(workflowId).build());
    }

    private static dev.sentinel.proto.v1.TaskSummary taskNamed(GetWorkflowStatusResponse status, String name) {
        return status.getTasksList().stream()
                .filter(task -> task.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no task named " + name));
    }

    /** Extractor kept as a named type so the assertion above reads as a sentence. */
    private interface TaskSummaryStatus {
        static TaskStatus of(dev.sentinel.proto.v1.TaskSummary summary) {
            return summary.getStatus();
        }
    }
}
