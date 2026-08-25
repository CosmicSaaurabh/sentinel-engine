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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.grpc.client.GrpcChannelFactory;
import org.springframework.test.context.TestPropertySource;

/**
 * The headline promise of Phase 3: a worker dies mid-task and the work still gets done.
 *
 * <p>This is the scenario Phase 2 could not survive. Building the worker SDK produced a test that
 * failed roughly one run in three, with a task stuck {@code RUNNING} and no error anywhere, because
 * closing a worker interrupts its in-flight poll while the engine may already have committed the
 * claim. Nothing read the lease, so nothing ever recovered it.
 *
 * <p>The lease and reaper timings are compressed so recovery happens in seconds rather than the
 * production forty. Nothing else is faked: a real worker really stops reporting, a real reaper
 * notices, and a second real worker finishes the job.
 */
@TestPropertySource(properties = {
        "sentinel.claim.lease-duration=2s",
        "sentinel.reaper.enabled=true",
        "sentinel.reaper.interval=300ms",
        "sentinel.reaper.requeue-delay=0s"
})
class WorkerRecoveryTest extends AbstractGrpcIntegrationTest {

    private static final Duration PATIENCE = Duration.ofSeconds(30);

    @Autowired
    private GrpcChannelFactory channels;

    @Test
    @DisplayName("a worker that stops reporting has its task re-queued and finished by another worker")
    void aDeadWorkersTaskIsFinishedElsewhere() throws Exception {
        String taskType = "recovery.handoff";
        // Three attempts, so the lapsed lease is a recoverable failure rather than a terminal one.
        // With a budget of one, silence on the only attempt is the end of the task, which is the
        // scenario the next test covers.
        String workflowId = submit("handoff", taskType, 3);

        CountDownLatch firstWorkerStarted = new CountDownLatch(1);
        CountDownLatch neverReleased = new CountDownLatch(1);
        AtomicReference<String> finishedBy = new AtomicReference<>();
        AtomicInteger secondWorkerAttempt = new AtomicInteger();

        // A worker that claims the task and then stops making progress, which is what a hung
        // process, a suspended host and a partition all look like from the engine's side.
        SentinelWorker hung = SentinelWorker.builder()
                .channel(channels.createChannel("engine"))
                .workerId("hung-worker")
                .concurrency(1)
                .pollWait(Duration.ofMillis(300))
                .heartbeatInterval(Duration.ofMillis(500))
                // No drain: closing this worker is meant to model the process going away, not
                // shutting down politely.
                .drainTimeout(Duration.ZERO)
                .register(taskType, context -> {
                    firstWorkerStarted.countDown();
                    neverReleased.await();
                    return TaskResult.completed("{}");
                })
                .build();

        try (SentinelWorker healthy = SentinelWorker.builder()
                .channel(channels.createChannel("engine"))
                .workerId("healthy-worker")
                .concurrency(1)
                .pollWait(Duration.ofMillis(300))
                .heartbeatInterval(Duration.ofMillis(500))
                .register(taskType, context -> {
                    secondWorkerAttempt.set(context.attempt());
                    finishedBy.set("healthy-worker");
                    return TaskResult.completed("{\"recovered\":true}");
                })
                .build()) {

            hung.start();
            assertThat(firstWorkerStarted.await(10, TimeUnit.SECONDS))
                    .as("the first worker should have claimed the task")
                    .isTrue();

            // Its heartbeats stop here. From this moment the engine hears nothing about the task.
            hung.close();

            healthy.start();
            await().atMost(PATIENCE).until(() -> statusOf(workflowId) == WorkflowStatus.WORKFLOW_STATUS_COMPLETED);
        } finally {
            neverReleased.countDown();
        }

        assertThat(finishedBy.get()).isEqualTo("healthy-worker");
        assertThat(secondWorkerAttempt.get())
                .as("the failed attempt is counted, so a task that kills its worker cannot be "
                        + "retried forever")
                .isEqualTo(2);

        GetWorkflowStatusResponse status = status(workflowId);
        assertThat(status.getTasks(0).getStatus()).isEqualTo(TaskStatus.TASK_STATUS_COMPLETED);
    }

    @Test
    @DisplayName("a task whose worker dies on its final attempt fails rather than looping forever")
    void aTaskWithNoAttemptsLeftIsFailedByTheReaper() throws Exception {
        String taskType = "recovery.exhausted";
        String workflowId = submit("exhausted", taskType, 1);

        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch neverReleased = new CountDownLatch(1);

        SentinelWorker hung = SentinelWorker.builder()
                .channel(channels.createChannel("engine"))
                .workerId("hung-worker")
                .concurrency(1)
                .pollWait(Duration.ofMillis(300))
                .heartbeatInterval(Duration.ofMillis(500))
                .drainTimeout(Duration.ZERO)
                .register(taskType, context -> {
                    started.countDown();
                    neverReleased.await();
                    return TaskResult.completed("{}");
                })
                .build();

        try {
            hung.start();
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            hung.close();

            await().atMost(PATIENCE).until(() -> statusOf(workflowId) == WorkflowStatus.WORKFLOW_STATUS_FAILED);
        } finally {
            neverReleased.countDown();
        }

        GetWorkflowStatusResponse status = status(workflowId);
        assertThat(status.getTasks(0).getStatus())
                .as("with a single attempt permitted, silence on that attempt is terminal")
                .isEqualTo(TaskStatus.TASK_STATUS_FAILED);

        long recorded = jdbcClient.sql(
                "SELECT count(*) FROM task_failures WHERE failure_kind = 'LEASE_EXPIRED'")
                .query(Long.class)
                .single();
        assertThat(recorded)
                .as("a lapsed lease is recorded differently from a reported failure, because it "
                        + "points at worker or network health rather than at the task")
                .isEqualTo(1);
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
