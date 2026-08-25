package dev.sentinel.engine.api.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Duration;
import dev.sentinel.proto.v1.PollForTasksRequest;
import dev.sentinel.proto.v1.PollForTasksResponse;
import dev.sentinel.proto.v1.SubmitWorkflowRequest;
import dev.sentinel.proto.v1.TaskDefinition;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The long poll's actual behaviour.
 *
 * <p>These are the tests that distinguish this design from a plain poll, and each one pins a
 * property that a well-meaning refactor could quietly remove.
 *
 * <p>Timing assertions are deliberately loose. Their job is to tell "waited" apart from "returned
 * straight away" and "woke up promptly" apart from "waited out the full window", which are order-of-
 * magnitude differences. Tight bounds here would buy nothing and would fail on a loaded CI machine.
 */
class LongPollTest extends AbstractGrpcIntegrationTest {

    private static final String TASK_TYPE = "test.echo";

    private final ExecutorService background = Executors.newFixedThreadPool(2, runnable -> {
        Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        return thread;
    });

    @AfterEach
    void shutdownBackground() throws InterruptedException {
        background.shutdownNow();
        assertThat(background.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    @DisplayName("an empty queue holds the request open rather than answering immediately")
    void anEmptyQueueWaits() {
        long startedAt = System.nanoTime();

        PollForTasksResponse response = poll(java.time.Duration.ofMillis(800));

        java.time.Duration elapsed = java.time.Duration.ofNanos(System.nanoTime() - startedAt);
        assertThat(response.getTasksList()).isEmpty();
        assertThat(elapsed)
                .as("returning straight away is what makes a plain poll need a short client interval, "
                        + "and a short client interval is what floods the database from an idle fleet")
                .isGreaterThan(java.time.Duration.ofMillis(500));
    }

    @Test
    @DisplayName("a waiting poll returns as soon as work appears, not when its window ends")
    void aWaitingPollWakesEarly() throws Exception {
        CompletableFuture<PollForTasksResponse> waiting =
                CompletableFuture.supplyAsync(() -> poll(java.time.Duration.ofSeconds(10)), background);

        // Let the poll settle into waiting before any work exists.
        Thread.sleep(300);
        long submittedAt = System.nanoTime();
        submitOneTask();

        PollForTasksResponse response = waiting.get(8, TimeUnit.SECONDS);
        java.time.Duration afterSubmission = java.time.Duration.ofNanos(System.nanoTime() - submittedAt);

        assertThat(response.getTasksList()).hasSize(1);
        assertThat(afterSubmission)
                .as("dispatch latency is the engine's retry interval, not the client's poll interval")
                .isLessThan(java.time.Duration.ofSeconds(3));
    }

    @Test
    @DisplayName("the call's deadline cuts the wait short, so the engine never queries for a caller that has gone")
    void theDeadlineBoundsTheWait() {
        long startedAt = System.nanoTime();

        PollForTasksResponse response = taskStub
                .withDeadlineAfter(700, TimeUnit.MILLISECONDS)
                .pollForTasks(pollRequest(java.time.Duration.ofSeconds(30)));

        java.time.Duration elapsed = java.time.Duration.ofNanos(System.nanoTime() - startedAt);
        assertThat(response.getTasksList()).isEmpty();
        assertThat(elapsed)
                .as("the engine must answer inside the deadline rather than let the call be cancelled, "
                        + "since a cancelled call is indistinguishable from a real failure to the client")
                .isLessThan(java.time.Duration.ofSeconds(2));
    }

    @Test
    @DisplayName("a zero wait behaves as a plain poll, for a client that wants to control its own cadence")
    void aZeroWaitReturnsImmediately() {
        long startedAt = System.nanoTime();

        PollForTasksResponse response = poll(java.time.Duration.ZERO);

        assertThat(response.getTasksList()).isEmpty();
        assertThat(java.time.Duration.ofNanos(System.nanoTime() - startedAt))
                .isLessThan(java.time.Duration.ofMillis(500));
    }

    @Test
    @DisplayName("a worker declaring no task types is answered immediately rather than parked")
    void aWorkerWithNoTaskTypesIsNotParked() {
        long startedAt = System.nanoTime();

        PollForTasksResponse response = taskStub.pollForTasks(PollForTasksRequest.newBuilder()
                .setWorkerId("typeless")
                .setBatchSize(1)
                .setMaxWait(Duration.newBuilder().setSeconds(5))
                .build());

        assertThat(response.getTasksList()).isEmpty();
        assertThat(java.time.Duration.ofNanos(System.nanoTime() - startedAt))
                .as("parking a worker that can run nothing would occupy a waiter slot to no purpose")
                .isLessThan(java.time.Duration.ofSeconds(2));
    }

    // ------------------------------------------------------------------

    private PollForTasksResponse poll(java.time.Duration maxWait) {
        return taskStub.pollForTasks(pollRequest(maxWait));
    }

    private static PollForTasksRequest pollRequest(java.time.Duration maxWait) {
        return PollForTasksRequest.newBuilder()
                .setWorkerId("worker-1")
                .addTaskTypes(TASK_TYPE)
                .setBatchSize(1)
                .setMaxConcurrency(1)
                .setMaxWait(Duration.newBuilder()
                        .setSeconds(maxWait.getSeconds())
                        .setNanos(maxWait.getNano()))
                .build();
    }

    private void submitOneTask() {
        workflowStub.submitWorkflow(SubmitWorkflowRequest.newBuilder()
                .setName("late-arrival")
                .addTasks(TaskDefinition.newBuilder()
                        .setName("only")
                        .setTaskType(TASK_TYPE)
                        .setInput("{}"))
                .build());
    }
}
