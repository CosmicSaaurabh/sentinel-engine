package dev.sentinel.engine.api.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Duration;
import dev.sentinel.proto.v1.PollForTasksRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestPropertySource;

/**
 * Load shedding on the poll path, with the waiter limit lowered so saturation is reachable.
 *
 * <p>The property under test is the one that keeps long polling from becoming an unbounded queue.
 * Virtual threads make a parked poll cheap, and "cheap" is exactly the reasoning that leads to
 * accepting an unlimited number of them; an engine with fifty thousand parked waiters is one that
 * falls over in a way no metric predicted.
 *
 * <p>Shedding keeps the pressure visible. A refused worker backs off and may reach a different
 * instance; a silently parked one simply waits, and nothing anywhere says so.
 */
@TestPropertySource(properties = {
        "sentinel.grpc.max-waiting-polls=1",
        "sentinel.grpc.waiting-poll-retry-after=2s"
})
class PollCapacityTest extends AbstractGrpcIntegrationTest {

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
    @DisplayName("polls beyond the waiter limit are shed with RESOURCE_EXHAUSTED and a retry hint")
    void excessPollsAreShed() throws Exception {
        CompletableFuture<?> occupying = CompletableFuture.runAsync(
                () -> taskStub.pollForTasks(pollRequest("worker-1", 3)), background);

        // Let the first poll take the only waiter slot.
        Thread.sleep(300);

        StatusRuntimeException thrown = org.assertj.core.api.Assertions.catchThrowableOfType(
                StatusRuntimeException.class,
                () -> taskStub.pollForTasks(pollRequest("worker-2", 3)));

        assertThat(thrown).isNotNull();
        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
        assertThat(thrown.getTrailers()).isNotNull();
        String retryHint = thrown.getTrailers().get(GrpcErrors.RETRY_AFTER_MILLIS);
        assertThat(retryHint)
                .as("a shed worker needs to be told how long to wait, or it comes straight back")
                .isNotNull();
        assertThat(Long.parseLong(retryHint)).isBetween(1_000L, 3_000L);

        occupying.get(10, TimeUnit.SECONDS);
    }

    @Test
    @DisplayName("a waiter slot is released when its poll ends, so shedding is transient")
    void slotsAreReleasedAfterEachPoll() {
        taskStub.pollForTasks(pollRequest("worker-1", 0));

        List<?> tasks = taskStub.pollForTasks(pollRequest("worker-2", 0)).getTasksList();

        assertThat(tasks)
                .as("a leaked permit would make the engine refuse every poll forever after")
                .isEmpty();
    }

    private static PollForTasksRequest pollRequest(String workerId, int waitSeconds) {
        return PollForTasksRequest.newBuilder()
                .setWorkerId(workerId)
                .addTaskTypes("test.echo")
                .setBatchSize(1)
                .setMaxConcurrency(1)
                .setMaxWait(Duration.newBuilder().setSeconds(waitSeconds))
                .build();
    }
}
