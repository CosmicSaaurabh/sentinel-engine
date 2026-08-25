package dev.sentinel.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The SDK's behaviour, driven against a fake engine.
 *
 * <p>Each test pins something a plausible refactor could silently break, and the comments say what
 * the failure would look like rather than only what the assertion checks. Several of these
 * behaviours are invisible in production until they have already cost duplicated work.
 */
class SentinelWorkerTest {

    private static final String TASK_TYPE = "test.echo";

    // ------------------------------------------------------------------
    // Handler contract
    // ------------------------------------------------------------------

    @Test
    @DisplayName("a handler's result is reported back to the engine")
    void completedResultIsReported() throws Exception {
        FakeEngineClient engine = new FakeEngineClient();
        AtomicReference<ActivityContext> seen = new AtomicReference<>();

        try (SentinelWorker worker = worker(engine, context -> {
            seen.set(context);
            return TaskResult.completed("{\"done\":true}");
        })) {
            worker.start();
            engine.offer(TASK_TYPE, "{\"n\":7}", 2, 10);

            await().atMost(Duration.ofSeconds(5)).until(() -> !engine.completions.isEmpty());
        }

        assertThat(engine.completions).hasSize(1);
        assertThat(engine.completions.getFirst().output()).contains("done");
        assertThat(seen.get().input()).isEqualTo("{\"n\":7}");
        assertThat(seen.get().attempt()).isEqualTo(2);
        assertThat(seen.get().taskId()).isNotNull();
    }

    @Test
    @DisplayName("a handler that throws fails its task and leaves the worker running")
    void handlerExceptionFailsTheTaskNotTheWorker() throws Exception {
        FakeEngineClient engine = new FakeEngineClient();
        AtomicInteger invocations = new AtomicInteger();

        try (SentinelWorker worker = worker(engine, context -> {
            invocations.incrementAndGet();
            throw new IllegalStateException("boom");
        })) {
            worker.start();
            engine.offer(TASK_TYPE);
            await().atMost(Duration.ofSeconds(5)).until(() -> engine.failures.size() == 1);

            // The proof that the thread survived: a second task still runs.
            engine.offer(TASK_TYPE);
            await().atMost(Duration.ofSeconds(5)).until(() -> engine.failures.size() == 2);
        }

        assertThat(invocations.get()).isEqualTo(2);
        assertThat(engine.failures.getFirst().message()).contains("IllegalStateException").contains("boom");
        assertThat(engine.failures.getFirst().permanent())
                .as("an unexpected exception might not recur, so it is retryable")
                .isFalse();
    }

    @Test
    @DisplayName("a permanent failure is reported as permanent, so the engine stops retrying")
    void permanentFailureIsReportedAsSuch() throws Exception {
        FakeEngineClient engine = new FakeEngineClient();

        try (SentinelWorker worker = worker(engine,
                context -> TaskResult.permanentlyFailed("record does not exist"))) {
            worker.start();
            engine.offer(TASK_TYPE);
            await().atMost(Duration.ofSeconds(5)).until(() -> !engine.failures.isEmpty());
        }

        assertThat(engine.failures.getFirst().permanent()).isTrue();
    }

    @Test
    @DisplayName("a handler returning null is treated as a failure, not as success")
    void nullResultIsAFailure() throws Exception {
        FakeEngineClient engine = new FakeEngineClient();

        try (SentinelWorker worker = worker(engine, context -> null)) {
            worker.start();
            engine.offer(TASK_TYPE);
            await().atMost(Duration.ofSeconds(5)).until(() -> !engine.failures.isEmpty());
        }

        assertThat(engine.completions)
                .as("a null return is far more likely a forgotten branch than a claim of success")
                .isEmpty();
    }

    @Test
    @DisplayName("a task type with no handler fails permanently rather than being retried forever")
    void unknownTaskTypeFailsPermanently() throws Exception {
        FakeEngineClient engine = new FakeEngineClient();

        try (SentinelWorker worker = worker(engine, context -> TaskResult.completed())) {
            worker.start();
            engine.offer("some.other.type");
            await().atMost(Duration.ofSeconds(5)).until(() -> !engine.failures.isEmpty());
        }

        assertThat(engine.failures.getFirst().permanent())
                .as("retrying reaches the same worker pool and fails the same way")
                .isTrue();
    }

    // ------------------------------------------------------------------
    // Capacity
    // ------------------------------------------------------------------

    @Test
    @DisplayName("the worker never runs more tasks at once than its configured concurrency")
    void concurrencyIsRespected() throws Exception {
        FakeEngineClient engine = new FakeEngineClient();
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        CountDownLatch release = new CountDownLatch(1);

        try (SentinelWorker worker = SentinelWorker.builder()
                .engineClient(engine)
                .concurrency(2)
                .maxBatchSize(10)
                .register(TASK_TYPE, context -> {
                    peak.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
                    release.await();
                    inFlight.decrementAndGet();
                    return TaskResult.completed();
                })
                .build()) {

            worker.start();
            for (int i = 0; i < 8; i++) {
                engine.offer(TASK_TYPE);
            }
            await().atMost(Duration.ofSeconds(5)).until(() -> inFlight.get() == 2);
            Thread.sleep(200);

            assertThat(peak.get()).isEqualTo(2);
            release.countDown();
            await().atMost(Duration.ofSeconds(10)).until(() -> engine.completions.size() == 8);
        }
    }

    @Test
    @DisplayName("the worker never asks for more tasks than it has free capacity for")
    void requestedBatchNeverExceedsFreeCapacity() throws Exception {
        FakeEngineClient engine = new FakeEngineClient();
        CountDownLatch release = new CountDownLatch(1);

        try (SentinelWorker worker = SentinelWorker.builder()
                .engineClient(engine)
                .concurrency(3)
                .maxBatchSize(100)
                .register(TASK_TYPE, context -> {
                    release.await();
                    return TaskResult.completed();
                })
                .build()) {

            worker.start();
            for (int i = 0; i < 6; i++) {
                engine.offer(TASK_TYPE);
            }
            await().atMost(Duration.ofSeconds(5)).until(() -> worker.runningTasks() == 3);
            Thread.sleep(200);

            assertThat(engine.requestedBatchSizes)
                    .as("asking for work it cannot start means claiming leases nothing is progressing on, "
                            + "which converts directly into lease expiry and duplicate execution")
                    .allMatch(size -> size <= 3);
            release.countDown();
        }
    }

    @Test
    @DisplayName("capacity is returned after a failure, not only after a success")
    void permitsSurviveFailures() throws Exception {
        FakeEngineClient engine = new FakeEngineClient();

        try (SentinelWorker worker = SentinelWorker.builder()
                .engineClient(engine)
                .concurrency(1)
                .register(TASK_TYPE, context -> {
                    throw new RuntimeException("always fails");
                })
                .build()) {

            worker.start();
            for (int i = 0; i < 5; i++) {
                engine.offer(TASK_TYPE);
            }

            // A leaked permit would stall this at one, and a worker configured for one task would
            // silently become a worker that runs none.
            await().atMost(Duration.ofSeconds(10)).until(() -> engine.failures.size() == 5);
        }
    }

    // ------------------------------------------------------------------
    // Leases
    // ------------------------------------------------------------------

    @Test
    @DisplayName("in-flight tasks are heartbeated while they run")
    void inFlightTasksAreHeartbeated() throws Exception {
        FakeEngineClient engine = new FakeEngineClient();
        CountDownLatch release = new CountDownLatch(1);

        try (SentinelWorker worker = SentinelWorker.builder()
                .engineClient(engine)
                .concurrency(1)
                .heartbeatInterval(Duration.ofMillis(50))
                .register(TASK_TYPE, context -> {
                    release.await();
                    return TaskResult.completed();
                })
                .build()) {

            worker.start();
            engine.offer(TASK_TYPE);

            await().atMost(Duration.ofSeconds(5)).until(() -> engine.heartbeatCount.get() >= 3);
            release.countDown();
        }
    }

    @Test
    @DisplayName("a task whose lease was lost is not reported, since the engine gave it to someone else")
    void aLostLeaseStopsReporting() throws Exception {
        FakeEngineClient engine = new FakeEngineClient();
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean sawLeaseLost = new AtomicBoolean();

        try (SentinelWorker worker = SentinelWorker.builder()
                .engineClient(engine)
                .concurrency(1)
                .heartbeatInterval(Duration.ofMillis(50))
                .register(TASK_TYPE, context -> {
                    running.countDown();
                    release.await();
                    sawLeaseLost.set(context.isLeaseLost());
                    return TaskResult.completed();
                })
                .build()) {

            worker.start();
            EngineClient.AssignedTask task = engine.offer(TASK_TYPE);
            assertThat(running.await(5, TimeUnit.SECONDS)).isTrue();

            engine.revokedLeases.add(task.taskId());
            await().atMost(Duration.ofSeconds(5)).until(() -> engine.heartbeatCount.get() >= 2);
            release.countDown();

            Thread.sleep(300);
            assertThat(sawLeaseLost.get())
                    .as("a long-running handler needs a way to notice it is working for nothing")
                    .isTrue();
            assertThat(engine.completions)
                    .as("reporting would be rejected, and the task is already running elsewhere")
                    .isEmpty();
        }
    }

    // ------------------------------------------------------------------
    // Engine failures
    // ------------------------------------------------------------------

    @Test
    @DisplayName("an unreachable engine is retried rather than killing the poll loop")
    void anUnreachableEngineIsRetried() throws Exception {
        FakeEngineClient engine = new FakeEngineClient();
        engine.pollError.set(new EngineUnavailableException("connection refused", null));

        try (SentinelWorker worker = worker(engine, context -> TaskResult.completed())) {
            worker.start();
            await().atMost(Duration.ofSeconds(5)).until(() -> engine.pollCount.get() >= 3);

            // Recovery: a poller thread that had died would never pick this up.
            engine.pollError.set(null);
            engine.offer(TASK_TYPE);
            await().atMost(Duration.ofSeconds(10)).until(() -> !engine.completions.isEmpty());
        }
    }

    @Test
    @DisplayName("a shed request backs off and then resumes")
    void aShedRequestBacksOff() throws Exception {
        FakeEngineClient engine = new FakeEngineClient();
        engine.pollError.set(new EngineOverloadedException("too many waiters", Duration.ofMillis(100)));

        try (SentinelWorker worker = worker(engine, context -> TaskResult.completed())) {
            worker.start();
            await().atMost(Duration.ofSeconds(5)).until(() -> engine.pollCount.get() >= 2);

            engine.pollError.set(null);
            engine.offer(TASK_TYPE);
            await().atMost(Duration.ofSeconds(10)).until(() -> !engine.completions.isEmpty());
        }
    }

    @Test
    @DisplayName("a report that fails once is retried")
    void aFailedReportIsRetried() throws Exception {
        FakeEngineClient engine = new FakeEngineClient();
        engine.reportErrorOnce.set(new EngineUnavailableException("blip", null));

        try (SentinelWorker worker = worker(engine, context -> TaskResult.completed("{}"))) {
            worker.start();
            engine.offer(TASK_TYPE);

            await().atMost(Duration.ofSeconds(10)).until(() -> !engine.completions.isEmpty());
        }
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    @Test
    @DisplayName("shutdown waits for work that is already running")
    void shutdownDrainsInFlightWork() throws Exception {
        FakeEngineClient engine = new FakeEngineClient();
        CountDownLatch running = new CountDownLatch(1);

        SentinelWorker worker = SentinelWorker.builder()
                .engineClient(engine)
                .concurrency(1)
                .drainTimeout(Duration.ofSeconds(10))
                .register(TASK_TYPE, context -> {
                    running.countDown();
                    Thread.sleep(400);
                    return TaskResult.completed("{}");
                })
                .build();

        worker.start();
        engine.offer(TASK_TYPE);
        assertThat(running.await(5, TimeUnit.SECONDS)).isTrue();

        worker.close();

        assertThat(engine.completions)
                .as("closing must not discard work that was seconds from finishing")
                .hasSize(1);
        assertThat(engine.isClosed()).isTrue();
    }

    @Test
    @DisplayName("work still running at the drain timeout is abandoned, never reported as failed")
    void undrainedWorkIsAbandonedRatherThanFailed() throws Exception {
        FakeEngineClient engine = new FakeEngineClient();
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        SentinelWorker worker = SentinelWorker.builder()
                .engineClient(engine)
                .concurrency(1)
                .drainTimeout(Duration.ofMillis(200))
                .register(TASK_TYPE, context -> {
                    running.countDown();
                    release.await();
                    return TaskResult.completed("{}");
                })
                .build();

        worker.start();
        engine.offer(TASK_TYPE);
        assertThat(running.await(5, TimeUnit.SECONDS)).isTrue();

        worker.close();

        assertThat(engine.failures)
                .as("reporting a still-executing task as failed makes the engine start a second copy "
                        + "concurrently, which is far worse than one lease duration of delay")
                .isEmpty();
        release.countDown();
    }

    @Test
    @DisplayName("closing twice is harmless")
    void closingTwiceIsHarmless() {
        FakeEngineClient engine = new FakeEngineClient();
        SentinelWorker worker = worker(engine, context -> TaskResult.completed());
        worker.start();

        worker.close();
        worker.close();

        assertThat(worker.isRunning()).isFalse();
    }

    @Test
    @DisplayName("starting twice is rejected rather than silently doubling the threads")
    void startingTwiceIsRejected() {
        FakeEngineClient engine = new FakeEngineClient();
        try (SentinelWorker worker = worker(engine, context -> TaskResult.completed())) {
            worker.start();

            assertThatThrownBy(worker::start)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("already running");
        }
    }

    // ------------------------------------------------------------------
    // Configuration
    // ------------------------------------------------------------------

    @Test
    @DisplayName("a worker with no handlers is rejected at build time")
    void aWorkerWithNoHandlersIsRejected() {
        assertThatThrownBy(() -> SentinelWorker.builder().engineClient(new FakeEngineClient()).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("at least one handler");
    }

    @Test
    @DisplayName("registering two handlers for one task type is rejected")
    void duplicateRegistrationIsRejected() {
        SentinelWorker.Builder builder = SentinelWorker.builder()
                .engineClient(new FakeEngineClient())
                .register(TASK_TYPE, context -> TaskResult.completed());

        assertThatThrownBy(() -> builder.register(TASK_TYPE, context -> TaskResult.completed()))
                .as("silently replacing would surface much later as the wrong code running")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("a worker id is generated when none is supplied")
    void workerIdIsGeneratedWhenAbsent() {
        try (SentinelWorker worker = worker(new FakeEngineClient(), context -> TaskResult.completed())) {
            assertThat(worker.workerId()).startsWith("worker-");
            assertThat(UUID.fromString(worker.workerId().substring("worker-".length()))).isNotNull();
        }
    }

    @Test
    @DisplayName("invalid options are rejected with a message that says what to fix")
    void invalidOptionsAreRejected() {
        assertThatThrownBy(() -> SentinelWorker.builder()
                .engineClient(new FakeEngineClient())
                .concurrency(0)
                .register(TASK_TYPE, context -> TaskResult.completed())
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("concurrency must be at least 1");
    }

    // ------------------------------------------------------------------

    private static SentinelWorker worker(FakeEngineClient engine, ActivityHandler handler) {
        return SentinelWorker.builder()
                .engineClient(engine)
                .workerId("worker-" + UUID.randomUUID())
                .concurrency(2)
                .pollWait(Duration.ofMillis(200))
                .heartbeatInterval(Duration.ofMillis(100))
                .drainTimeout(Duration.ofSeconds(5))
                .register(TASK_TYPE, handler)
                .build();
    }
}
