package dev.sentinel.engine.infra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import dev.sentinel.engine.service.ReaperService;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The scheduler's own behaviour, with the recovery logic mocked out.
 *
 * <p>Plain unit tests. What is being checked here is not whether recovery works, which
 * {@code ReaperServiceTest} covers, but whether the loop that drives it survives the things loops
 * are usually killed by.
 */
class ReaperSchedulerTest {

    private static final ReaperProperties FAST = new ReaperProperties(
            true, Duration.ofMillis(50), 0.2, 200, Duration.ofSeconds(1));

    @Test
    @DisplayName("a pass that throws does not end the schedule")
    void aThrowingPassDoesNotKillTheLoop() {
        ReaperService reaper = mock(ReaperService.class);
        AtomicInteger passes = new AtomicInteger();
        when(reaper.reapOnce()).thenAnswer(invocation -> {
            // Fails forever. A scheduled task that throws is silently cancelled and never runs
            // again, which here would mean lease recovery stopping for the life of the process
            // with every task held by a dead worker stranded and nothing in the logs.
            passes.incrementAndGet();
            throw new IllegalStateException("the database is unreachable");
        });
        doNothing().when(reaper).recordPassError();

        ReaperScheduler scheduler = new ReaperScheduler(reaper, FAST);
        try {
            scheduler.start();
            await().atMost(Duration.ofSeconds(5)).until(() -> passes.get() >= 5);
        } finally {
            scheduler.stop();
        }

        assertThat(passes.get())
                .as("recovery must keep being attempted while the cause of the failure persists")
                .isGreaterThanOrEqualTo(5);
    }

    @Test
    @DisplayName("a full batch is drained immediately rather than one batch per interval")
    void aFullBatchReschedulesWithoutWaiting() {
        ReaperService reaper = mock(ReaperService.class);
        AtomicInteger passes = new AtomicInteger();
        when(reaper.reapOnce()).thenAnswer(invocation -> {
            passes.incrementAndGet();
            return new ReaperService.ReapResult(200, 0, true);
        });

        // A one second interval: without the immediate reschedule, five passes would take five
        // seconds, and a mass expiry would take minutes to drain for no reason.
        ReaperScheduler scheduler = new ReaperScheduler(
                reaper, new ReaperProperties(true, Duration.ofSeconds(1), 0.0, 200, Duration.ofSeconds(1)));
        try {
            scheduler.start();
            await().atMost(Duration.ofSeconds(3)).until(() -> passes.get() >= 5);
        } finally {
            scheduler.stop();
        }
    }

    @Test
    @DisplayName("a disabled reaper never runs, and that is a deliberate, loud choice")
    void aDisabledReaperNeverRuns() throws InterruptedException {
        ReaperService reaper = mock(ReaperService.class);
        ReaperScheduler scheduler = new ReaperScheduler(
                reaper, new ReaperProperties(false, Duration.ofMillis(20), 0.0, 10, Duration.ZERO));

        scheduler.start();
        Thread.sleep(200);
        scheduler.stop();

        org.mockito.Mockito.verify(reaper, org.mockito.Mockito.never()).reapOnce();
    }

    @Test
    @DisplayName("stopping the scheduler stops the loop")
    void stoppingEndsTheLoop() throws InterruptedException {
        ReaperService reaper = mock(ReaperService.class);
        AtomicInteger passes = new AtomicInteger();
        when(reaper.reapOnce()).thenAnswer(invocation -> {
            passes.incrementAndGet();
            return new ReaperService.ReapResult(0, 0, false);
        });

        ReaperScheduler scheduler = new ReaperScheduler(reaper, FAST);
        scheduler.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> passes.get() >= 2);
        scheduler.stop();

        int afterStop = passes.get();
        Thread.sleep(300);

        assertThat(passes.get())
                .as("a reaper thread that outlives its engine instance would keep writing after shutdown")
                .isEqualTo(afterStop);
    }
}
