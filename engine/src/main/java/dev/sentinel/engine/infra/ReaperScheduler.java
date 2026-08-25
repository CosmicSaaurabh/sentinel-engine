package dev.sentinel.engine.infra;

import dev.sentinel.engine.service.ReaperService;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Runs the reaper on a jittered interval.
 *
 * <p>Owns a thread and nothing else. All of the recovery logic lives in {@link ReaperService},
 * which is directly callable, so tests drive recovery rather than waiting on a timer.
 *
 * <h2>Why the schedule is hand-rolled</h2>
 *
 * <p>A fixed-rate or fixed-delay schedule would be less code, but it cannot express the two things
 * this needs.
 *
 * <p><strong>Jitter between passes.</strong> Every engine instance starts its reaper at roughly the
 * same moment after a deployment, so a fixed interval has them all reaping in lockstep forever
 * after. They would contend on the same rows every five seconds, and {@code SKIP LOCKED} would turn
 * that into most instances finding nothing, which is wasteful rather than wrong but entirely
 * avoidable.
 *
 * <p><strong>Running again immediately after a full batch.</strong> After a mass expiry, waiting
 * out the interval between batches would drain a large backlog at one batch per tick. Rescheduling
 * with no delay when a pass fills its batch means the backlog drains as fast as the database
 * allows, while every individual transaction stays bounded.
 *
 * <h2>Why every failure is caught</h2>
 *
 * <p>A scheduled task that throws is silently cancelled and never runs again. Here that would mean
 * lease recovery quietly stopping for the life of the process, with every task held by a dead
 * worker stranded and nothing in the logs to say why. The pass is wrapped so that no failure can
 * end the schedule.
 */
@Component
public class ReaperScheduler {

    private static final Logger log = LoggerFactory.getLogger(ReaperScheduler.class);

    private final ReaperService reaper;
    private final ReaperProperties properties;
    private final ScheduledExecutorService scheduler;

    private volatile boolean running;

    public ReaperScheduler(ReaperService reaper, ReaperProperties properties) {
        this.reaper = reaper;
        this.properties = properties;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "sentinel-reaper");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Starts once the application is fully up.
     *
     * <p>Deliberately not in the constructor: the reaper writes to the database on its first pass,
     * and starting before migrations and the connection pool are ready would produce a burst of
     * startup errors that mean nothing.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (!properties.enabled()) {
            log.warn("the reaper is disabled; tasks held by a dead worker will never be recovered");
            return;
        }
        running = true;
        scheduleNext(properties.interval().toMillis());
        log.info("reaper started, every {} with {}% jitter, up to {} task(s) per pass",
                properties.interval(), Math.round(properties.jitter() * 100), properties.batchSize());
    }

    private void scheduleNext(long delayMillis) {
        if (!running) {
            return;
        }
        try {
            scheduler.schedule(this::runPass, delayMillis, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // The scheduler is shutting down. Nothing to recover from and nothing worth logging
            // loudly, since this is the ordinary end of the loop.
            log.debug("reaper not rescheduled: the scheduler is shutting down");
        }
    }

    private void runPass() {
        long nextDelayMillis = jitteredIntervalMillis();
        try {
            ReaperService.ReapResult result = reaper.reapOnce();
            if (result.sawFullBatch()) {
                // More is almost certainly waiting. Draining at one batch per interval would make
                // recovery from a mass expiry take minutes for no reason.
                nextDelayMillis = 0;
            }
        } catch (RuntimeException e) {
            reaper.recordPassError();
            log.error("reaper pass failed; recovery will be retried on the next tick", e);
        } catch (Error e) {
            reaper.recordPassError();
            log.error("reaper pass hit an error", e);
            throw e;
        } finally {
            scheduleNext(nextDelayMillis);
        }
    }

    /** Spreads instances apart so they do not all reap on the same second forever. */
    private long jitteredIntervalMillis() {
        long base = properties.interval().toMillis();
        long spread = Math.max(1, (long) (base * properties.jitter()));
        return base - spread / 2 + ThreadLocalRandom.current().nextLong(spread + 1);
    }

    @PreDestroy
    public void stop() {
        running = false;
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("reaper thread did not stop within 5 seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
