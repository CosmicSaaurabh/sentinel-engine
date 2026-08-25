package dev.sentinel.worker;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps every in-flight lease alive, on a thread of its own.
 *
 * <h2>Why a dedicated thread</h2>
 *
 * <p>The obvious simplifications are to heartbeat from the poller thread, or to have each activity
 * thread renew its own lease. Both are wrong, and for the same underlying reason.
 *
 * <p>If handlers saturate the activity pool with CPU-bound work, an activity thread cannot
 * reliably get scheduled in time to heartbeat. The worker is healthy and working hard, misses its
 * heartbeats <em>because</em> it is working hard, gets reaped, and has its work duplicated
 * elsewhere. Load causes a failure that looks exactly like a crash, and it gets worse the busier
 * the worker is.
 *
 * <p>One thread that does nothing but send a small batched call on a timer does not have that
 * failure mode.
 *
 * <h2>Shared state</h2>
 *
 * <p>{@code activeLeases} is written by activity threads as tasks start and finish, and read by
 * this scheduler on its timer, so it is genuinely concurrent. A {@link ConcurrentHashMap} is
 * enough because the scheduler does not need a consistent snapshot: renewing a lease that was
 * released a millisecond ago is harmless, since the engine's guarded update simply matches nothing.
 */
final class HeartbeatScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatScheduler.class);

    private final EngineClient client;
    private final Duration interval;
    private final Map<UUID, Long> activeLeases = new ConcurrentHashMap<>();
    private final Set<UUID> lostLeases = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService scheduler;

    HeartbeatScheduler(EngineClient client, Duration interval) {
        this.client = client;
        this.interval = interval;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "sentinel-heartbeat");
            // Daemon so a forgotten close() cannot keep a JVM alive forever. Shutdown still stops
            // it deliberately; this is a backstop, not the mechanism.
            thread.setDaemon(true);
            return thread;
        });
    }

    void start() {
        // Fixed delay, not fixed rate. If one heartbeat call is slow, fixed rate would queue the
        // next one immediately behind it and pile up; fixed delay simply spaces them out.
        scheduler.scheduleWithFixedDelay(
                this::renewAllSafely, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Starts renewing this task's lease. Called by an activity thread as the task begins. */
    void track(UUID taskId, long fencingToken) {
        activeLeases.put(taskId, fencingToken);
    }

    /** Stops renewing. Called from a {@code finally} block, so it runs however the task ended. */
    void release(UUID taskId) {
        activeLeases.remove(taskId);
        lostLeases.remove(taskId);
    }

    /** Whether the engine has told us this task is no longer ours. */
    boolean isLost(UUID taskId) {
        return lostLeases.contains(taskId);
    }

    int activeLeaseCount() {
        return activeLeases.size();
    }

    /**
     * Renews everything, absorbing any failure.
     *
     * <p>Catching {@link Throwable} here is not laziness. A scheduled task that throws is silently
     * cancelled and never runs again: heartbeats would stop for the life of the process, every
     * in-flight task would be reaped, and nothing anywhere would say why. This is one of the few
     * places where catching broadly is the correct choice, and it always logs.
     */
    private void renewAllSafely() {
        try {
            renewAll();
        } catch (RuntimeException e) {
            log.warn("heartbeat round failed; leases will be retried on the next tick", e);
        } catch (Error e) {
            log.error("heartbeat thread hit an error", e);
            throw e;
        }
    }

    void renewAll() {
        if (activeLeases.isEmpty()) {
            return;
        }

        Map<UUID, Long> snapshot = Map.copyOf(activeLeases);
        EngineClient.HeartbeatOutcome outcome;
        try {
            outcome = client.heartbeat(snapshot);
        } catch (EngineUnavailableException | EngineOverloadedException e) {
            // The engine being briefly unreachable is not a lost lease. Assuming the worst and
            // abandoning healthy work would turn a network blip into duplicated execution, so the
            // tasks keep running and the next tick tries again. If the outage outlasts the lease,
            // the engine re-queues the task and the fencing token makes this worker's eventual
            // report harmless, which is exactly the intended outcome.
            log.warn("could not renew {} lease(s): {}", snapshot.size(), e.getMessage());
            return;
        }

        for (UUID lost : outcome.lost()) {
            if (lostLeases.add(lost)) {
                log.warn("lost the lease on task {}; its handler should stop, and nothing it "
                        + "reports will be accepted", lost);
            }
        }
        if (log.isDebugEnabled() && !outcome.renewed().isEmpty()) {
            log.debug("renewed {} lease(s)", outcome.renewed().size());
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("heartbeat thread did not stop within 5 seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
