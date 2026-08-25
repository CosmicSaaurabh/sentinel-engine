package dev.sentinel.worker;

import java.time.Duration;
import java.util.Objects;

/**
 * Tuning for a worker. Built through {@link SentinelWorker#builder()}; every value has a default
 * that works.
 *
 * <h2>The one relationship that matters</h2>
 *
 * <p>{@code heartbeatInterval} must be comfortably shorter than the engine's lease duration, and by
 * default it is a third of it. That ratio is what lets a worker miss two heartbeats to a network
 * blip without losing work it is still doing, while still meeting the engine's promise to recover a
 * genuinely dead worker's task within three missed intervals.
 *
 * <p>Raising the interval towards the lease duration removes that margin: a single dropped packet
 * then costs the worker its task. Lowering it costs a little more traffic and buys nothing, since
 * the lease is what the engine actually measures.
 *
 * @param engineTarget host and port of the engine, for example {@code localhost:9090}
 * @param workerId stable identity of this worker process. Must survive a reconnect, or the engine
 *        cannot tell a returning worker from a new one. Generated if not supplied
 * @param concurrency how many tasks this worker runs at once. This is the size of the activity
 *        thread pool and the number of permits, and the worker never claims work beyond it
 * @param pollWait how long the engine may hold a poll open before answering with nothing. Longer
 *        means fewer round trips from an idle worker; the engine caps it regardless
 * @param heartbeatInterval how often in-flight leases are renewed
 * @param drainTimeout how long {@link SentinelWorker#close()} waits for running tasks before giving
 *        up on them
 * @param maxBatchSize ceiling on tasks claimed per poll, further clamped by free permits and by the
 *        engine
 */
public record WorkerOptions(
        String engineTarget,
        String workerId,
        int concurrency,
        Duration pollWait,
        Duration heartbeatInterval,
        Duration drainTimeout,
        int maxBatchSize) {

    public static final Duration DEFAULT_POLL_WAIT = Duration.ofSeconds(20);

    /** A third of the engine's default 30 second lease, so two heartbeats may be lost harmlessly. */
    public static final Duration DEFAULT_HEARTBEAT_INTERVAL = Duration.ofSeconds(10);

    public static final Duration DEFAULT_DRAIN_TIMEOUT = Duration.ofSeconds(30);
    public static final int DEFAULT_CONCURRENCY = 4;
    public static final int DEFAULT_MAX_BATCH_SIZE = 8;

    public WorkerOptions {
        Objects.requireNonNull(engineTarget, "engineTarget");
        Objects.requireNonNull(workerId, "workerId");
        Objects.requireNonNull(pollWait, "pollWait");
        Objects.requireNonNull(heartbeatInterval, "heartbeatInterval");
        Objects.requireNonNull(drainTimeout, "drainTimeout");

        if (engineTarget.isBlank()) {
            throw new IllegalArgumentException("engineTarget must not be blank");
        }
        if (workerId.isBlank()) {
            throw new IllegalArgumentException("workerId must not be blank");
        }
        if (concurrency < 1) {
            throw new IllegalArgumentException("concurrency must be at least 1, was " + concurrency);
        }
        if (maxBatchSize < 1) {
            throw new IllegalArgumentException("maxBatchSize must be at least 1, was " + maxBatchSize);
        }
        if (heartbeatInterval.isZero() || heartbeatInterval.isNegative()) {
            throw new IllegalArgumentException("heartbeatInterval must be positive");
        }
        if (drainTimeout.isNegative()) {
            throw new IllegalArgumentException("drainTimeout must not be negative");
        }
    }
}
