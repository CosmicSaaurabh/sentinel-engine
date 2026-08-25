package dev.sentinel.worker;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with full jitter, used when the engine cannot be reached.
 *
 * <p>Jitter is not decoration here, and the scenario it exists for is specific. When an engine
 * instance restarts or a deployment rolls, every worker attached to it fails at the same instant.
 * With a purely exponential schedule, all of them retry at the same instant too, then again at the
 * next same instant, forever. The reconnection arrives as a wave, and each wave is capable of
 * knocking over the instance that is trying to come back up.
 *
 * <p>Worse, a rolling restart turns that from survivable into cascading, because each instance's
 * wave lands on the instances that are still healthy.
 *
 * <p>Full jitter, drawing uniformly from zero up to the computed delay, spreads a thousand workers
 * across the whole window instead of stacking them on one moment. The average wait rises slightly;
 * the synchronised burst disappears entirely.
 *
 * <p>Not thread safe, and does not need to be: each instance belongs to one loop on one thread.
 */
final class BackoffPolicy {

    private final Duration baseDelay;
    private final Duration maxDelay;
    private int consecutiveFailures;

    BackoffPolicy(Duration baseDelay, Duration maxDelay) {
        this.baseDelay = baseDelay;
        this.maxDelay = maxDelay;
    }

    /** Records a failure and returns how long to wait before the next attempt. */
    Duration nextDelay() {
        consecutiveFailures++;
        long ceilingMillis = Math.min(maxDelay.toMillis(), exponentialMillis(consecutiveFailures));
        return Duration.ofMillis(ThreadLocalRandom.current().nextLong(ceilingMillis + 1));
    }

    /** Called after a successful call, so a recovered connection starts from the bottom again. */
    void reset() {
        consecutiveFailures = 0;
    }

    int consecutiveFailures() {
        return consecutiveFailures;
    }

    /**
     * Bounded shift rather than {@code Math.pow}, so a worker that has been failing for a very long
     * time cannot overflow the exponent into a nonsense delay.
     */
    private long exponentialMillis(int failures) {
        int exponent = Math.max(0, Math.min(failures - 1, 32));
        long base = baseDelay.toMillis();
        long multiplier = 1L << exponent;
        return base > Long.MAX_VALUE / multiplier ? Long.MAX_VALUE : base * multiplier;
    }
}
