package dev.sentinel.engine.domain;

import java.time.Duration;
import java.util.Objects;
import java.util.random.RandomGenerator;

/**
 * Exponential backoff with full jitter.
 *
 * <p>The schedule doubles: with a two second base, the un-jittered delays are 2, 4, 8, 16, 32
 * seconds and so on, capped so that a long-lived task does not end up waiting hours.
 *
 * <h2>Why jitter is not optional</h2>
 *
 * <p>A purely exponential schedule is deterministic, which sounds like a virtue and is actually the
 * problem. Failures in a distributed system are correlated: a downstream service goes down and a
 * thousand tasks fail within the same second. With a deterministic schedule, all thousand retry at
 * exactly the same instant, two seconds later, and again four seconds after that. The retries
 * arrive as waves, and each wave is capable of knocking the recovering service back down.
 *
 * <p>Full jitter, choosing uniformly from zero up to the computed delay, spreads that thousand
 * across the whole window instead of stacking them on one instant. It trades a slightly longer
 * average wait for the elimination of synchronised bursts, which is a very good trade when the
 * thing being retried is already unwell.
 *
 * <p>This is a deliberate divergence from HLD-001, which named a fixed 2, 4, 8, 16, 32 second
 * sequence. It is recorded in LLD-001 section 17 rather than applied quietly.
 */
public final class ExponentialBackoffRetryPolicy implements RetryPolicy {

    private final Duration baseDelay;
    private final Duration maxDelay;
    private final RandomGenerator random;

    /**
     * @param random injected rather than taken from a static, so tests can pin the jitter and assert
     *        the schedule instead of asserting a range and hoping
     */
    public ExponentialBackoffRetryPolicy(Duration baseDelay, Duration maxDelay, RandomGenerator random) {
        this.baseDelay = Objects.requireNonNull(baseDelay, "baseDelay");
        this.maxDelay = Objects.requireNonNull(maxDelay, "maxDelay");
        this.random = Objects.requireNonNull(random, "random");
        if (baseDelay.isNegative() || baseDelay.isZero()) {
            throw new IllegalArgumentException("baseDelay must be positive");
        }
        if (maxDelay.compareTo(baseDelay) < 0) {
            throw new IllegalArgumentException("maxDelay must be at least baseDelay");
        }
    }

    @Override
    public Duration delayBefore(int attempt, FailureKind kind) {
        if (kind == FailureKind.PERMANENT) {
            return Duration.ZERO;
        }
        long ceilingMillis = Math.min(maxDelay.toMillis(), exponentialMillis(attempt));
        // Upper bound is exclusive, so +1 keeps the full delay reachable.
        return Duration.ofMillis(random.nextLong(ceilingMillis + 1));
    }

    /**
     * The un-jittered ceiling for this attempt.
     *
     * <p>Shifting rather than using {@code Math.pow} keeps this in integer arithmetic, and the shift
     * is bounded because a task with 62 attempts would otherwise overflow the exponent into
     * nonsense.
     */
    private long exponentialMillis(int attempt) {
        int exponent = Math.max(0, Math.min(attempt - 1, 32));
        long base = baseDelay.toMillis();
        long multiplier = 1L << exponent;
        return base > Long.MAX_VALUE / multiplier ? Long.MAX_VALUE : base * multiplier;
    }
}
