package dev.sentinel.engine.domain;

import java.time.Duration;

/**
 * Decides whether a failed task runs again, and how long to wait first.
 *
 * <p>An interface rather than a method on the failure path, because retry behaviour is the most
 * likely thing in this engine to become per-task-type configurable. A task that calls a rate-limited
 * payment API and a task that resizes an image want very different schedules, and the substitution
 * point should exist before that requirement arrives rather than be retrofitted through the
 * completion service.
 */
public interface RetryPolicy {

    /**
     * @param attempt attempts already consumed, including the one that just failed
     * @param kind whether the failure is worth repeating at all
     * @return how long to wait before the task becomes claimable again
     */
    Duration delayBefore(int attempt, FailureKind kind);

    /**
     * Whether another attempt is permitted at all.
     *
     * <p>A permanent failure is refused immediately regardless of remaining attempts. Spending nine
     * more attempts to reach the same rejection wastes worker capacity and, worse, delays the
     * terminal state a caller is waiting on.
     */
    default boolean shouldRetry(int attempt, int maxAttempts, FailureKind kind) {
        return kind == FailureKind.RETRYABLE && attempt < maxAttempts;
    }
}
