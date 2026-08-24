package dev.sentinel.engine.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.random.RandomGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Backoff arithmetic and the jitter contract, with randomness pinned. */
class ExponentialBackoffRetryPolicyTest {

    private static final Duration BASE = Duration.ofSeconds(2);
    private static final Duration CAP = Duration.ofMinutes(5);

    /** Always returns the top of the range, which exposes the un-jittered schedule for assertion. */
    private static final RandomGenerator ALWAYS_MAX = new RandomGenerator() {
        @Override
        public long nextLong() {
            return Long.MAX_VALUE;
        }

        @Override
        public long nextLong(long bound) {
            return bound - 1;
        }
    };

    /** Always returns the bottom of the range, which is the shortest delay jitter can produce. */
    private static final RandomGenerator ALWAYS_MIN = new RandomGenerator() {
        @Override
        public long nextLong() {
            return 0;
        }

        @Override
        public long nextLong(long bound) {
            return 0;
        }
    };

    @Test
    @DisplayName("the un-jittered schedule doubles: 2s, 4s, 8s, 16s, 32s")
    void scheduleDoubles() {
        RetryPolicy policy = new ExponentialBackoffRetryPolicy(BASE, CAP, ALWAYS_MAX);

        assertThat(policy.delayBefore(1, FailureKind.RETRYABLE)).isEqualTo(Duration.ofSeconds(2));
        assertThat(policy.delayBefore(2, FailureKind.RETRYABLE)).isEqualTo(Duration.ofSeconds(4));
        assertThat(policy.delayBefore(3, FailureKind.RETRYABLE)).isEqualTo(Duration.ofSeconds(8));
        assertThat(policy.delayBefore(4, FailureKind.RETRYABLE)).isEqualTo(Duration.ofSeconds(16));
        assertThat(policy.delayBefore(5, FailureKind.RETRYABLE)).isEqualTo(Duration.ofSeconds(32));
    }

    @Test
    @DisplayName("the schedule is capped, so a long-lived task never waits hours")
    void scheduleIsCapped() {
        RetryPolicy policy = new ExponentialBackoffRetryPolicy(BASE, CAP, ALWAYS_MAX);

        assertThat(policy.delayBefore(20, FailureKind.RETRYABLE)).isEqualTo(CAP);
        assertThat(policy.delayBefore(1000, FailureKind.RETRYABLE))
                .as("a huge attempt number must not overflow the exponent into nonsense")
                .isEqualTo(CAP);
    }

    @Test
    @DisplayName("full jitter can return anything from zero up to the scheduled delay")
    void jitterSpansTheWholeWindow() {
        assertThat(new ExponentialBackoffRetryPolicy(BASE, CAP, ALWAYS_MIN)
                .delayBefore(5, FailureKind.RETRYABLE))
                .isEqualTo(Duration.ZERO);
        assertThat(new ExponentialBackoffRetryPolicy(BASE, CAP, ALWAYS_MAX)
                .delayBefore(5, FailureKind.RETRYABLE))
                .isEqualTo(Duration.ofSeconds(32));
    }

    @Test
    @DisplayName("real jitter spreads retries instead of stacking them on one instant")
    void jitterActuallyVaries() {
        RetryPolicy policy = new ExponentialBackoffRetryPolicy(
                BASE, CAP, java.util.random.RandomGenerator.getDefault());

        long distinctDelays = java.util.stream.IntStream.range(0, 200)
                .mapToObj(i -> policy.delayBefore(6, FailureKind.RETRYABLE))
                .distinct()
                .count();

        assertThat(distinctDelays)
                .as("a deterministic schedule makes a correlated failure retry as a synchronised wave")
                .isGreaterThan(100);
    }

    @Test
    @DisplayName("a permanent failure is never retried, however many attempts remain")
    void permanentFailuresAreNotRetried() {
        RetryPolicy policy = new ExponentialBackoffRetryPolicy(BASE, CAP, ALWAYS_MAX);

        assertThat(policy.shouldRetry(1, 10, FailureKind.PERMANENT))
                .as("nine more attempts would reach the same rejection and delay the terminal state")
                .isFalse();
        assertThat(policy.shouldRetry(1, 10, FailureKind.RETRYABLE)).isTrue();
    }

    @Test
    @DisplayName("retries stop once the attempt budget is spent")
    void retriesStopAtMaxAttempts() {
        RetryPolicy policy = new ExponentialBackoffRetryPolicy(BASE, CAP, ALWAYS_MAX);

        assertThat(policy.shouldRetry(9, 10, FailureKind.RETRYABLE)).isTrue();
        assertThat(policy.shouldRetry(10, 10, FailureKind.RETRYABLE)).isFalse();
    }
}
