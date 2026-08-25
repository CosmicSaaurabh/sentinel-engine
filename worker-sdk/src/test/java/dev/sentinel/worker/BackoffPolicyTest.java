package dev.sentinel.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Backoff arithmetic and the jitter contract. */
class BackoffPolicyTest {

    private static final Duration BASE = Duration.ofMillis(100);
    private static final Duration MAX = Duration.ofSeconds(30);

    @Test
    @DisplayName("delays are drawn from a window that doubles with each failure")
    void theWindowDoubles() {
        BackoffPolicy policy = new BackoffPolicy(BASE, MAX);

        // Full jitter means any single delay can be small, so the ceiling is what is asserted:
        // nothing may exceed the window for that attempt.
        assertThat(policy.nextDelay()).isLessThanOrEqualTo(Duration.ofMillis(100));
        assertThat(policy.nextDelay()).isLessThanOrEqualTo(Duration.ofMillis(200));
        assertThat(policy.nextDelay()).isLessThanOrEqualTo(Duration.ofMillis(400));
        assertThat(policy.nextDelay()).isLessThanOrEqualTo(Duration.ofMillis(800));
    }

    @Test
    @DisplayName("the window is capped, so a long outage does not produce an absurd delay")
    void theWindowIsCapped() {
        BackoffPolicy policy = new BackoffPolicy(BASE, MAX);

        IntStream.range(0, 100).forEach(i -> policy.nextDelay());

        assertThat(policy.nextDelay())
                .as("a worker that has been failing for hours must still retry within the cap")
                .isLessThanOrEqualTo(MAX);
    }

    @Test
    @DisplayName("a recovered connection starts from the bottom again")
    void resetReturnsToTheStart() {
        BackoffPolicy policy = new BackoffPolicy(BASE, MAX);
        IntStream.range(0, 10).forEach(i -> policy.nextDelay());

        policy.reset();

        assertThat(policy.consecutiveFailures()).isZero();
        assertThat(policy.nextDelay()).isLessThanOrEqualTo(Duration.ofMillis(100));
    }

    @Test
    @DisplayName("delays actually vary, which is the whole point of jitter")
    void delaysVary() {
        long distinct = IntStream.range(0, 200)
                .mapToObj(i -> {
                    BackoffPolicy policy = new BackoffPolicy(BASE, MAX);
                    IntStream.range(0, 8).forEach(attempt -> policy.nextDelay());
                    return policy.nextDelay();
                })
                .distinct()
                .count();

        assertThat(distinct)
                .as("a deterministic schedule makes every worker that failed together retry together")
                .isGreaterThan(50);
    }
}
