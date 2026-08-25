package dev.sentinel.engine.infra;

import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Backoff and attempt budget, with per-task-type overrides.
 *
 * <p>Different task types want genuinely different schedules. A task calling a rate-limited payment
 * API should back off hard and give up early, because hammering it makes things worse and a human
 * needs to know sooner. A cheap idempotent task should retry fast and often, because retrying costs
 * almost nothing and succeeding is likely.
 *
 * <pre>
 * sentinel:
 *   retry:
 *     base-delay: 2s          # applies to any type not named below
 *     max-delay: 5m
 *     max-attempts: 10
 *     policies:
 *       billing.charge:
 *         base-delay: 10s
 *         max-delay: 15m
 *         max-attempts: 5
 * </pre>
 *
 * <h2>Why the two halves are resolved at different times</h2>
 *
 * <p>{@code maxAttempts} is resolved at submission and stored on the task row. It could be looked up
 * at failure time instead, but then editing this configuration would retroactively change the budget
 * of tasks already in flight, and a task could exhaust its attempts because somebody edited a YAML
 * file while it was running. Resolving once makes a task's budget a property of the task.
 *
 * <p>The backoff schedule is looked up at failure time and deliberately not stored, because it is
 * only ever used to compute the next delay. An operator lowering a delay during an incident expects
 * it to take effect on the tasks currently failing, which is precisely when it matters.
 *
 * @param policies overrides by task type; any type absent here uses the top-level values
 */
@ConfigurationProperties("sentinel.retry")
public record RetryProperties(
        @DefaultValue("2s") Duration baseDelay,
        @DefaultValue("5m") Duration maxDelay,
        @DefaultValue("10") int maxAttempts,
        @DefaultValue Map<String, PerTaskType> policies) {

    public RetryProperties {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("sentinel.retry.max-attempts must be at least 1");
        }
        policies = policies == null ? Map.of() : Map.copyOf(policies);
    }

    /** Attempt budget for a task type, resolved once at submission. */
    public int maxAttemptsFor(String taskType) {
        PerTaskType override = policies.get(taskType);
        return override != null && override.maxAttempts() != null ? override.maxAttempts() : maxAttempts;
    }

    public Duration baseDelayFor(String taskType) {
        PerTaskType override = policies.get(taskType);
        return override != null && override.baseDelay() != null ? override.baseDelay() : baseDelay;
    }

    public Duration maxDelayFor(String taskType) {
        PerTaskType override = policies.get(taskType);
        return override != null && override.maxDelay() != null ? override.maxDelay() : maxDelay;
    }

    /**
     * A partial override. Every field is nullable on purpose: naming a task type to change only its
     * attempt budget should not silently reset its delays to the global defaults.
     */
    public record PerTaskType(Duration baseDelay, Duration maxDelay, Integer maxAttempts) {
    }
}
