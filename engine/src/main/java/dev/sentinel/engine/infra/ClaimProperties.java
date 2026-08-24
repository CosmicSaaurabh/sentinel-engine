package dev.sentinel.engine.infra;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Tuning for the claim path.
 *
 * <p>{@code leaseDuration} is the deadline the reaper honours, so it has to be comfortably longer
 * than the worker's heartbeat interval. The relationship that matters is lease = 3 x heartbeat: it
 * lets a worker miss two heartbeats to a blip without losing its task, while still satisfying
 * NFR4's "re-queued within three missed heartbeat intervals".
 *
 * @param leaseDuration how long a claim owns a task before the reaper may take it back
 * @param maxBatchSize hard ceiling on how many tasks one poll can take, whatever the caller asks
 *        for. A worker that asks for a thousand tasks would hold a thousand leases it cannot
 *        possibly service, so the engine caps the request rather than trusting it
 * @param recordClaimEvents whether to write a history event per claimed task. On by default so the
 *        execution history is complete; the cost is one extra insert per claim batch
 */
@ConfigurationProperties("sentinel.claim")
public record ClaimProperties(
        @DefaultValue("30s") Duration leaseDuration,
        @DefaultValue("32") int maxBatchSize,
        @DefaultValue("true") boolean recordClaimEvents) {

    public ClaimProperties {
        if (maxBatchSize < 1) {
            throw new IllegalArgumentException("sentinel.claim.max-batch-size must be at least 1");
        }
        if (leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("sentinel.claim.lease-duration must be positive");
        }
    }

    /** Clamps a worker's request without failing it, since asking for too much is not an error. */
    public int effectiveBatchSize(int requested) {
        return Math.max(1, Math.min(requested, maxBatchSize));
    }
}
