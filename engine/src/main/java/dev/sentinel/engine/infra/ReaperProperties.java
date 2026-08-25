package dev.sentinel.engine.infra;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Pacing for lease recovery.
 *
 * @param enabled the reaper can be turned off for tests that drive it by hand. Turning it off in a
 *        real environment means nothing ever recovers a dead worker's task
 * @param interval how often a pass runs. Detection latency is dominated by the lease duration, so
 *        this only has to be comfortably below it; running it faster buys almost nothing
 * @param jitter fraction by which each interval is randomised, so several engine instances do not
 *        settle into reaping in lockstep
 * @param batchSize tasks per pass. Bounds the transaction size and the lock footprint, which is
 *        what stops a mass expiry from becoming one enormous transaction that blocks every claimer
 * @param requeueDelay base delay before a recovered task becomes claimable again, jittered per task
 *        inside the SQL so a recovered batch does not all become claimable at the same instant
 */
@ConfigurationProperties("sentinel.reaper")
public record ReaperProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("5s") Duration interval,
        @DefaultValue("0.4") double jitter,
        @DefaultValue("200") int batchSize,
        @DefaultValue("5s") Duration requeueDelay) {

    public ReaperProperties {
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("sentinel.reaper.interval must be positive");
        }
        if (jitter < 0 || jitter >= 1) {
            throw new IllegalArgumentException("sentinel.reaper.jitter must be in [0, 1)");
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException("sentinel.reaper.batch-size must be at least 1");
        }
    }
}
