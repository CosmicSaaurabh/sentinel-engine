package dev.sentinel.engine.infra;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Backpressure at the submission boundary.
 *
 * @param enabled admission control can be turned off for local experiments, but never in an
 *        environment that matters. Without it the engine accepts work indefinitely and converts a
 *        visible rejection into an invisible backlog
 * @param totalSlots how many tasks may be in flight across the whole fleet before the engine starts
 *        refusing new workflows
 * @param retryAfter base hint returned to a rejected client, jittered before it is sent so a
 *        rejected fleet does not come back in one synchronised wave
 */
@ConfigurationProperties("sentinel.admission")
public record AdmissionProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("10000") long totalSlots,
        @DefaultValue("5s") Duration retryAfter) {

    public AdmissionProperties {
        if (totalSlots < 1) {
            throw new IllegalArgumentException("sentinel.admission.total-slots must be at least 1");
        }
    }
}
