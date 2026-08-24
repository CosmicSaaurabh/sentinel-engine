package dev.sentinel.engine.infra;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Backoff schedule for retryable task failures.
 *
 * @param baseDelay first un-jittered delay; each subsequent attempt doubles it
 * @param maxDelay ceiling, so a long-lived task with many attempts does not end up waiting hours
 */
@ConfigurationProperties("sentinel.retry")
public record RetryProperties(
        @DefaultValue("2s") Duration baseDelay,
        @DefaultValue("5m") Duration maxDelay) {
}
