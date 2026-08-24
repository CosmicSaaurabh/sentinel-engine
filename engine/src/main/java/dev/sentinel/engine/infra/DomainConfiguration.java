package dev.sentinel.engine.infra;

import dev.sentinel.engine.domain.DagValidator;
import dev.sentinel.engine.domain.ExponentialBackoffRetryPolicy;
import dev.sentinel.engine.domain.RetryPolicy;
import java.util.concurrent.ThreadLocalRandom;
import java.util.random.RandomGenerator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the framework-free domain classes into the Spring context.
 *
 * <p>{@link DagValidator} and {@link RetryPolicy} know nothing about Spring, configuration binding
 * or the database, which is what lets them be tested as plain objects in microseconds. This is the
 * one place that knows both worlds, so the dependency arrow keeps pointing inward.
 */
@Configuration(proxyBeanMethods = false)
public class DomainConfiguration {

    @Bean
    public DagValidator dagValidator(DagProperties properties) {
        return new DagValidator(properties.maxTasks(), properties.maxEdges(), properties.defaultMaxAttempts());
    }

    /**
     * Jitter is drawn from a per-thread generator.
     *
     * <p>The retry path runs on every request thread at once. A single shared generator would have
     * all of them contending on one seed to produce a number whose exact value nobody cares about,
     * which is pure contention for no benefit. {@link ThreadLocalRandom} gives each thread its own.
     */
    @Bean
    public RetryPolicy retryPolicy(RetryProperties properties) {
        RandomGenerator perThread = () -> ThreadLocalRandom.current().nextLong();
        return new ExponentialBackoffRetryPolicy(properties.baseDelay(), properties.maxDelay(), perThread);
    }
}
