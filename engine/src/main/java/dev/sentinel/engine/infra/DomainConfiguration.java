package dev.sentinel.engine.infra;

import dev.sentinel.engine.domain.DagValidator;
import dev.sentinel.engine.domain.ExponentialBackoffRetryPolicy;
import dev.sentinel.engine.domain.RetryPolicy;
import dev.sentinel.engine.domain.RetryPolicyResolver;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    public DagValidator dagValidator(DagProperties properties, RetryProperties retryProperties) {
        return new DagValidator(properties.maxTasks(), properties.maxEdges(), retryProperties::maxAttemptsFor);
    }

    /**
     * Resolves the retry policy for a task type, caching one policy per type.
     *
     * <p>Cached because a policy is immutable and building one per failure would allocate for no
     * reason, and because the cache is bounded by the number of distinct task types a deployment
     * actually uses rather than by traffic.
     *
     * <p>Jitter is drawn from a per-thread generator. The retry path runs on every request thread
     * at once, and a single shared generator would have all of them contending on one seed to
     * produce a number whose exact value nobody cares about. {@link ThreadLocalRandom} gives each
     * thread its own.
     */
    @Bean
    public RetryPolicyResolver retryPolicyResolver(RetryProperties properties) {
        RandomGenerator perThread = () -> ThreadLocalRandom.current().nextLong();
        Map<String, RetryPolicy> cache = new ConcurrentHashMap<>();
        return taskType -> cache.computeIfAbsent(taskType, type -> new ExponentialBackoffRetryPolicy(
                properties.baseDelayFor(type), properties.maxDelayFor(type), perThread));
    }
}
