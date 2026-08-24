package dev.sentinel.engine.infra;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Bounds on what a single submission may contain.
 *
 * <p>These are not arbitrary tidiness limits. Submission inserts a whole DAG inside one
 * transaction, and an unbounded DAG would mean unbounded work holding a transaction open, plus
 * unbounded SQL text to parse. A rejected oversized submission is a clear error; an accepted one is
 * a latency spike that nobody can attribute.
 *
 * @param maxTasks tasks per workflow
 * @param maxEdges dependency edges per workflow
 * @param defaultMaxAttempts used when a task does not specify its own
 */
@ConfigurationProperties("sentinel.dag")
public record DagProperties(
        @DefaultValue("1000") int maxTasks,
        @DefaultValue("10000") int maxEdges,
        @DefaultValue("10") int defaultMaxAttempts) {

    public DagProperties {
        if (maxTasks < 1) {
            throw new IllegalArgumentException("sentinel.dag.max-tasks must be at least 1");
        }
        if (maxEdges < 0) {
            throw new IllegalArgumentException("sentinel.dag.max-edges must not be negative");
        }
        if (defaultMaxAttempts < 1) {
            throw new IllegalArgumentException("sentinel.dag.default-max-attempts must be at least 1");
        }
    }
}
