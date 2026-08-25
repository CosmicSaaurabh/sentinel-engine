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
 * <p>The default attempt budget deliberately does not live here. It moved to
 * {@code sentinel.retry.max-attempts} when retry became configurable per task type, because two
 * places to set the same number is a trap: whichever one an operator edits, the other looks like it
 * should have worked.
 *
 * @param maxTasks tasks per workflow
 * @param maxEdges dependency edges per workflow
 */
@ConfigurationProperties("sentinel.dag")
public record DagProperties(
        @DefaultValue("1000") int maxTasks,
        @DefaultValue("10000") int maxEdges) {

    public DagProperties {
        if (maxTasks < 1) {
            throw new IllegalArgumentException("sentinel.dag.max-tasks must be at least 1");
        }
        if (maxEdges < 0) {
            throw new IllegalArgumentException("sentinel.dag.max-edges must not be negative");
        }
    }
}
