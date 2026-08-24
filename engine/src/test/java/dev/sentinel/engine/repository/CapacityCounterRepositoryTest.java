package dev.sentinel.engine.repository;

import static org.assertj.core.api.Assertions.assertThat;

import dev.sentinel.engine.AbstractIntegrationTest;
import dev.sentinel.engine.domain.NewTask;
import dev.sentinel.engine.domain.NewWorkflow;
import dev.sentinel.engine.support.DagFixtures;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** The sharded counter: constant-time reads, spread writes, and tolerated drift. */
class CapacityCounterRepositoryTest extends AbstractIntegrationTest {

    @Autowired
    private CapacityCounterRepository counters;

    @Autowired
    private WorkflowRepository workflows;

    @Autowired
    private TaskRepository tasks;

    @Test
    @DisplayName("the migration seeds the configured shard count")
    void shardsAreSeeded() {
        assertThat(counters.shardCount()).isEqualTo(16);
        assertThat(counters.totalInFlight()).isZero();
    }

    @Test
    @DisplayName("the total is the sum across shards, read in one statement")
    void totalSumsEveryShard() {
        counters.addInFlight(0, 3);
        counters.addInFlight(7, 4);

        assertThat(counters.totalInFlight()).isEqualTo(7);
    }

    @Test
    @DisplayName("a completion need not return to the shard its claim incremented")
    void decrementsNeedNotMatchTheirIncrementShard() {
        counters.addInFlight(1, 5);

        counters.addInFlight(9, -5);

        assertThat(counters.totalInFlight())
                .as("only the aggregate is ever read, so per-shard values need not be individually correct")
                .isZero();
    }

    @Test
    @DisplayName("an individual shard may go negative, because decrements are not routed back")
    void anIndividualShardMayGoNegative() {
        counters.addInFlight(9, -5);

        assertThat(shardValue(9))
                .as("rejecting this would break the property that any shard can absorb any decrement")
                .isEqualTo(-5);
    }

    @Test
    @DisplayName("drift below zero is absorbed by the aggregate read, never by discarding a completion")
    void aggregateIsClampedAtZero() {
        counters.addInFlight(3, 1);

        counters.addInFlight(3, -10);

        assertThat(counters.totalInFlight()).isZero();
    }

    @Test
    @DisplayName("shard selection is deterministic for a given worker")
    void shardSelectionIsStable() {
        int first = counters.shardFor("worker-alpha", 16);
        int second = counters.shardFor("worker-alpha", 16);

        assertThat(first).isEqualTo(second).isBetween(0, 15);
    }

    @Test
    @DisplayName("reconciliation rebuilds the counter from the tasks that are actually running")
    void reconciliationRepairsDrift() {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("drift")).id();
        tasks.insertAll(workflowId, List.of(
                NewTask.runnable("a", DagFixtures.TASK_TYPE, "{}"),
                NewTask.runnable("b", DagFixtures.TASK_TYPE, "{}"),
                NewTask.runnable("c", DagFixtures.TASK_TYPE, "{}")));
        tasks.claimBatch("worker-1", List.of(DagFixtures.TASK_TYPE), 2, Duration.ofSeconds(30));
        counters.addInFlight(0, 99);

        long actual = counters.reconcileFromTasks();

        assertThat(actual).isEqualTo(2);
        assertThat(counters.totalInFlight()).isEqualTo(2);
    }

    private long shardValue(int shardId) {
        return jdbcClient.sql("SELECT in_flight FROM capacity_counters WHERE shard_id = :id")
                .param("id", shardId)
                .query(Long.class)
                .single();
    }
}
