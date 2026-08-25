package dev.sentinel.engine.repository;

import static org.assertj.core.api.Assertions.assertThat;

import dev.sentinel.engine.AbstractIntegrationTest;
import dev.sentinel.engine.domain.ClaimedTask;
import dev.sentinel.engine.domain.NewTask;
import dev.sentinel.engine.domain.NewWorkflow;
import dev.sentinel.engine.domain.Task;
import dev.sentinel.engine.domain.TaskStatus;
import dev.sentinel.engine.support.DagFixtures;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Lease expiry: the mechanism that turns "a worker disappeared" into "the task runs somewhere
 * else", without ever needing to know why the worker went quiet.
 */
class LeaseReaperTest extends AbstractIntegrationTest {

    private static final Duration LEASE = Duration.ofSeconds(30);

    @Autowired
    private WorkflowRepository workflows;

    @Autowired
    private TaskRepository tasks;

    @Test
    @DisplayName("an expired lease returns the task to the queue")
    void expiredLeaseIsRequeued() {
        ClaimedTask claimed = claimOne(10);
        expireLease(claimed.id());

        List<TaskRepository.ExpiredLease> requeued = tasks.requeueExpiredLeases(100, Duration.ZERO);

        assertThat(requeued).extracting(TaskRepository.ExpiredLease::taskId).containsExactly(claimed.id());
        assertThat(requeued.getFirst().ownerWorkerId())
                .as("who was holding the lease is the detail that turns \"a task was reaped\" into "
                        + "\"this worker stopped reporting\", and it is gone from the row once released")
                .isEqualTo("worker-1");
        assertThat(requeued.getFirst().attempt()).isEqualTo(1);
        Task stored = tasks.findById(claimed.id()).orElseThrow();
        assertThat(stored.status()).isEqualTo(TaskStatus.PENDING);
        assertThat(stored.lease()).isNull();
        assertThat(stored.attempt()).as("the failed attempt stays counted").isEqualTo(1);
    }

    @Test
    @DisplayName("a live lease is left alone")
    void liveLeaseIsUntouched() {
        ClaimedTask claimed = claimOne(10);

        assertThat(tasks.requeueExpiredLeases(100, Duration.ZERO)).isEmpty();
        assertThat(tasks.findById(claimed.id()).orElseThrow().status()).isEqualTo(TaskStatus.RUNNING);
    }

    @Test
    @DisplayName("a re-queued task can be claimed by a different worker with a fresh token")
    void requeuedTaskIsClaimableByAnotherWorker() {
        ClaimedTask original = claimOne(10);
        expireLease(original.id());
        tasks.requeueExpiredLeases(100, Duration.ZERO);

        ClaimedTask reclaimed = tasks.claimBatch("worker-2", List.of(DagFixtures.TASK_TYPE), 10, LEASE).getFirst();

        assertThat(reclaimed.id()).as("the task keeps its identity, so the client's idempotency key still holds")
                .isEqualTo(original.id());
        assertThat(reclaimed.fencingToken()).isGreaterThan(original.fencingToken());
    }

    @Test
    @DisplayName("a task whose attempts are spent fails instead of looping forever")
    void exhaustedTaskIsFailedRatherThanRequeued() {
        ClaimedTask claimed = claimOne(1);
        expireLease(claimed.id());

        assertThat(tasks.requeueExpiredLeases(100, Duration.ZERO))
                .as("re-queueing would let a task that kills its worker consume the fleet forever")
                .isEmpty();
        assertThat(tasks.failExhaustedExpiredLeases(100))
                .extracting(TaskRepository.ExpiredLease::taskId)
                .containsExactly(claimed.id());
        assertThat(tasks.findById(claimed.id()).orElseThrow().status()).isEqualTo(TaskStatus.FAILED);
    }

    @Test
    @DisplayName("the reaper takes a bounded batch, so mass expiry cannot become one huge transaction")
    void reaperBatchIsBounded() {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("mass-expiry")).id();
        tasks.insertAll(workflowId, List.of(
                NewTask.runnable("a", DagFixtures.TASK_TYPE, "{}"),
                NewTask.runnable("b", DagFixtures.TASK_TYPE, "{}"),
                NewTask.runnable("c", DagFixtures.TASK_TYPE, "{}"),
                NewTask.runnable("d", DagFixtures.TASK_TYPE, "{}")));
        tasks.claimBatch("worker-1", List.of(DagFixtures.TASK_TYPE), 10, LEASE)
                .forEach(claimed -> expireLease(claimed.id()));

        assertThat(tasks.requeueExpiredLeases(2, Duration.ZERO)).hasSize(2);
        assertThat(tasks.requeueExpiredLeases(2, Duration.ZERO)).hasSize(2);
        assertThat(tasks.requeueExpiredLeases(2, Duration.ZERO)).isEmpty();
    }

    @Test
    @DisplayName("the re-queue delay is jittered per task, so a recovered batch does not stampede")
    void requeueDelayIsJitteredPerTask() {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("stampede")).id();
        tasks.insertAll(workflowId, java.util.stream.IntStream.range(0, 20)
                .mapToObj(i -> NewTask.runnable("task-" + i, DagFixtures.TASK_TYPE, "{}"))
                .toList());
        tasks.claimBatch("worker-1", List.of(DagFixtures.TASK_TYPE), 20, LEASE)
                .forEach(claimed -> expireLease(claimed.id()));

        tasks.requeueExpiredLeases(50, Duration.ofMinutes(10));

        long distinctTimes = jdbcClient.sql(
                "SELECT count(DISTINCT next_attempt_at) FROM tasks WHERE workflow_id = :id")
                .param("id", workflowId)
                .query(Long.class)
                .single();
        assertThat(distinctTimes)
                .as("one delay for the whole batch would just move the stampede ten minutes later")
                .isGreaterThan(15);
    }

    @Test
    @DisplayName("a re-queue delay is applied, so a whole failed batch does not stampede straight back")
    void requeueDelayIsApplied() {
        ClaimedTask claimed = claimOne(10);
        expireLease(claimed.id());

        tasks.requeueExpiredLeases(100, Duration.ofMinutes(10));

        assertThat(tasks.claimBatch("worker-2", List.of(DagFixtures.TASK_TYPE), 10, LEASE)).isEmpty();
    }

    private ClaimedTask claimOne(int maxAttempts) {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("reaper")).id();
        tasks.insertAll(workflowId, List.of(
                new NewTask("only", DagFixtures.TASK_TYPE, TaskStatus.PENDING, "{}", maxAttempts, 0, null)));
        return tasks.claimBatch("worker-1", List.of(DagFixtures.TASK_TYPE), 10, LEASE).getFirst();
    }

    private void expireLease(UUID taskId) {
        jdbcClient.sql("UPDATE tasks SET lease_expires_at = now() - interval '1 minute' WHERE id = :id")
                .param("id", taskId)
                .update();
    }
}
