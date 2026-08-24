package dev.sentinel.engine.repository;

import static org.assertj.core.api.Assertions.assertThat;

import dev.sentinel.engine.AbstractIntegrationTest;
import dev.sentinel.engine.domain.ClaimedTask;
import dev.sentinel.engine.domain.FailureKind;
import dev.sentinel.engine.domain.NewTask;
import dev.sentinel.engine.domain.NewWorkflow;
import dev.sentinel.engine.domain.Task;
import dev.sentinel.engine.domain.TaskFailure;
import dev.sentinel.engine.domain.TaskStatus;
import dev.sentinel.engine.support.DagFixtures;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Every worker-driven transition, in both directions: it works for the rightful owner, and it is
 * refused for anyone else.
 *
 * <p>The refusals matter more than the successes. A guarded update that fails to reject is
 * indistinguishable from one that has no guard at all, and the symptom shows up much later as a
 * task that ran twice.
 */
class TaskLifecycleTest extends AbstractIntegrationTest {

    private static final Duration LEASE = Duration.ofSeconds(30);
    private static final TaskFailure RETRYABLE = new TaskFailure("downstream timed out", FailureKind.RETRYABLE);
    private static final TaskFailure PERMANENT = new TaskFailure("payload rejected", FailureKind.PERMANENT);

    @Autowired
    private WorkflowRepository workflows;

    @Autowired
    private TaskRepository tasks;

    // ------------------------------------------------------------------
    // Legal transitions
    // ------------------------------------------------------------------

    @Test
    @DisplayName("RUNNING to COMPLETED records output and releases the lease")
    void completeReleasesTheLease() {
        ClaimedTask claimed = claimOne();

        assertThat(tasks.markCompleted(claimed.id(), "worker-1", claimed.fencingToken(), "{\"ok\":true}")).isTrue();

        Task stored = tasks.findById(claimed.id()).orElseThrow();
        assertThat(stored.status()).isEqualTo(TaskStatus.COMPLETED);
        assertThat(stored.output()).contains("true");
        assertThat(stored.lease()).as("a terminal task must not keep a lease").isNull();
    }

    @Test
    @DisplayName("RUNNING to FAILED records the failure kind")
    void failIsTerminalAndRecordsClassification() {
        ClaimedTask claimed = claimOne();

        assertThat(tasks.markFailed(claimed.id(), "worker-1", claimed.fencingToken(), PERMANENT)).isTrue();

        Task stored = tasks.findById(claimed.id()).orElseThrow();
        assertThat(stored.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(stored.lastFailure()).isNotNull();
        assertThat(stored.lastFailure().kind()).isEqualTo(FailureKind.PERMANENT);
        assertThat(stored.lease()).isNull();
    }

    @Test
    @DisplayName("RUNNING back to PENDING pushes the next attempt into the future")
    void retryReturnsTheTaskToTheQueueAfterADelay() {
        ClaimedTask claimed = claimOne();

        assertThat(tasks.scheduleRetry(claimed.id(), "worker-1", claimed.fencingToken(), RETRYABLE,
                Duration.ofMinutes(5))).isTrue();

        Task stored = tasks.findById(claimed.id()).orElseThrow();
        assertThat(stored.status()).isEqualTo(TaskStatus.PENDING);
        assertThat(stored.lease()).isNull();
        assertThat(stored.attempt()).as("the attempt was already consumed by the claim").isEqualTo(1);
        assertThat(tasks.claimBatch("worker-2", List.of(DagFixtures.TASK_TYPE), 10, LEASE))
                .as("the backoff is enforced by the queue itself, not by an in-memory timer")
                .isEmpty();
    }

    @Test
    @DisplayName("a retried task becomes claimable again once its delay has passed")
    void retriedTaskIsClaimableAfterTheDelay() {
        ClaimedTask claimed = claimOne();
        tasks.scheduleRetry(claimed.id(), "worker-1", claimed.fencingToken(), RETRYABLE, Duration.ZERO);

        List<ClaimedTask> reclaimed = tasks.claimBatch("worker-2", List.of(DagFixtures.TASK_TYPE), 10, LEASE);

        assertThat(reclaimed).hasSize(1);
        assertThat(reclaimed.getFirst().attempt()).isEqualTo(2);
        assertThat(reclaimed.getFirst().fencingToken())
                .as("every claim advances the token, which is what invalidates the previous owner")
                .isEqualTo(2L);
    }

    @Test
    @DisplayName("heartbeats extend the lease for the rightful owner")
    void heartbeatExtendsTheLease() {
        ClaimedTask claimed = claimOne();

        assertThat(tasks.renewLease(claimed.id(), "worker-1", claimed.fencingToken(), Duration.ofMinutes(10)))
                .isTrue();

        Task stored = tasks.findById(claimed.id()).orElseThrow();
        assertThat(stored.lease().expiresAt()).isAfter(claimed.leaseExpiresAt());
    }

    // ------------------------------------------------------------------
    // Refusals
    // ------------------------------------------------------------------

    @Test
    @DisplayName("a worker holding a stale fencing token cannot complete, fail, retry or heartbeat")
    void staleFencingTokenIsRejectedOnEveryPath() {
        ClaimedTask original = claimOne();
        long staleToken = original.fencingToken();

        // Simulate the reaper taking the task back and another worker claiming it.
        expireLease(original.id());
        tasks.requeueExpiredLeases(10, Duration.ZERO);
        ClaimedTask reclaimed = tasks.claimBatch("worker-2", List.of(DagFixtures.TASK_TYPE), 10, LEASE).getFirst();
        assertThat(reclaimed.fencingToken()).isGreaterThan(staleToken);

        assertThat(tasks.renewLease(original.id(), "worker-1", staleToken, LEASE)).isFalse();
        assertThat(tasks.markCompleted(original.id(), "worker-1", staleToken, "{}")).isFalse();
        assertThat(tasks.markFailed(original.id(), "worker-1", staleToken, RETRYABLE)).isFalse();
        assertThat(tasks.scheduleRetry(original.id(), "worker-1", staleToken, RETRYABLE, Duration.ZERO)).isFalse();

        assertThat(tasks.findById(original.id()).orElseThrow().status())
                .as("the zombie's writes changed nothing")
                .isEqualTo(TaskStatus.RUNNING);
    }

    @Test
    @DisplayName("the right token presented by the wrong worker is still rejected")
    void ownershipChecksTheWorkerNotJustTheToken() {
        ClaimedTask claimed = claimOne();

        assertThat(tasks.markCompleted(claimed.id(), "some-other-worker", claimed.fencingToken(), "{}")).isFalse();
    }

    @Test
    @DisplayName("completing twice is refused the second time")
    void completingTwiceIsRefused() {
        ClaimedTask claimed = claimOne();

        assertThat(tasks.markCompleted(claimed.id(), "worker-1", claimed.fencingToken(), "{}")).isTrue();
        assertThat(tasks.markCompleted(claimed.id(), "worker-1", claimed.fencingToken(), "{}"))
                .as("an ack lost on the way back to the worker must not be able to reopen a finished task")
                .isFalse();
    }

    @Test
    @DisplayName("a task that was never claimed cannot be completed")
    void pendingTaskCannotBeCompleted() {
        var dag = DagFixtures.singleTask(workflows, tasks, "only");

        assertThat(tasks.markCompleted(dag.taskId("only"), "worker-1", 0L, "{}")).isFalse();
        assertThat(tasks.findById(dag.taskId("only")).orElseThrow().status()).isEqualTo(TaskStatus.PENDING);
    }

    @Test
    @DisplayName("a terminal task is never returned to the queue by a retry")
    void terminalTaskCannotBeRetried() {
        ClaimedTask claimed = claimOne();
        tasks.markCompleted(claimed.id(), "worker-1", claimed.fencingToken(), "{}");

        assertThat(tasks.scheduleRetry(claimed.id(), "worker-1", claimed.fencingToken(), RETRYABLE, Duration.ZERO))
                .isFalse();
    }

    @Test
    @DisplayName("a task on its final attempt cannot be retried back into the queue")
    void retryIsRefusedWhenAttemptsAreSpent() {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("last-chance")).id();
        tasks.insertAll(workflowId, List.of(
                new NewTask("once", DagFixtures.TASK_TYPE, TaskStatus.PENDING, "{}", 1, 0, null)));
        ClaimedTask claimed = tasks.claimBatch("worker-1", List.of(DagFixtures.TASK_TYPE), 10, LEASE).getFirst();

        assertThat(claimed.attempt()).isEqualTo(claimed.maxAttempts());
        assertThat(tasks.scheduleRetry(claimed.id(), "worker-1", claimed.fencingToken(), RETRYABLE, Duration.ZERO))
                .as("re-queueing here would let the claim push attempt past max_attempts")
                .isFalse();
    }

    @Test
    @DisplayName("cancelling leaves a running task alone, because its side effects may already exist")
    void cancelDoesNotTouchRunningTasks() {
        ClaimedTask claimed = claimOne();

        assertThat(tasks.cancelAll(List.of(claimed.id()))).isEmpty();
        assertThat(tasks.findById(claimed.id()).orElseThrow().status()).isEqualTo(TaskStatus.RUNNING);
    }

    @Test
    @DisplayName("cancelling applies to tasks that have not started")
    void cancelAppliesToNotYetStartedTasks() {
        var dag = DagFixtures.singleTask(workflows, tasks, "only");

        assertThat(tasks.cancelAll(List.of(dag.taskId("only")))).containsExactly(dag.taskId("only"));
        assertThat(tasks.findById(dag.taskId("only")).orElseThrow().status()).isEqualTo(TaskStatus.CANCELLED);
    }

    // ------------------------------------------------------------------
    // The transition table itself
    // ------------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(TaskStatus.class)
    @DisplayName("terminal statuses permit no onward transition at all")
    void terminalStatusesArePermanent(TaskStatus status) {
        if (status.isTerminal()) {
            assertThat(status.allowedTransitions()).isEmpty();
        } else {
            assertThat(status.allowedTransitions()).isNotEmpty();
        }
    }

    @Test
    @DisplayName("the domain's transition table matches what the repository will actually do")
    void transitionTableMatchesReality() {
        assertThat(TaskStatus.BLOCKED.canTransitionTo(TaskStatus.PENDING)).isTrue();
        assertThat(TaskStatus.BLOCKED.canTransitionTo(TaskStatus.RUNNING))
                .as("a blocked task is invisible to the claim query, so it can never go straight to RUNNING")
                .isFalse();
        assertThat(TaskStatus.PENDING.canTransitionTo(TaskStatus.RUNNING)).isTrue();
        assertThat(TaskStatus.RUNNING.canTransitionTo(TaskStatus.PENDING))
                .as("retry and lease expiry both use this edge")
                .isTrue();
        assertThat(TaskStatus.COMPLETED.canTransitionTo(TaskStatus.RUNNING)).isFalse();
        assertThat(TaskStatus.FAILED.canTransitionTo(TaskStatus.PENDING)).isFalse();
        assertThat(TaskStatus.CANCELLED.canTransitionTo(TaskStatus.PENDING)).isFalse();
    }

    // ------------------------------------------------------------------

    private ClaimedTask claimOne() {
        DagFixtures.singleTask(workflows, tasks, "only");
        return tasks.claimBatch("worker-1", List.of(DagFixtures.TASK_TYPE), 10, LEASE).getFirst();
    }

    /**
     * Pushes a lease into the past without sleeping.
     *
     * <p>Waiting out a real lease would make this suite slow and flaky. Moving the deadline is
     * honest here because the reaper's predicate is a comparison against the database clock, and
     * this changes the value that comparison reads, not the comparison itself.
     */
    private void expireLease(UUID taskId) {
        jdbcClient.sql("UPDATE tasks SET lease_expires_at = now() - interval '1 minute' WHERE id = :id")
                .param("id", taskId)
                .update();
    }
}
