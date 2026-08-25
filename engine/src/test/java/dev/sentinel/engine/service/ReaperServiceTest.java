package dev.sentinel.engine.service;

import static org.assertj.core.api.Assertions.assertThat;

import dev.sentinel.engine.AbstractIntegrationTest;
import dev.sentinel.engine.domain.ClaimedTask;
import dev.sentinel.engine.domain.FailureRecordKind;
import dev.sentinel.engine.domain.NewTask;
import dev.sentinel.engine.domain.NewWorkflow;
import dev.sentinel.engine.domain.OutboxEvent;
import dev.sentinel.engine.domain.OutboxEventType;
import dev.sentinel.engine.domain.RecordedFailure;
import dev.sentinel.engine.domain.Task;
import dev.sentinel.engine.domain.TaskStatus;
import dev.sentinel.engine.domain.WorkflowStatus;
import dev.sentinel.engine.repository.CapacityCounterRepository;
import dev.sentinel.engine.repository.OutboxRepository;
import dev.sentinel.engine.repository.TaskFailureRepository;
import dev.sentinel.engine.repository.TaskRepository;
import dev.sentinel.engine.repository.WorkflowRepository;
import dev.sentinel.engine.support.Workflows;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Lease recovery, driven a pass at a time.
 *
 * <p>The reaper is what makes a lease mean anything. Without it {@code lease_expires_at} is a column
 * nobody reads, and a task claimed by a worker that then dies stays {@code RUNNING} forever with no
 * error anywhere. Phase 2 hit exactly that, which is why several of these tests describe scenarios
 * that were previously unrecoverable rather than merely untested.
 */
class ReaperServiceTest extends AbstractIntegrationTest {

    private static final Duration LEASE = Duration.ofSeconds(30);

    @Autowired
    private ReaperService reaper;

    @Autowired
    private WorkflowSubmissionService submissionService;

    @Autowired
    private TaskClaimService claimService;

    @Autowired
    private TaskRepository tasks;

    @Autowired
    private TaskFailureRepository failures;

    @Autowired
    private WorkflowRepository workflows;

    @Autowired
    private OutboxRepository outbox;

    @Autowired
    private CapacityCounterRepository capacity;

    private final ExecutorService reapers = Executors.newFixedThreadPool(4, runnable -> {
        Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        return thread;
    });

    @AfterEach
    void shutdownReapers() throws InterruptedException {
        reapers.shutdownNow();
        assertThat(reapers.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }

    // ------------------------------------------------------------------
    // Recovery
    // ------------------------------------------------------------------

    @Test
    @DisplayName("a task whose worker went silent is returned to the queue")
    void anExpiredLeaseIsRecovered() {
        submissionService.submit(Workflows.singleTask());
        ClaimedTask claimed = claimOne("worker-1");
        expireLease(claimed.id());

        ReaperService.ReapResult result = reaper.reapOnce();

        assertThat(result.requeued()).isEqualTo(1);
        assertThat(result.failed()).isZero();
        Task recovered = tasks.findById(claimed.id()).orElseThrow();
        assertThat(recovered.status()).isEqualTo(TaskStatus.PENDING);
        assertThat(recovered.lease()).isNull();
    }

    @Test
    @DisplayName("the recovered task is claimable by a different worker with a fresh fencing token")
    void aRecoveredTaskRunsElsewhere() {
        submissionService.submit(Workflows.singleTask());
        ClaimedTask original = claimOne("worker-1");
        expireLease(original.id());
        reaper.reapOnce();
        makeClaimableNow(original.id());

        ClaimedTask reclaimed = claimOne("worker-2");

        assertThat(reclaimed.id())
                .as("the task keeps its identity, so the client's idempotency key still applies")
                .isEqualTo(original.id());
        assertThat(reclaimed.fencingToken()).isGreaterThan(original.fencingToken());
        assertThat(reclaimed.attempt()).isEqualTo(2);
    }

    @Test
    @DisplayName("a live lease is left alone")
    void aLiveLeaseIsUntouched() {
        submissionService.submit(Workflows.singleTask());
        ClaimedTask claimed = claimOne("worker-1");

        assertThat(reaper.reapOnce().isEmpty()).isTrue();
        assertThat(tasks.findById(claimed.id()).orElseThrow().status()).isEqualTo(TaskStatus.RUNNING);
    }

    @Test
    @DisplayName("an empty pass does nothing and says so")
    void anEmptyPassIsANoOp() {
        ReaperService.ReapResult result = reaper.reapOnce();

        assertThat(result.isEmpty()).isTrue();
        assertThat(result.sawFullBatch()).isFalse();
    }

    // ------------------------------------------------------------------
    // The ordering that is easy to get backwards
    // ------------------------------------------------------------------

    @Test
    @DisplayName("a task with no attempts left is failed, never re-queued into a state it cannot leave")
    void anExhaustedTaskIsFailedRatherThanRequeued() {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("last-chance")).id();
        tasks.insertAll(workflowId, List.of(
                new NewTask("only", Workflows.TASK_TYPE, TaskStatus.PENDING, "{}", 1, 0, null)));
        ClaimedTask claimed = claimOne("worker-1");
        expireLease(claimed.id());

        ReaperService.ReapResult result = reaper.reapOnce();

        assertThat(result.failed()).isEqualTo(1);
        assertThat(result.requeued()).isZero();
        assertThat(tasks.findById(claimed.id()).orElseThrow().status())
                .as("re-queueing here would make the task fail its own claim predicate and become "
                        + "invisible in the queue forever")
                .isEqualTo(TaskStatus.FAILED);
    }

    @Test
    @DisplayName("a mixed batch is handled correctly: exhausted tasks fail, the rest are re-queued")
    void aMixedBatchSplitsCorrectly() {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("mixed")).id();
        tasks.insertAll(workflowId, List.of(
                new NewTask("doomed", Workflows.TASK_TYPE, TaskStatus.PENDING, "{}", 1, 0, null),
                new NewTask("recoverable", Workflows.TASK_TYPE, TaskStatus.PENDING, "{}", 10, 0, null)));
        claimService.claim(new ClaimRequest("worker-1", List.of(Workflows.TASK_TYPE), 10))
                .forEach(claimed -> expireLease(claimed.id()));

        ReaperService.ReapResult result = reaper.reapOnce();

        assertThat(result.failed()).isEqualTo(1);
        assertThat(result.requeued()).isEqualTo(1);
        assertThat(tasks.countByStatus(workflowId))
                .containsEntry(TaskStatus.FAILED, 1)
                .containsEntry(TaskStatus.CANCELLED, 1);
    }

    // ------------------------------------------------------------------
    // What the reaper records
    // ------------------------------------------------------------------

    @Test
    @DisplayName("recovery records who held the lease, on which attempt, as LEASE_EXPIRED")
    void recoveryRecordsTheCause() {
        submissionService.submit(Workflows.singleTask());
        ClaimedTask claimed = claimOne("worker-7");
        expireLease(claimed.id());

        reaper.reapOnce();

        List<RecordedFailure> chain = failures.findByTaskId(claimed.id());
        assertThat(chain).hasSize(1);
        RecordedFailure recorded = chain.getFirst();
        assertThat(recorded.kind())
                .as("nobody reported this, which is a different problem from a reported failure")
                .isEqualTo(FailureRecordKind.LEASE_EXPIRED);
        assertThat(recorded.workerId()).isEqualTo("worker-7");
        assertThat(recorded.attempt()).isEqualTo(1);
        assertThat(recorded.failedAt()).isNotNull();
    }

    @Test
    @DisplayName("the failure record is bounded at one row per attempt")
    void oneRecordPerAttempt() {
        submissionService.submit(Workflows.singleTask());
        ClaimedTask claimed = claimOne("worker-1");
        expireLease(claimed.id());

        reaper.reapOnce();
        // A second pass finds nothing, but even if the same attempt were offered twice the unique
        // constraint would absorb it rather than failing the surrounding transaction.
        reaper.reapOnce();

        assertThat(failures.countForTask(claimed.id())).isEqualTo(1);
    }

    @Test
    @DisplayName("recovery is recorded in history, saying which outcome it was")
    void recoveryIsRecordedInHistory() {
        var workflow = submissionService.submit(Workflows.singleTask()).workflow();
        ClaimedTask claimed = claimOne("worker-1");
        expireLease(claimed.id());

        reaper.reapOnce();

        assertThat(outbox.findByWorkflowId(workflow.id()))
                .filteredOn(event -> event.eventType() == OutboxEventType.TASK_LEASE_EXPIRED)
                .extracting(OutboxEvent::payload)
                .allSatisfy(payload -> assertThat(payload).contains("requeued"));
    }

    @Test
    @DisplayName("recovery returns the capacity the dead worker was holding")
    void recoveryReleasesCapacity() {
        submissionService.submit(Workflows.singleTask());
        ClaimedTask claimed = claimOne("worker-1");
        assertThat(capacity.totalInFlight()).isEqualTo(1);
        expireLease(claimed.id());

        reaper.reapOnce();

        assertThat(capacity.totalInFlight())
                .as("capacity held by a task nobody is running would leak until the engine restarted")
                .isZero();
    }

    @Test
    @DisplayName("a terminally expired task fails its workflow and cancels what can no longer run")
    void terminalExpiryFailsTheWorkflow() {
        var workflow = submissionService.submit(Workflows.chain("first", "second")).workflow();
        jdbcClient.sql("UPDATE tasks SET max_attempts = 1 WHERE workflow_id = :id")
                .param("id", workflow.id())
                .update();
        ClaimedTask claimed = claimOne("worker-1");
        expireLease(claimed.id());

        reaper.reapOnce();

        assertThat(workflows.findById(workflow.id()).orElseThrow().status())
                .isEqualTo(WorkflowStatus.FAILED);
        assertThat(tasks.countByStatus(workflow.id())).containsEntry(TaskStatus.CANCELLED, 1);
    }

    // ------------------------------------------------------------------
    // Leaderless
    // ------------------------------------------------------------------

    @Test
    @DisplayName("several engine instances reaping at once divide the work instead of duplicating it")
    void concurrentReapersDivideTheWork() throws Exception {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("mass-expiry")).id();
        tasks.insertAll(workflowId, java.util.stream.IntStream.range(0, 60)
                .mapToObj(i -> NewTask.runnable("task-" + i, Workflows.TASK_TYPE, "{}"))
                .toList());
        // Claimed through the repository rather than the service, because the service clamps a
        // poll to sentinel.claim.max-batch-size and this test needs sixty leases in flight, not a
        // realistic worker poll.
        tasks.claimBatch("worker-1", List.of(Workflows.TASK_TYPE), 60, LEASE)
                .forEach(claimed -> expireLease(claimed.id()));

        int reaperCount = 4;
        CyclicBarrier allReady = new CyclicBarrier(reaperCount);
        AtomicInteger totalRequeued = new AtomicInteger();
        List<Future<?>> passes = new java.util.ArrayList<>();

        for (int i = 0; i < reaperCount; i++) {
            passes.add(reapers.submit(() -> {
                try {
                    allReady.await(10, TimeUnit.SECONDS);
                    totalRequeued.addAndGet(reaper.reapOnce().requeued());
                } catch (Exception e) {
                    throw new IllegalStateException("reaper pass failed", e);
                }
            }));
        }
        for (Future<?> pass : passes) {
            pass.get(30, TimeUnit.SECONDS);
        }

        assertThat(totalRequeued.get())
                .as("SKIP LOCKED means concurrent reapers split the batch; recovering a task twice "
                        + "would mean the guarded update matched twice, which it cannot")
                .isEqualTo(60);
        assertThat(tasks.countByStatus(workflowId)).containsEntry(TaskStatus.PENDING, 60);
    }

    @Test
    @DisplayName("a pass that fills its batch says so, so the backlog is drained rather than trickled")
    void afullBatchIsSignalled() {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("backlog")).id();
        tasks.insertAll(workflowId, java.util.stream.IntStream.range(0, 250)
                .mapToObj(i -> NewTask.runnable("task-" + i, Workflows.TASK_TYPE, "{}"))
                .toList());
        tasks.claimBatch("worker-1", List.of(Workflows.TASK_TYPE), 250, LEASE)
                .forEach(claimed -> expireLease(claimed.id()));

        ReaperService.ReapResult first = reaper.reapOnce();

        assertThat(first.requeued())
                .as("the batch bounds the transaction, which is what stops a mass expiry locking "
                        + "thousands of rows at once")
                .isEqualTo(200);
        assertThat(first.sawFullBatch()).isTrue();
        assertThat(reaper.reapOnce().requeued()).isEqualTo(50);
    }

    // ------------------------------------------------------------------

    private ClaimedTask claimOne(String workerId) {
        List<ClaimedTask> claimed =
                claimService.claim(new ClaimRequest(workerId, List.of(Workflows.TASK_TYPE), 1));
        assertThat(claimed).hasSize(1);
        return claimed.getFirst();
    }

    /**
     * Moves a lease into the past instead of waiting one out.
     *
     * <p>Honest because the reaper's predicate is a comparison against the database clock; this
     * changes the value being compared, not the comparison.
     */
    private void expireLease(UUID taskId) {
        jdbcClient.sql("UPDATE tasks SET lease_expires_at = now() - interval '1 minute' WHERE id = :id")
                .param("id", taskId)
                .update();
    }

    private void makeClaimableNow(UUID taskId) {
        jdbcClient.sql("UPDATE tasks SET next_attempt_at = now() - interval '1 second' WHERE id = :id")
                .param("id", taskId)
                .update();
    }
}
