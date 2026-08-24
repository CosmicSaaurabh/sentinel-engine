package dev.sentinel.engine.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.sentinel.engine.AbstractIntegrationTest;
import dev.sentinel.engine.domain.ClaimedTask;
import dev.sentinel.engine.domain.FailureKind;
import dev.sentinel.engine.domain.OutboxEvent;
import dev.sentinel.engine.domain.OutboxEventType;
import dev.sentinel.engine.domain.Task;
import dev.sentinel.engine.domain.TaskDefinition;
import dev.sentinel.engine.domain.TaskStatus;
import dev.sentinel.engine.domain.Workflow;
import dev.sentinel.engine.domain.WorkflowDefinition;
import dev.sentinel.engine.domain.WorkflowStatus;
import dev.sentinel.engine.error.StaleFencingTokenException;
import dev.sentinel.engine.repository.CapacityCounterRepository;
import dev.sentinel.engine.repository.OutboxRepository;
import dev.sentinel.engine.repository.TaskRepository;
import dev.sentinel.engine.repository.WorkflowRepository;
import dev.sentinel.engine.support.Workflows;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** Reporting back: success, failure, retry, and the reports the engine refuses. */
class TaskCompletionServiceTest extends AbstractIntegrationTest {

    @Autowired
    private WorkflowSubmissionService submissionService;

    @Autowired
    private TaskClaimService claimService;

    @Autowired
    private TaskCompletionService completionService;

    @Autowired
    private TaskRepository tasks;

    @Autowired
    private WorkflowRepository workflows;

    @Autowired
    private OutboxRepository outbox;

    @Autowired
    private CapacityCounterRepository capacity;

    // ------------------------------------------------------------------
    // Success
    // ------------------------------------------------------------------

    @Test
    @DisplayName("completing a workflow's only task settles the workflow too")
    void completingTheLastTaskSettlesTheWorkflow() {
        Workflow workflow = submissionService.submit(Workflows.singleTask()).workflow();
        ClaimedTask claimed = claimOne();

        assertThat(completionService.complete(claimed.id(), "worker-1", claimed.fencingToken(), "{\"ok\":1}"))
                .isEqualTo(CompletionOutcome.RECORDED);

        assertThat(workflows.findById(workflow.id()).orElseThrow().status()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(outbox.findByWorkflowId(workflow.id())).extracting(OutboxEvent::eventType)
                .contains(OutboxEventType.TASK_COMPLETED, OutboxEventType.WORKFLOW_COMPLETED);
    }

    @Test
    @DisplayName("completing gives the task slot back to admission control")
    void completingReleasesCapacity() {
        submissionService.submit(Workflows.singleTask()).workflow();
        ClaimedTask claimed = claimOne();
        assertThat(capacity.totalInFlight()).isEqualTo(1);

        completionService.complete(claimed.id(), "worker-1", claimed.fencingToken(), "{}");

        assertThat(capacity.totalInFlight()).isZero();
    }

    @Test
    @DisplayName("a duplicate report from the rightful owner is accepted as already recorded")
    void duplicateReportIsNotAnError() {
        submissionService.submit(Workflows.singleTask()).workflow();
        ClaimedTask claimed = claimOne();
        completionService.complete(claimed.id(), "worker-1", claimed.fencingToken(), "{}");

        assertThat(completionService.complete(claimed.id(), "worker-1", claimed.fencingToken(), "{}"))
                .as("an ack lost on the way back makes an honest worker report twice")
                .isEqualTo(CompletionOutcome.ALREADY_RECORDED);
    }

    // ------------------------------------------------------------------
    // Refusals
    // ------------------------------------------------------------------

    @Test
    @DisplayName("a worker that lost its lease is refused, and told so explicitly")
    void fencedWorkerIsRefused() {
        submissionService.submit(Workflows.singleTask()).workflow();
        ClaimedTask original = claimOne();
        expireLease(original.id());
        tasks.requeueExpiredLeases(10, Duration.ZERO);
        ClaimedTask reclaimed = claimOne();

        assertThatThrownBy(() -> completionService.complete(
                original.id(), "worker-1", original.fencingToken(), "{}"))
                .isInstanceOf(StaleFencingTokenException.class)
                .hasMessageContaining("the lease was lost");

        assertThat(tasks.findById(original.id()).orElseThrow().status())
                .as("the zombie's report changed nothing")
                .isEqualTo(TaskStatus.RUNNING);
        assertThat(reclaimed.fencingToken()).isGreaterThan(original.fencingToken());
    }

    @Test
    @DisplayName("a heartbeat from a fenced worker is refused, which is its signal to stop working")
    void fencedHeartbeatIsRefused() {
        submissionService.submit(Workflows.singleTask()).workflow();
        ClaimedTask original = claimOne();
        expireLease(original.id());
        tasks.requeueExpiredLeases(10, Duration.ZERO);
        claimOne();

        assertThatThrownBy(() -> completionService.heartbeat(
                original.id(), "worker-1", original.fencingToken()))
                .isInstanceOf(StaleFencingTokenException.class);
    }

    @Test
    @DisplayName("a heartbeat from the rightful owner extends the lease")
    void heartbeatExtendsTheLease() {
        submissionService.submit(Workflows.singleTask()).workflow();
        ClaimedTask claimed = claimOne();

        completionService.heartbeat(claimed.id(), "worker-1", claimed.fencingToken());

        assertThat(tasks.findById(claimed.id()).orElseThrow().lease().expiresAt())
                .isAfterOrEqualTo(claimed.leaseExpiresAt());
    }

    // ------------------------------------------------------------------
    // Failure and retry
    // ------------------------------------------------------------------

    @Test
    @DisplayName("a retryable failure returns the task to the queue behind a backoff")
    void retryableFailureIsRescheduled() {
        Workflow workflow = submissionService.submit(Workflows.singleTask()).workflow();
        ClaimedTask claimed = claimOne();

        completionService.fail(claimed.id(), "worker-1", claimed.fencingToken(),
                FailureKind.RETRYABLE, "downstream timed out");

        Task stored = tasks.findById(claimed.id()).orElseThrow();
        assertThat(stored.status()).isEqualTo(TaskStatus.PENDING);
        assertThat(stored.lastFailure().message()).isEqualTo("downstream timed out");
        assertThat(workflows.findById(workflow.id()).orElseThrow().status())
                .as("a retryable failure has not failed anything yet")
                .isEqualTo(WorkflowStatus.RUNNING);
        assertThat(outbox.findByWorkflowId(workflow.id())).extracting(OutboxEvent::eventType)
                .contains(OutboxEventType.TASK_RETRY_SCHEDULED);
    }

    @Test
    @DisplayName("a permanent failure goes terminal immediately, however many attempts remain")
    void permanentFailureSkipsRetries() {
        Workflow workflow = submissionService.submit(Workflows.singleTask()).workflow();
        ClaimedTask claimed = claimOne();
        assertThat(claimed.attempt()).isLessThan(claimed.maxAttempts());

        completionService.fail(claimed.id(), "worker-1", claimed.fencingToken(),
                FailureKind.PERMANENT, "payload rejected");

        assertThat(tasks.findById(claimed.id()).orElseThrow().status()).isEqualTo(TaskStatus.FAILED);
        assertThat(workflows.findById(workflow.id()).orElseThrow().status()).isEqualTo(WorkflowStatus.FAILED);
    }

    @Test
    @DisplayName("a retryable failure goes terminal once the attempt budget is spent")
    void retriesStopWhenAttemptsRunOut() {
        Workflow workflow = submissionService.submit(new WorkflowDefinition("one-shot", null, null,
                List.of(new TaskDefinition("only", Workflows.TASK_TYPE, "{}", 1, List.of())))).workflow();
        ClaimedTask claimed = claimOne();

        completionService.fail(claimed.id(), "worker-1", claimed.fencingToken(),
                FailureKind.RETRYABLE, "still broken");

        assertThat(tasks.findById(claimed.id()).orElseThrow().status())
                .as("a task that keeps failing must eventually stop consuming the fleet")
                .isEqualTo(TaskStatus.FAILED);
        assertThat(workflows.findById(workflow.id()).orElseThrow().status()).isEqualTo(WorkflowStatus.FAILED);
    }

    @Test
    @DisplayName("failing gives the task slot back, exactly as completing does")
    void failingReleasesCapacity() {
        submissionService.submit(Workflows.singleTask()).workflow();
        ClaimedTask claimed = claimOne();

        completionService.fail(claimed.id(), "worker-1", claimed.fencingToken(),
                FailureKind.PERMANENT, "nope");

        assertThat(capacity.totalInFlight())
                .as("a slot held by a failed task would leak capacity until the engine restarted")
                .isZero();
    }

    @Test
    @DisplayName("a retried task can be claimed again with a fresh fencing token")
    void retriedTaskRunsAgain() {
        submissionService.submit(Workflows.singleTask()).workflow();
        ClaimedTask first = claimOne();
        completionService.fail(first.id(), "worker-1", first.fencingToken(), FailureKind.RETRYABLE, "blip");
        makeClaimableNow(first.id());

        ClaimedTask second = claimOne();

        assertThat(second.id()).isEqualTo(first.id());
        assertThat(second.attempt()).isEqualTo(2);
        assertThat(second.fencingToken()).isGreaterThan(first.fencingToken());
    }

    // ------------------------------------------------------------------

    private ClaimedTask claimOne() {
        List<ClaimedTask> claimed =
                claimService.claim(new ClaimRequest("worker-1", List.of(Workflows.TASK_TYPE), 10));
        assertThat(claimed).hasSize(1);
        return claimed.getFirst();
    }

    private void expireLease(UUID taskId) {
        jdbcClient.sql("UPDATE tasks SET lease_expires_at = now() - interval '1 minute' WHERE id = :id")
                .param("id", taskId)
                .update();
    }

    /** Skips past a backoff without sleeping, by moving the deadline rather than the clock. */
    private void makeClaimableNow(UUID taskId) {
        jdbcClient.sql("UPDATE tasks SET next_attempt_at = now() - interval '1 second' WHERE id = :id")
                .param("id", taskId)
                .update();
    }
}
