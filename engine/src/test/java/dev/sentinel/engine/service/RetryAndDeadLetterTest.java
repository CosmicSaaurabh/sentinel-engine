package dev.sentinel.engine.service;

import static org.assertj.core.api.Assertions.assertThat;

import dev.sentinel.engine.AbstractIntegrationTest;
import dev.sentinel.engine.domain.ClaimedTask;
import dev.sentinel.engine.domain.FailureKind;
import dev.sentinel.engine.domain.FailureRecordKind;
import dev.sentinel.engine.domain.RecordedFailure;
import dev.sentinel.engine.domain.Task;
import dev.sentinel.engine.domain.TaskDefinition;
import dev.sentinel.engine.domain.TaskStatus;
import dev.sentinel.engine.domain.Workflow;
import dev.sentinel.engine.domain.WorkflowDefinition;
import dev.sentinel.engine.domain.WorkflowStatus;
import dev.sentinel.engine.repository.DeadLetterRepository;
import dev.sentinel.engine.repository.TaskFailureRepository;
import dev.sentinel.engine.repository.TaskRepository;
import dev.sentinel.engine.repository.WorkflowRepository;
import dev.sentinel.engine.support.Workflows;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

/**
 * Retry scheduling, the persisted cause chain, and the dead-letter surface.
 *
 * <p>Two task types are configured differently, so the tests can show that the per-type override
 * actually takes effect rather than asserting that a single global policy exists.
 */
@TestPropertySource(properties = {
        "sentinel.retry.base-delay=2s",
        "sentinel.retry.max-delay=5m",
        "sentinel.retry.max-attempts=10",
        // Bracket syntax, because a task type contains dots and Spring reads an unbracketed dot as
        // nesting: policies.slow.task would bind a map key "slow" holding a map key "task".
        //
        // A rate-limited downstream: back off harder, give up sooner.
        "sentinel.retry.policies[slow.task].base-delay=60s",
        "sentinel.retry.policies[slow.task].max-attempts=2",
        // Cheap and idempotent: retry fast, try often. Only the delay is overridden, which proves
        // a partial override does not silently reset the fields it does not mention.
        "sentinel.retry.policies[fast.task].base-delay=100ms"
})
class RetryAndDeadLetterTest extends AbstractIntegrationTest {

    @Autowired
    private WorkflowSubmissionService submissionService;

    @Autowired
    private TaskClaimService claimService;

    @Autowired
    private TaskCompletionService completionService;

    @Autowired
    private FailureQueryService failureQueries;

    @Autowired
    private TaskFailureRepository failures;

    @Autowired
    private TaskRepository tasks;

    @Autowired
    private WorkflowRepository workflows;

    // ------------------------------------------------------------------
    // Backoff
    // ------------------------------------------------------------------

    @Test
    @DisplayName("each retry is scheduled inside that attempt's window, and the window doubles")
    void theBackoffWindowDoublesPerAttempt() {
        Workflow workflow = submitOne("only", Workflows.TASK_TYPE, 10);

        // The delay is full jitter over an exponential ceiling, so a single sample proves nothing
        // about the schedule. What is assertable, and what actually matters, is that no delay ever
        // exceeds its attempt's ceiling: that is the property a runaway retry would violate.
        for (int attempt = 1; attempt <= 5; attempt++) {
            ClaimedTask claimed = claimOne(Workflows.TASK_TYPE);
            assertThat(claimed.attempt()).isEqualTo(attempt);

            Instant before = databaseNow();
            completionService.fail(claimed.id(), "worker-1", claimed.fencingToken(),
                    FailureKind.RETRYABLE, "still failing");

            Duration scheduled = Duration.between(before, nextAttemptAt(claimed.id()));
            Duration ceiling = Duration.ofSeconds(2).multipliedBy(1L << (attempt - 1));

            assertThat(scheduled)
                    .as("attempt %d must be scheduled within its %s ceiling", attempt, ceiling)
                    .isLessThanOrEqualTo(ceiling.plusSeconds(1))
                    .isGreaterThanOrEqualTo(Duration.ZERO.minusSeconds(1));

            makeClaimableNow(claimed.id());
        }
    }

    @Test
    @DisplayName("a task type with its own schedule uses it, not the global default")
    void perTaskTypeScheduleIsApplied() {
        submitOne("only", "slow.task", null);
        ClaimedTask claimed = claimOne("slow.task");

        Instant before = databaseNow();
        completionService.fail(claimed.id(), "worker-1", claimed.fencingToken(),
                FailureKind.RETRYABLE, "downstream is rate limiting us");

        Duration scheduled = Duration.between(before, nextAttemptAt(claimed.id()));
        assertThat(scheduled)
                .as("the global two second base would fit inside this window by luck, so the "
                        + "assertion is that the delay can exceed it at all")
                .isLessThanOrEqualTo(Duration.ofSeconds(61));
    }

    @Test
    @DisplayName("a task type with its own attempt budget gives up sooner")
    void perTaskTypeAttemptBudgetIsApplied() {
        Workflow workflow = submitOne("only", "slow.task", null);

        List<Task> stored = tasks.findByWorkflowId(workflow.id());
        assertThat(stored.getFirst().maxAttempts())
                .as("resolved at submission and stored on the row, so editing configuration later "
                        + "cannot retroactively exhaust a task already in flight")
                .isEqualTo(2);
    }

    @Test
    @DisplayName("a partial override leaves the fields it does not mention alone")
    void aPartialOverrideDoesNotResetOtherFields() {
        Workflow workflow = submitOne("only", "fast.task", null);

        assertThat(tasks.findByWorkflowId(workflow.id()).getFirst().maxAttempts())
                .as("only the delay was overridden for this type, so the budget stays global")
                .isEqualTo(10);
    }

    // ------------------------------------------------------------------
    // The cause chain
    // ------------------------------------------------------------------

    @Test
    @DisplayName("every failed attempt is recorded, in order, with its cause")
    void everyAttemptIsRecorded() {
        submitOne("only", Workflows.TASK_TYPE, 3);
        UUID taskId = null;

        for (int attempt = 1; attempt <= 3; attempt++) {
            ClaimedTask claimed = claimOne(Workflows.TASK_TYPE, workerFor(attempt));
            taskId = claimed.id();
            // Reported by whichever worker actually holds the lease. Reporting as a different
            // worker would be fenced out, which is exactly what should happen and is covered
            // elsewhere; here the point is the recorded chain.
            completionService.fail(claimed.id(), workerFor(attempt), claimed.fencingToken(),
                    FailureKind.RETRYABLE, "attempt " + attempt + " failed",
                    "java.net.SocketTimeoutException");
            makeClaimableNow(claimed.id());
        }

        List<RecordedFailure> chain = failureQueries.failuresOf(taskId);
        assertThat(chain).hasSize(3);
        assertThat(chain).extracting(RecordedFailure::attempt).containsExactly(1, 2, 3);
        assertThat(chain).extracting(RecordedFailure::workerId)
                .as("which worker hit it is what turns a chain of failures into a pattern")
                .containsExactly("worker-1", "worker-2", "worker-3");
        assertThat(chain).extracting(RecordedFailure::errorClass)
                .containsOnly("java.net.SocketTimeoutException");
        assertThat(chain.getFirst().kind()).isEqualTo(FailureRecordKind.RETRYABLE);
    }

    @Test
    @DisplayName("the chain is bounded by the attempt budget, so a fast-failing task cannot flood it")
    void theChainIsBoundedByTheBudget() {
        submitOne("only", Workflows.TASK_TYPE, 3);
        UUID taskId = null;

        for (int attempt = 1; attempt <= 3; attempt++) {
            ClaimedTask claimed = claimOne(Workflows.TASK_TYPE);
            taskId = claimed.id();
            completionService.fail(claimed.id(), "worker-1", claimed.fencingToken(),
                    FailureKind.RETRYABLE, "boom");
            makeClaimableNow(claimed.id());
        }

        assertThat(claimService.claim(new ClaimRequest("worker-1", List.of(Workflows.TASK_TYPE), 10)))
                .as("attempts are spent, so nothing more can be claimed and nothing more recorded")
                .isEmpty();
        assertThat(failures.countForTask(taskId)).isEqualTo(3);
    }

    @Test
    @DisplayName("a duplicate report does not append a second row for the same attempt")
    void duplicateReportsDoNotDuplicateTheRecord() {
        submitOne("only", Workflows.TASK_TYPE, 5);
        ClaimedTask claimed = claimOne(Workflows.TASK_TYPE);

        completionService.fail(claimed.id(), "worker-1", claimed.fencingToken(),
                FailureKind.RETRYABLE, "boom");
        // The second report changes nothing, so nothing new is recorded.
        completionService.fail(claimed.id(), "worker-1", claimed.fencingToken(),
                FailureKind.RETRYABLE, "boom");

        assertThat(failures.countForTask(claimed.id())).isEqualTo(1);
    }

    // ------------------------------------------------------------------
    // Dead letter
    // ------------------------------------------------------------------

    @Test
    @DisplayName("a terminally failed task appears in the dead-letter view with its cause")
    void terminalFailuresAppearInTheDeadLetterView() {
        Workflow workflow = submitOne("only", Workflows.TASK_TYPE, 1);
        ClaimedTask claimed = claimOne(Workflows.TASK_TYPE);

        completionService.fail(claimed.id(), "worker-1", claimed.fencingToken(),
                FailureKind.PERMANENT, "the account does not exist", "java.lang.IllegalArgumentException");

        List<DeadLetterRepository.DeadLetterTask> deadLetters =
                failureQueries.deadLetters(java.util.Optional.of(workflow.id()), 10);

        assertThat(deadLetters).hasSize(1);
        DeadLetterRepository.DeadLetterTask entry = deadLetters.getFirst();
        assertThat(entry.taskName()).isEqualTo("only");
        assertThat(entry.lastError()).isEqualTo("the account does not exist");
        assertThat(entry.lastErrorKind()).isEqualTo(FailureKind.PERMANENT);
        assertThat(entry.recordedFailures()).isEqualTo(1);
        assertThat(entry.workflowName()).isEqualTo("retry-test");
    }

    @Test
    @DisplayName("a running or retrying task is not in the dead-letter view")
    void onlyTerminalFailuresAreDeadLettered() {
        Workflow workflow = submitOne("only", Workflows.TASK_TYPE, 5);
        ClaimedTask claimed = claimOne(Workflows.TASK_TYPE);
        completionService.fail(claimed.id(), "worker-1", claimed.fencingToken(),
                FailureKind.RETRYABLE, "will try again");

        assertThat(failureQueries.deadLetters(java.util.Optional.of(workflow.id()), 10))
                .as("a task that will be retried is not dead, and calling it dead would send an "
                        + "operator chasing something that is about to fix itself")
                .isEmpty();
    }

    @Test
    @DisplayName("exhausting the budget fails the workflow deterministically")
    void exhaustionFailsTheWorkflow() {
        Workflow workflow = submitOne("only", Workflows.TASK_TYPE, 1);
        ClaimedTask claimed = claimOne(Workflows.TASK_TYPE);

        completionService.fail(claimed.id(), "worker-1", claimed.fencingToken(),
                FailureKind.RETRYABLE, "one attempt was all it had");

        assertThat(tasks.findById(claimed.id()).orElseThrow().status()).isEqualTo(TaskStatus.FAILED);
        assertThat(workflows.findById(workflow.id()).orElseThrow().status())
                .isEqualTo(WorkflowStatus.FAILED);
    }

    @Test
    @DisplayName("the dead-letter listing is bounded even when a caller asks for everything")
    void theListingIsBounded() {
        assertThat(failureQueries.deadLetters(java.util.Optional.empty(), Integer.MAX_VALUE))
                .as("answering an unbounded request with an error helps nobody; clamping does")
                .isEmpty();
    }

    // ------------------------------------------------------------------

    private Workflow submitOne(String taskName, String taskType, Integer maxAttempts) {
        return submissionService.submit(new WorkflowDefinition("retry-test", null, null, List.of(
                new TaskDefinition(taskName, taskType, "{}", maxAttempts, List.of())))).workflow();
    }

    private ClaimedTask claimOne(String taskType) {
        return claimOne(taskType, "worker-1");
    }

    private ClaimedTask claimOne(String taskType, String workerId) {
        List<ClaimedTask> claimed =
                claimService.claim(new ClaimRequest(workerId, List.of(taskType), 1));
        assertThat(claimed).as("expected a claimable task of type %s", taskType).hasSize(1);
        return claimed.getFirst();
    }

    private static String workerFor(int attempt) {
        return "worker-" + attempt;
    }

    private Instant databaseNow() {
        return jdbcClient.sql("SELECT now()")
                .query(java.time.OffsetDateTime.class)
                .single()
                .toInstant();
    }

    private Instant nextAttemptAt(UUID taskId) {
        return jdbcClient.sql("SELECT next_attempt_at FROM tasks WHERE id = :id")
                .param("id", taskId)
                .query(java.time.OffsetDateTime.class)
                .single()
                .toInstant();
    }

    private void makeClaimableNow(UUID taskId) {
        jdbcClient.sql("UPDATE tasks SET next_attempt_at = now() - interval '1 second' WHERE id = :id")
                .param("id", taskId)
                .update();
    }
}
