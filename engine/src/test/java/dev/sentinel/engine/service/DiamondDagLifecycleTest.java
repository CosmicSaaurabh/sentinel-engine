package dev.sentinel.engine.service;

import static org.assertj.core.api.Assertions.assertThat;

import dev.sentinel.engine.AbstractIntegrationTest;
import dev.sentinel.engine.domain.ClaimedTask;
import dev.sentinel.engine.domain.FailureKind;
import dev.sentinel.engine.domain.OutboxEvent;
import dev.sentinel.engine.domain.OutboxEventType;
import dev.sentinel.engine.domain.Task;
import dev.sentinel.engine.domain.TaskStatus;
import dev.sentinel.engine.domain.Workflow;
import dev.sentinel.engine.domain.WorkflowStatus;
import dev.sentinel.engine.repository.CapacityCounterRepository;
import dev.sentinel.engine.repository.OutboxRepository;
import dev.sentinel.engine.repository.TaskRepository;
import dev.sentinel.engine.repository.WorkflowRepository;
import dev.sentinel.engine.support.Workflows;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * The whole engine loop over a diamond DAG, which is the shape the task breakdown asks for
 * specifically: {@code root -> left}, {@code root -> right}, and {@code left, right -> join}.
 *
 * <p>It is the interesting shape because it exercises fan-out, fan-in through a counter, and the
 * failure branch where a task downstream of a failure must be guaranteed never to run.
 */
class DiamondDagLifecycleTest extends AbstractIntegrationTest {

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

    @Test
    @DisplayName("happy path: root, then both branches, then the join, then the workflow completes")
    void diamondRunsToCompletion() {
        Workflow workflow = submissionService.submit(Workflows.diamond());

        // Only root is runnable.
        List<ClaimedTask> firstPoll = claim(10);
        assertThat(firstPoll).extracting(ClaimedTask::name).containsExactly("root");
        completeAll(firstPoll);

        // Completing root unblocks both branches at once, and they are claimable together.
        List<ClaimedTask> branches = claim(10);
        assertThat(branches).extracting(ClaimedTask::name).containsExactlyInAnyOrder("left", "right");
        assertThat(statusOf(workflow, "join"))
                .as("the join must wait for both branches, not just for work to appear")
                .isEqualTo(TaskStatus.BLOCKED);

        // Completing one branch is not enough.
        completeAll(List.of(branches.getFirst()));
        assertThat(statusOf(workflow, "join")).isEqualTo(TaskStatus.BLOCKED);
        assertThat(pendingDependenciesOf(workflow, "join")).isEqualTo(1);

        completeAll(List.of(branches.get(1)));
        assertThat(statusOf(workflow, "join")).isEqualTo(TaskStatus.PENDING);

        List<ClaimedTask> join = claim(10);
        assertThat(join).extracting(ClaimedTask::name).containsExactly("join");
        completeAll(join);

        assertThat(workflows.findById(workflow.id()).orElseThrow().status()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(workflows.findById(workflow.id()).orElseThrow().finishedAt()).isNotNull();
        assertThat(capacity.totalInFlight()).as("every slot must be given back").isZero();
        assertThat(outbox.findByWorkflowId(workflow.id())).extracting(OutboxEvent::eventType)
                .contains(OutboxEventType.WORKFLOW_SUBMITTED, OutboxEventType.WORKFLOW_COMPLETED);
    }

    @Test
    @DisplayName("failure branch: left fails permanently, join never runs, and the workflow fails")
    void aFailedBranchStopsEverythingDownstream() {
        Workflow workflow = submissionService.submit(Workflows.diamond());
        completeAll(claim(10));

        List<ClaimedTask> branches = claim(10);
        ClaimedTask left = branches.stream()
                .filter(task -> task.name().equals("left"))
                .findFirst()
                .orElseThrow();

        completionService.fail(left.id(), "worker-1", left.fencingToken(),
                FailureKind.PERMANENT, "left branch is broken");

        assertThat(statusOf(workflow, "left")).isEqualTo(TaskStatus.FAILED);
        assertThat(statusOf(workflow, "join"))
                .as("join depends on left, so it can never become runnable")
                .isEqualTo(TaskStatus.CANCELLED);
        assertThat(workflows.findById(workflow.id()).orElseThrow().status()).isEqualTo(WorkflowStatus.FAILED);

        assertThat(claim(10))
                .as("nothing from a failed workflow may still be handed out")
                .isEmpty();
    }

    @Test
    @DisplayName("a sibling already running when the workflow fails is left to finish, not falsified")
    void aRunningSiblingIsLeftAlone() {
        Workflow workflow = submissionService.submit(Workflows.diamond());
        completeAll(claim(10));
        List<ClaimedTask> branches = claim(10);
        ClaimedTask left = named(branches, "left");
        ClaimedTask right = named(branches, "right");

        completionService.fail(left.id(), "worker-1", left.fencingToken(), FailureKind.PERMANENT, "broken");

        assertThat(statusOf(workflow, "right"))
                .as("its worker may already have caused a side effect; calling it cancelled would be a lie")
                .isEqualTo(TaskStatus.RUNNING);
        assertThat(completionService.complete(right.id(), "worker-1", right.fencingToken(), "{}"))
                .as("the surviving worker can still report, and the report is honoured")
                .isEqualTo(CompletionOutcome.RECORDED);
        assertThat(workflows.findById(workflow.id()).orElseThrow().status())
                .as("a late success does not undo the workflow's failure")
                .isEqualTo(WorkflowStatus.FAILED);
    }

    @Test
    @DisplayName("history distinguishes tasks cancelled by an ancestor from tasks cancelled by the workflow")
    void cancellationReasonsAreRecordedSeparately() {
        // A bystander on a different routing key, so this worker never claims it and it is still
        // waiting when the workflow fails. That is what makes both cancellation reasons observable
        // in one run: join dies because its ancestor failed, the bystander because the workflow did.
        Workflow workflow = submissionService.submit(new dev.sentinel.engine.domain.WorkflowDefinition(
                "diamond-with-bystander", null, null, List.of(
                        dev.sentinel.engine.domain.TaskDefinition.of("root", Workflows.TASK_TYPE),
                        dev.sentinel.engine.domain.TaskDefinition.of("left", Workflows.TASK_TYPE, "root"),
                        dev.sentinel.engine.domain.TaskDefinition.of("right", Workflows.TASK_TYPE, "root"),
                        dev.sentinel.engine.domain.TaskDefinition.of("join", Workflows.TASK_TYPE, "left", "right"),
                        dev.sentinel.engine.domain.TaskDefinition.of("bystander", "other.routing.key"))));
        completeAll(claim(10));
        ClaimedTask left = named(claim(10), "left");

        completionService.fail(left.id(), "worker-1", left.fencingToken(), FailureKind.PERMANENT, "broken");

        assertThat(statusOf(workflow, "bystander")).isEqualTo(TaskStatus.CANCELLED);
        List<String> cancellationPayloads = outbox.findByWorkflowId(workflow.id()).stream()
                .filter(event -> event.eventType() == OutboxEventType.TASK_CANCELLED)
                .map(OutboxEvent::payload)
                .toList();

        assertThat(cancellationPayloads)
                .as("an operator should not have to guess why a task was cancelled")
                .anyMatch(payload -> payload.contains("ancestor_failed"))
                .anyMatch(payload -> payload.contains("workflow_failed"));
    }

    @Test
    @DisplayName("a retryable failure in a branch leaves the workflow running")
    void aRetryableBranchFailureDoesNotFailTheWorkflow() {
        Workflow workflow = submissionService.submit(Workflows.diamond());
        completeAll(claim(10));
        ClaimedTask left = named(claim(10), "left");

        completionService.fail(left.id(), "worker-1", left.fencingToken(), FailureKind.RETRYABLE, "blip");

        assertThat(statusOf(workflow, "left")).isEqualTo(TaskStatus.PENDING);
        assertThat(statusOf(workflow, "join")).isEqualTo(TaskStatus.BLOCKED);
        assertThat(workflows.findById(workflow.id()).orElseThrow().status()).isEqualTo(WorkflowStatus.RUNNING);
    }

    // ------------------------------------------------------------------

    private List<ClaimedTask> claim(int batchSize) {
        return claimService.claim(new ClaimRequest("worker-1", List.of(Workflows.TASK_TYPE), batchSize));
    }

    private void completeAll(List<ClaimedTask> claimed) {
        claimed.forEach(task ->
                completionService.complete(task.id(), "worker-1", task.fencingToken(), "{\"ok\":true}"));
    }

    private static ClaimedTask named(List<ClaimedTask> claimed, String name) {
        return claimed.stream()
                .filter(task -> task.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("expected to have claimed " + name));
    }

    private TaskStatus statusOf(Workflow workflow, String taskName) {
        return taskNamed(workflow.id(), taskName).status();
    }

    private int pendingDependenciesOf(Workflow workflow, String taskName) {
        return taskNamed(workflow.id(), taskName).pendingDependencies();
    }

    private Task taskNamed(UUID workflowId, String taskName) {
        return tasks.findByWorkflowId(workflowId).stream()
                .filter(task -> task.name().equals(taskName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no task named " + taskName));
    }
}
