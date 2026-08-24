package dev.sentinel.engine.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.sentinel.engine.AbstractIntegrationTest;
import dev.sentinel.engine.domain.OutboxEvent;
import dev.sentinel.engine.domain.OutboxEventType;
import dev.sentinel.engine.domain.Task;
import dev.sentinel.engine.domain.TaskDefinition;
import dev.sentinel.engine.domain.TaskStatus;
import dev.sentinel.engine.domain.Workflow;
import dev.sentinel.engine.domain.WorkflowDefinition;
import dev.sentinel.engine.domain.WorkflowStatus;
import dev.sentinel.engine.error.InvalidWorkflowException;
import dev.sentinel.engine.repository.OutboxRepository;
import dev.sentinel.engine.repository.TaskDependencyRepository;
import dev.sentinel.engine.repository.TaskRepository;
import dev.sentinel.engine.support.Workflows;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** Submission: what gets written, what gets refused, and what happens on a retry. */
class WorkflowSubmissionServiceTest extends AbstractIntegrationTest {

    @Autowired
    private WorkflowSubmissionService submissionService;

    @Autowired
    private TaskClaimService claimService;

    @Autowired
    private TaskRepository tasks;

    @Autowired
    private TaskDependencyRepository dependencies;

    @Autowired
    private OutboxRepository outbox;

    @Test
    @DisplayName("a submitted diamond is written whole: workflow, tasks, edges and history")
    void submissionWritesTheWholeGraph() {
        Workflow workflow = submissionService.submit(Workflows.diamond());

        assertThat(workflow.status()).isEqualTo(WorkflowStatus.RUNNING);
        assertThat(tasks.findByWorkflowId(workflow.id())).hasSize(4);
        assertThat(dependencies.findByWorkflowId(workflow.id())).hasSize(4);
    }

    @Test
    @DisplayName("only dependency-free tasks start claimable; the rest start blocked")
    void onlyRootTasksStartRunnable() {
        Workflow workflow = submissionService.submit(Workflows.diamond());

        assertThat(tasks.countByStatus(workflow.id()))
                .containsEntry(TaskStatus.PENDING, 1)
                .containsEntry(TaskStatus.BLOCKED, 3);
    }

    @Test
    @DisplayName("submission records a workflow event and one scheduled event per runnable task")
    void submissionRecordsHistory() {
        Workflow workflow = submissionService.submit(Workflows.diamond());

        List<OutboxEvent> events = outbox.findByWorkflowId(workflow.id());
        assertThat(events).extracting(OutboxEvent::eventType)
                .containsExactly(OutboxEventType.WORKFLOW_SUBMITTED, OutboxEventType.TASK_SCHEDULED);
        // Read the payload through jsonb operators rather than as text. jsonb is a parsed
        // structure, not the string that was written: Postgres reorders keys and normalises
        // whitespace, so asserting on the raw text would be asserting on storage formatting.
        assertThat(jsonField(events.getFirst().id(), "taskCount")).isEqualTo("4");
        assertThat(jsonField(events.getFirst().id(), "edgeCount")).isEqualTo("4");
    }

    @Test
    @DisplayName("a malformed DAG is refused without writing anything at all")
    void invalidWorkflowWritesNothing() {
        WorkflowDefinition cyclic = WorkflowDefinition.of("loop",
                TaskDefinition.of("a", Workflows.TASK_TYPE, "b"),
                TaskDefinition.of("b", Workflows.TASK_TYPE, "a"));

        assertThatThrownBy(() -> submissionService.submit(cyclic))
                .isInstanceOf(InvalidWorkflowException.class);

        assertThat(jdbcClient.sql("SELECT count(*) FROM workflows").query(Integer.class).single())
                .as("validation runs before the transaction opens, so there is nothing to roll back")
                .isZero();
    }

    @Test
    @DisplayName("resubmitting the same idempotency key returns the original execution")
    void submissionKeyMakesRetriesSafe() {
        WorkflowDefinition definition = new WorkflowDefinition(
                "billing", "invoice-2026-08", null, Workflows.diamond().tasks());

        Workflow first = submissionService.submit(definition);
        Workflow second = submissionService.submit(definition);

        assertThat(second.id()).isEqualTo(first.id());
        assertThat(jdbcClient.sql("SELECT count(*) FROM tasks").query(Integer.class).single())
                .as("a client retrying an ambiguous submission must not double-execute the work")
                .isEqualTo(4);
    }

    @Test
    @DisplayName("a workflow scheduled for the future is written now but not claimable yet")
    void scheduledWorkflowIsNotClaimableEarly() {
        Instant triggerAt = Instant.now().plus(Duration.ofHours(1));
        Workflow workflow = submissionService.submit(new WorkflowDefinition(
                "later", null, triggerAt, Workflows.singleTask().tasks()));

        assertThat(workflow.status()).isEqualTo(WorkflowStatus.RUNNING);
        assertThat(tasks.countByStatus(workflow.id())).containsEntry(TaskStatus.PENDING, 1);
        assertThat(claimService.claim(new ClaimRequest("worker-1", List.of(Workflows.TASK_TYPE), 10)))
                .as("the delay lives on the task's next_attempt_at, which the claim query already honours")
                .isEmpty();
    }

    @Test
    @DisplayName("a chain submits with each task blocked on the one before it")
    void chainIsWrittenInOrder() {
        Workflow workflow = submissionService.submit(Workflows.chain("a", "b", "c"));

        List<Task> stored = tasks.findByWorkflowId(workflow.id());
        assertThat(stored).filteredOn(task -> task.status() == TaskStatus.PENDING)
                .extracting(Task::name)
                .containsExactly("a");
        assertThat(stored).filteredOn(task -> task.name().equals("c"))
                .allMatch(task -> task.pendingDependencies() == 1);
    }

    private String jsonField(java.util.UUID eventId, String field) {
        return jdbcClient.sql("SELECT payload ->> :field FROM outbox WHERE id = :id")
                .param("field", field)
                .param("id", eventId)
                .query(String.class)
                .single();
    }
}
