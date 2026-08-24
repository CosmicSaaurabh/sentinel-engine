package dev.sentinel.engine.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.sentinel.engine.AbstractIntegrationTest;
import dev.sentinel.engine.domain.ClaimedTask;
import dev.sentinel.engine.domain.NewWorkflow;
import dev.sentinel.engine.domain.Workflow;
import dev.sentinel.engine.domain.WorkflowStatus;
import dev.sentinel.engine.support.DagFixtures;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** Workflow-level bookkeeping: submission identity and terminal settlement. */
class WorkflowRepositoryTest extends AbstractIntegrationTest {

    private static final Duration LEASE = Duration.ofSeconds(30);

    @Autowired
    private WorkflowRepository workflows;

    @Autowired
    private TaskRepository tasks;

    @Autowired
    private TaskDependencyRepository dependencies;

    @Test
    @DisplayName("a new workflow starts RUNNING with database-generated timestamps")
    void insertedWorkflowStartsRunning() {
        Workflow workflow = workflows.insert(NewWorkflow.immediate("nightly-report"));

        assertThat(workflow.id()).isNotNull();
        assertThat(workflow.status()).isEqualTo(WorkflowStatus.RUNNING);
        assertThat(workflow.createdAt()).isNotNull();
        assertThat(workflow.finishedAt()).isNull();
        assertThat(workflows.findById(workflow.id())).contains(workflow);
    }

    @Test
    @DisplayName("a future trigger time is stored rather than turned into an immediate start")
    void scheduledWorkflowKeepsItsTriggerTime() {
        Instant triggerAt = Instant.now().plus(Duration.ofDays(1));

        Workflow workflow = workflows.insert(new NewWorkflow("delayed", null, triggerAt));

        assertThat(workflow.scheduledAt()).isCloseTo(triggerAt, org.assertj.core.api.Assertions.within(
                java.time.temporal.ChronoUnit.SECONDS.getDuration()));
    }

    @Test
    @DisplayName("the same submission key cannot start two executions")
    void submissionKeyIsUnique() {
        workflows.insert(new NewWorkflow("billing-run", "invoice-2026-08", null));

        assertThatThrownBy(() -> workflows.insert(new NewWorkflow("billing-run", "invoice-2026-08", null)))
                .hasMessageContaining("workflows_submission_key_uq");
    }

    @Test
    @DisplayName("a client retrying an ambiguous submission finds the original execution")
    void submissionKeyLooksUpTheOriginal() {
        Workflow original = workflows.insert(new NewWorkflow("billing-run", "invoice-2026-08", null));

        assertThat(workflows.findBySubmissionKey("invoice-2026-08")).contains(original);
    }

    @Test
    @DisplayName("workflows without a submission key do not collide with each other")
    void nullSubmissionKeysDoNotCollide() {
        workflows.insert(NewWorkflow.immediate("one"));
        workflows.insert(NewWorkflow.immediate("two"));

        assertThat(jdbcClient.sql("SELECT count(*) FROM workflows").query(Integer.class).single()).isEqualTo(2);
    }

    @Test
    @DisplayName("a workflow is not completed while any task is still in flight")
    void workflowIsNotCompletedEarly() {
        var dag = DagFixtures.chain(workflows, tasks, dependencies, "first", "second");

        assertThat(workflows.completeIfAllTasksTerminal(dag.workflowId())).isFalse();
        assertThat(workflows.findById(dag.workflowId()).orElseThrow().status()).isEqualTo(WorkflowStatus.RUNNING);
    }

    @Test
    @DisplayName("a workflow with a failed task is not settled as completed")
    void aFailedTaskBlocksCompletion() {
        var dag = DagFixtures.singleTask(workflows, tasks, "only");
        ClaimedTask claimed = tasks.claimBatch("worker-1", List.of(DagFixtures.TASK_TYPE), 10, LEASE).getFirst();
        tasks.markFailed(claimed.id(), "worker-1", claimed.fencingToken(),
                new dev.sentinel.engine.domain.TaskFailure("nope", dev.sentinel.engine.domain.FailureKind.PERMANENT));

        assertThat(workflows.completeIfAllTasksTerminal(dag.workflowId()))
                .as("every task is terminal, but one of them failed")
                .isFalse();
        assertThat(workflows.markFailed(dag.workflowId())).isTrue();
        assertThat(workflows.findById(dag.workflowId()).orElseThrow().finishedAt()).isNotNull();
    }

    @Test
    @DisplayName("settling a workflow twice is a benign race, not an error")
    void settlingTwiceIsBenign() {
        var dag = DagFixtures.singleTask(workflows, tasks, "only");
        ClaimedTask claimed = tasks.claimBatch("worker-1", List.of(DagFixtures.TASK_TYPE), 10, LEASE).getFirst();
        tasks.markCompleted(claimed.id(), "worker-1", claimed.fencingToken(), "{}");

        assertThat(workflows.completeIfAllTasksTerminal(dag.workflowId())).isTrue();
        assertThat(workflows.completeIfAllTasksTerminal(dag.workflowId()))
                .as("the second finisher loses the race and changes nothing")
                .isFalse();
    }

    @Test
    @DisplayName("started_at records the first start only")
    void startedAtIsRecordedOnce() {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("start-once")).id();

        assertThat(workflows.markStartedIfUnset(workflowId)).isTrue();
        Instant firstStart = workflows.findById(workflowId).orElseThrow().startedAt();
        assertThat(workflows.markStartedIfUnset(workflowId)).isFalse();
        assertThat(workflows.findById(workflowId).orElseThrow().startedAt()).isEqualTo(firstStart);
    }

    @Test
    @DisplayName("deleting a workflow removes its tasks and edges")
    void deleteCascades() {
        var dag = DagFixtures.chain(workflows, tasks, dependencies, "first", "second");

        jdbcClient.sql("DELETE FROM workflows WHERE id = :id").param("id", dag.workflowId()).update();

        assertThat(tasks.findByWorkflowId(dag.workflowId())).isEmpty();
        assertThat(dependencies.findByWorkflowId(dag.workflowId())).isEmpty();
    }
}
