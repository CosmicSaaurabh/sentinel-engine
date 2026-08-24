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
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** What the claim query does and, just as importantly, what it refuses to do. */
class TaskClaimTest extends AbstractIntegrationTest {

    private static final Duration LEASE = Duration.ofSeconds(30);

    @Autowired
    private WorkflowRepository workflows;

    @Autowired
    private TaskRepository tasks;

    @Autowired
    private TaskDependencyRepository dependencies;

    @Test
    @DisplayName("claiming takes ownership, bumps the fencing token, and consumes an attempt")
    void claimTakesOwnershipAtomically() {
        var dag = DagFixtures.singleTask(workflows, tasks, "only");

        List<ClaimedTask> claimed = tasks.claimBatch("worker-1", List.of(DagFixtures.TASK_TYPE), 10, LEASE);

        assertThat(claimed).hasSize(1);
        ClaimedTask task = claimed.getFirst();
        assertThat(task.id()).isEqualTo(dag.taskId("only"));
        assertThat(task.attempt()).isEqualTo(1);
        assertThat(task.fencingToken()).isEqualTo(1L);
        assertThat(task.leaseExpiresAt()).isAfter(Instant.now().minusSeconds(5));

        Task stored = tasks.findById(task.id()).orElseThrow();
        assertThat(stored.status()).isEqualTo(TaskStatus.RUNNING);
        assertThat(stored.lease()).isNotNull();
        assertThat(stored.lease().ownerWorkerId()).isEqualTo("worker-1");
    }

    @Test
    @DisplayName("a task is never handed to two workers")
    void aTaskIsClaimedOnlyOnce() {
        DagFixtures.singleTask(workflows, tasks, "only");

        List<ClaimedTask> first = tasks.claimBatch("worker-1", List.of(DagFixtures.TASK_TYPE), 10, LEASE);
        List<ClaimedTask> second = tasks.claimBatch("worker-2", List.of(DagFixtures.TASK_TYPE), 10, LEASE);

        assertThat(first).hasSize(1);
        assertThat(second).isEmpty();
    }

    @Test
    @DisplayName("blocked tasks are invisible to the claim query")
    void blockedTasksAreNotClaimable() {
        DagFixtures.chain(workflows, tasks, dependencies, "first", "second", "third");

        List<ClaimedTask> claimed = tasks.claimBatch("worker-1", List.of(DagFixtures.TASK_TYPE), 10, LEASE);

        assertThat(claimed).hasSize(1);
        assertThat(claimed.getFirst().name()).isEqualTo("first");
    }

    @Test
    @DisplayName("a task scheduled for the future is not claimed early")
    void futureTasksAreNotClaimedEarly() {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("scheduled")).id();
        tasks.insertAll(workflowId, List.of(new NewTask(
                "later", DagFixtures.TASK_TYPE, TaskStatus.PENDING, "{}", 10, 0,
                Instant.now().plus(Duration.ofHours(1)))));

        assertThat(tasks.claimBatch("worker-1", List.of(DagFixtures.TASK_TYPE), 10, LEASE)).isEmpty();
    }

    @Test
    @DisplayName("a worker only receives task types it declared")
    void taskTypeFilterIsRespected() {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("routing")).id();
        tasks.insertAll(workflowId, List.of(
                NewTask.runnable("a", "type.alpha", "{}"),
                NewTask.runnable("b", "type.beta", "{}")));

        List<ClaimedTask> claimed = tasks.claimBatch("worker-1", List.of("type.beta"), 10, LEASE);

        assertThat(claimed).hasSize(1);
        assertThat(claimed.getFirst().taskType()).isEqualTo("type.beta");
    }

    @Test
    @DisplayName("batch size bounds how much one poll can take")
    void batchSizeIsHonoured() {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("batch")).id();
        tasks.insertAll(workflowId, List.of(
                NewTask.runnable("a", DagFixtures.TASK_TYPE, "{}"),
                NewTask.runnable("b", DagFixtures.TASK_TYPE, "{}"),
                NewTask.runnable("c", DagFixtures.TASK_TYPE, "{}")));

        assertThat(tasks.claimBatch("worker-1", List.of(DagFixtures.TASK_TYPE), 2, LEASE)).hasSize(2);
        assertThat(tasks.claimBatch("worker-2", List.of(DagFixtures.TASK_TYPE), 2, LEASE)).hasSize(1);
    }

    @Test
    @DisplayName("an empty queue returns an empty list rather than an error")
    void emptyQueueIsOrdinary() {
        assertThat(tasks.claimBatch("worker-1", List.of(DagFixtures.TASK_TYPE), 10, LEASE)).isEmpty();
    }

    @Test
    @DisplayName("a worker declaring no task types claims nothing and issues no query")
    void noDeclaredTypesClaimsNothing() {
        DagFixtures.singleTask(workflows, tasks, "only");

        assertThat(tasks.claimBatch("worker-1", List.of(), 10, LEASE)).isEmpty();
    }

    @Test
    @DisplayName("a task with no attempts left is not claimable, so attempt can never pass its bound")
    void exhaustedTasksAreNotClaimable() {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("exhausted")).id();
        var ids = tasks.insertAll(workflowId, List.of(
                new NewTask("spent", DagFixtures.TASK_TYPE, TaskStatus.PENDING, "{}", 1, 0, null)));
        jdbcClient.sql("UPDATE tasks SET attempt = 1 WHERE id = :id")
                .param("id", ids.get("spent"))
                .update();

        assertThat(tasks.claimBatch("worker-1", List.of(DagFixtures.TASK_TYPE), 10, LEASE)).isEmpty();
    }

    @Test
    @DisplayName("claims come back oldest-eligible first")
    void claimOrderFollowsNextAttemptAt() {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("ordering")).id();
        Instant now = Instant.now();
        tasks.insertAll(workflowId, List.of(
                new NewTask("newer", DagFixtures.TASK_TYPE, TaskStatus.PENDING, "{}", 10, 0, now.minusSeconds(10)),
                new NewTask("older", DagFixtures.TASK_TYPE, TaskStatus.PENDING, "{}", 10, 0, now.minusSeconds(600))));

        List<ClaimedTask> claimed = tasks.claimBatch("worker-1", List.of(DagFixtures.TASK_TYPE), 1, LEASE);

        assertThat(claimed).hasSize(1);
        assertThat(claimed.getFirst().name()).isEqualTo("older");
    }
}
