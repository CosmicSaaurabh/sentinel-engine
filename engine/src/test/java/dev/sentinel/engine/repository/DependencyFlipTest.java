package dev.sentinel.engine.repository;

import static org.assertj.core.api.Assertions.assertThat;

import dev.sentinel.engine.AbstractIntegrationTest;
import dev.sentinel.engine.domain.ClaimedTask;
import dev.sentinel.engine.domain.Task;
import dev.sentinel.engine.domain.TaskStatus;
import dev.sentinel.engine.support.DagFixtures;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionTemplate;

/** Dependency counting: what unblocks, when, and what stays blocked. */
class DependencyFlipTest extends AbstractIntegrationTest {

    private static final Duration LEASE = Duration.ofSeconds(30);

    @Autowired
    private WorkflowRepository workflows;

    @Autowired
    private TaskRepository tasks;

    @Autowired
    private TaskDependencyRepository dependencies;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Test
    @DisplayName("completing a parent unblocks its only child")
    void completingAParentUnblocksItsChild() {
        var dag = DagFixtures.chain(workflows, tasks, dependencies, "first", "second");

        completeAndFlip(dag.workflowId(), claimNext());

        assertThat(statusOf(dag.taskId("second"))).isEqualTo(TaskStatus.PENDING);
    }

    @Test
    @DisplayName("a child with two parents waits for both, not just the first")
    void aJoinWaitsForEveryParent() {
        var dag = DagFixtures.diamond(workflows, tasks, dependencies);

        completeAndFlip(dag.workflowId(), claimNext());
        assertThat(statusOf(dag.taskId("left"))).isEqualTo(TaskStatus.PENDING);
        assertThat(statusOf(dag.taskId("right"))).isEqualTo(TaskStatus.PENDING);
        assertThat(statusOf(dag.taskId("join"))).isEqualTo(TaskStatus.BLOCKED);

        // Both branches are claimable at once, so one poll takes both. Which of the two comes
        // back first is not defined, and the assertion below deliberately does not care.
        List<ClaimedTask> branches = tasks.claimBatch("worker-1", List.of(DagFixtures.TASK_TYPE), 10, LEASE);
        assertThat(branches).hasSize(2);

        completeAndFlip(dag.workflowId(), branches.get(0));
        assertThat(statusOf(dag.taskId("join")))
                .as("one of two parents is not enough")
                .isEqualTo(TaskStatus.BLOCKED);
        assertThat(pendingDependenciesOf(dag.taskId("join"))).isEqualTo(1);

        completeAndFlip(dag.workflowId(), branches.get(1));
        assertThat(statusOf(dag.taskId("join"))).isEqualTo(TaskStatus.PENDING);
        assertThat(pendingDependenciesOf(dag.taskId("join"))).isZero();
    }

    @Test
    @DisplayName("children are locked in ascending id order, which is what makes concurrent parents deadlock-free")
    void childrenAreLockedInIdOrder() {
        var dag = DagFixtures.diamond(workflows, tasks, dependencies);

        List<UUID> locked = transactionTemplate.execute(
                status -> tasks.lockChildrenOfCompletedTask(dag.workflowId(), dag.taskId("root")));

        assertThat(locked).hasSize(2).isSortedAccordingTo(Comparator.naturalOrder());
    }

    @Test
    @DisplayName("a flip only ever moves BLOCKED tasks, never something already running")
    void flipIgnoresTasksThatAreNotBlocked() {
        var dag = DagFixtures.chain(workflows, tasks, dependencies, "first", "second");
        completeAndFlip(dag.workflowId(), claimNext());
        ClaimedTask second = claimNext();
        assertThat(second.name()).isEqualTo("second");

        List<UUID> flipped = tasks.decrementDependenciesAndFlip(List.of(second.id()));

        assertThat(flipped).isEmpty();
        assertThat(statusOf(second.id())).isEqualTo(TaskStatus.RUNNING);
    }

    @Test
    @DisplayName("a whole chain runs to the end, one unblock at a time")
    void aChainRunsToCompletion() {
        var dag = DagFixtures.chain(workflows, tasks, dependencies, "a", "b", "c", "d");

        for (String name : List.of("a", "b", "c", "d")) {
            ClaimedTask claimed = tasks.claimBatch("worker-1", List.of(DagFixtures.TASK_TYPE), 10, LEASE).getFirst();
            assertThat(claimed.name()).isEqualTo(name);
            transactionTemplate.executeWithoutResult(status -> {
                tasks.markCompleted(claimed.id(), "worker-1", claimed.fencingToken(), "{}");
                tasks.decrementDependenciesAndFlip(
                        tasks.lockChildrenOfCompletedTask(dag.workflowId(), claimed.id()));
            });
        }

        assertThat(tasks.countByStatus(dag.workflowId())).containsEntry(TaskStatus.COMPLETED, 4);
        assertThat(workflows.completeIfAllTasksTerminal(dag.workflowId())).isTrue();
    }

    @Test
    @DisplayName("descendants of a task are found transitively, and a diamond is not double-counted")
    void descendantsAreFoundTransitivelyAndDeduplicated() {
        var dag = DagFixtures.diamond(workflows, tasks, dependencies);

        List<UUID> descendants = dependencies.findDescendants(dag.workflowId(), dag.taskId("root"));

        assertThat(descendants).containsExactlyInAnyOrder(
                dag.taskId("left"), dag.taskId("right"), dag.taskId("join"));
    }

    @Test
    @DisplayName("a leaf task has no descendants")
    void aLeafHasNoDescendants() {
        var dag = DagFixtures.diamond(workflows, tasks, dependencies);

        assertThat(dependencies.findDescendants(dag.workflowId(), dag.taskId("join"))).isEmpty();
    }

    /** Claims when exactly one task is expected to be claimable, and fails loudly otherwise. */
    private ClaimedTask claimNext() {
        List<ClaimedTask> claimed = tasks.claimBatch("worker-1", List.of(DagFixtures.TASK_TYPE), 10, LEASE);
        assertThat(claimed).as("expected exactly one claimable task at this point").hasSize(1);
        return claimed.getFirst();
    }

    /**
     * Completes a claimed task and flips its children in one transaction, which is the boundary the
     * completion service will own. Doing the two in separate transactions is what would let the
     * dependency counter drift.
     */
    private void completeAndFlip(UUID workflowId, ClaimedTask claimed) {
        transactionTemplate.executeWithoutResult(status -> {
            assertThat(tasks.markCompleted(claimed.id(), "worker-1", claimed.fencingToken(), "{}")).isTrue();
            tasks.decrementDependenciesAndFlip(tasks.lockChildrenOfCompletedTask(workflowId, claimed.id()));
        });
    }

    private TaskStatus statusOf(UUID taskId) {
        return tasks.findById(taskId).map(Task::status).orElseThrow();
    }

    private int pendingDependenciesOf(UUID taskId) {
        return tasks.findById(taskId).map(Task::pendingDependencies).orElseThrow();
    }
}
