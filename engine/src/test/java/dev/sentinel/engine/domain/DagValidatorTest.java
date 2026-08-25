package dev.sentinel.engine.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import dev.sentinel.engine.error.InvalidWorkflowException;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Structural validation, tested as plain objects.
 *
 * <p>No Spring and no database here on purpose. This logic is pure, so its tests should run in
 * microseconds and be the first thing that fails when someone breaks cycle detection, rather than
 * being buried inside a slower integration test.
 */
class DagValidatorTest {

    private static final int MAX_TASKS = 100;
    private static final int MAX_EDGES = 500;
    private static final int DEFAULT_MAX_ATTEMPTS = 10;

    // The attempt budget is resolved per task type, so the validator takes a lookup rather than a
    // number. Every type gets the same budget here; RetryAndDeadLetterTest covers the per-type case.
    private final DagValidator validator =
            new DagValidator(MAX_TASKS, MAX_EDGES, taskType -> DEFAULT_MAX_ATTEMPTS);

    // ------------------------------------------------------------------
    // Accepted shapes
    // ------------------------------------------------------------------

    @Test
    @DisplayName("a single task with no dependencies is a valid workflow")
    void singleTaskIsValid() {
        ValidatedDag dag = validator.validate(WorkflowDefinition.of("one-step", TaskDefinition.of("a", "t.a")));

        assertThat(dag.tasks()).hasSize(1);
        assertThat(dag.tasks().getFirst().status()).isEqualTo(TaskStatus.PENDING);
        assertThat(dag.tasks().getFirst().pendingDependencies()).isZero();
        assertThat(dag.edges()).isEmpty();
    }

    @Test
    @DisplayName("disconnected branches are legal; a workflow is a forest, not one tree")
    void disconnectedComponentsAreValid() {
        ValidatedDag dag = validator.validate(WorkflowDefinition.of("forest",
                TaskDefinition.of("a", "t.a"),
                TaskDefinition.of("b", "t.b"),
                TaskDefinition.of("a2", "t.a", "a"),
                TaskDefinition.of("b2", "t.b", "b")));

        assertThat(dag.rootTasks()).extracting(NewTask::name).containsExactlyInAnyOrder("a", "b");
    }

    @Test
    @DisplayName("a diamond initialises the join with two blockers, counted once per distinct parent")
    void diamondCountsBlockersCorrectly() {
        ValidatedDag dag = validator.validate(diamond());

        assertThat(byName(dag, "root").pendingDependencies()).isZero();
        assertThat(byName(dag, "root").status()).isEqualTo(TaskStatus.PENDING);
        assertThat(byName(dag, "left").pendingDependencies()).isEqualTo(1);
        assertThat(byName(dag, "join").pendingDependencies()).isEqualTo(2);
        assertThat(byName(dag, "join").status()).isEqualTo(TaskStatus.BLOCKED);
        assertThat(dag.edges()).hasSize(4);
    }

    @Test
    @DisplayName("a dependency declared twice counts once, rather than blocking the child forever")
    void duplicateDependenciesAreDeduplicated() {
        ValidatedDag dag = validator.validate(WorkflowDefinition.of("sloppy",
                TaskDefinition.of("a", "t.a"),
                new TaskDefinition("b", "t.b", "{}", null, List.of("a", "a", "a"))));

        assertThat(byName(dag, "b").pendingDependencies())
                .as("counting a repeated declaration three times would leave b permanently blocked")
                .isEqualTo(1);
        assertThat(dag.edges()).hasSize(1);
    }

    @Test
    @DisplayName("a trigger time delays the root tasks, which are the only ones it can delay")
    void triggerTimeAppliesToRootTasksOnly() {
        Instant triggerAt = Instant.parse("2030-01-01T00:00:00Z");

        ValidatedDag dag = validator.validate(new WorkflowDefinition("scheduled", null, triggerAt, List.of(
                TaskDefinition.of("root", "t.a"),
                TaskDefinition.of("child", "t.a", "root"))));

        assertThat(byName(dag, "root").notBefore()).isEqualTo(triggerAt);
        assertThat(byName(dag, "child").notBefore())
                .as("a blocked task is already waiting on its parent, which is itself delayed")
                .isNull();
    }

    @Test
    @DisplayName("a task without an explicit attempt budget gets the engine default")
    void defaultMaxAttemptsIsApplied() {
        ValidatedDag dag = validator.validate(WorkflowDefinition.of("defaults",
                TaskDefinition.of("a", "t.a"),
                new TaskDefinition("b", "t.b", "{}", 3, List.of())));

        assertThat(byName(dag, "a").maxAttempts()).isEqualTo(DEFAULT_MAX_ATTEMPTS);
        assertThat(byName(dag, "b").maxAttempts()).isEqualTo(3);
    }

    // ------------------------------------------------------------------
    // Rejected shapes
    // ------------------------------------------------------------------

    @Test
    @DisplayName("an empty workflow is rejected, because it could never reach a terminal state")
    void emptyWorkflowIsRejected() {
        assertThatThrownBy(() -> validator.validate(WorkflowDefinition.of("nothing")))
                .isInstanceOf(InvalidWorkflowException.class)
                .hasMessageContaining("at least one task");
    }

    @Test
    @DisplayName("a two-task cycle is detected and both tasks are named")
    void simpleCycleIsRejected() {
        InvalidWorkflowException thrown = validateExpectingFailure(WorkflowDefinition.of("loop",
                TaskDefinition.of("a", "t.a", "b"),
                TaskDefinition.of("b", "t.b", "a")));

        assertThat(thrown.violations()).hasSize(1);
        assertThat(thrown.violations().getFirst()).contains("cycle").contains("a, b");
    }

    @Test
    @DisplayName("a longer cycle is detected, and unrelated tasks are not blamed for it")
    void longerCycleNamesOnlyTheTasksInvolved() {
        InvalidWorkflowException thrown = validateExpectingFailure(WorkflowDefinition.of("long-loop",
                TaskDefinition.of("independent", "t.a"),
                TaskDefinition.of("a", "t.a", "c"),
                TaskDefinition.of("b", "t.b", "a"),
                TaskDefinition.of("c", "t.c", "b")));

        String violation = thrown.violations().getFirst();
        assertThat(violation).contains("a, b, c");
        assertThat(violation)
                .as("naming a task that is not in the cycle sends the author looking in the wrong place")
                .doesNotContain("independent");
    }

    @Test
    @DisplayName("a task depending on itself is rejected as such, not reported as a cycle")
    void selfDependencyIsRejected() {
        InvalidWorkflowException thrown = validateExpectingFailure(
                WorkflowDefinition.of("selfish", TaskDefinition.of("a", "t.a", "a")));

        assertThat(thrown.violations().getFirst()).contains("depends on itself");
    }

    @Test
    @DisplayName("a dependency on a task outside the submission is rejected")
    void danglingDependencyIsRejected() {
        InvalidWorkflowException thrown = validateExpectingFailure(WorkflowDefinition.of("dangling",
                TaskDefinition.of("a", "t.a", "somewhere-else")));

        assertThat(thrown.violations().getFirst())
                .contains("not part of this workflow")
                .as("this is the rule the whole sharding strategy rests on, so the message says so")
                .contains("workflow boundary");
    }

    @Test
    @DisplayName("duplicate task names are rejected, since names identify tasks within a workflow")
    void duplicateNamesAreRejected() {
        InvalidWorkflowException thrown = validateExpectingFailure(WorkflowDefinition.of("twins",
                TaskDefinition.of("a", "t.a"),
                TaskDefinition.of("a", "t.b")));

        assertThat(thrown.violations().getFirst()).contains("used more than once");
    }

    @Test
    @DisplayName("every violation is reported at once, not one per round trip")
    void allViolationsAreReportedTogether() {
        InvalidWorkflowException thrown = validateExpectingFailure(WorkflowDefinition.of("mess",
                TaskDefinition.of("a", "not a valid type"),
                TaskDefinition.of("a", "t.b"),
                TaskDefinition.of("c", "t.c", "missing")));

        assertThat(thrown.violations())
                .as("fixing a generated workflow one error at a time is a miserable experience")
                .hasSizeGreaterThan(2);
    }

    @Test
    @DisplayName("a workflow larger than the configured bound is rejected")
    void oversizedWorkflowIsRejected() {
        List<TaskDefinition> tooMany = java.util.stream.IntStream.rangeClosed(0, MAX_TASKS)
                .mapToObj(i -> TaskDefinition.of("task-" + i, "t.a"))
                .toList();

        assertThatThrownBy(() -> validator.validate(new WorkflowDefinition("huge", null, null, tooMany)))
                .isInstanceOf(InvalidWorkflowException.class)
                .hasMessageContaining("at most %d tasks".formatted(MAX_TASKS));
    }

    @Test
    @DisplayName("an invalid task type is rejected before it can reach the database constraint")
    void invalidTaskTypeIsRejected() {
        InvalidWorkflowException thrown = validateExpectingFailure(
                WorkflowDefinition.of("routing", TaskDefinition.of("a", "has,comma")));

        assertThat(thrown.violations().getFirst()).contains("invalid task type");
    }

    @Test
    @DisplayName("a dangling dependency is not misreported as a cycle")
    void danglingDependencyDoesNotMasqueradeAsACycle() {
        InvalidWorkflowException thrown = validateExpectingFailure(WorkflowDefinition.of("confusing",
                TaskDefinition.of("a", "t.a", "ghost"),
                TaskDefinition.of("b", "t.b", "a")));

        assertThat(thrown.violations())
                .as("running the topological sort over a broken graph would bury the real error")
                .noneMatch(violation -> violation.contains("cycle"));
    }

    // ------------------------------------------------------------------

    private static WorkflowDefinition diamond() {
        return WorkflowDefinition.of("diamond",
                TaskDefinition.of("root", "t.a"),
                TaskDefinition.of("left", "t.a", "root"),
                TaskDefinition.of("right", "t.a", "root"),
                TaskDefinition.of("join", "t.a", "left", "right"));
    }

    private InvalidWorkflowException validateExpectingFailure(WorkflowDefinition definition) {
        return catchThrowableOfType(InvalidWorkflowException.class, () -> validator.validate(definition));
    }

    private static NewTask byName(ValidatedDag dag, String name) {
        return dag.tasks().stream()
                .filter(task -> task.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no task named " + name));
    }
}
