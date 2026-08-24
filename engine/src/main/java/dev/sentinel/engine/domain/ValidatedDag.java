package dev.sentinel.engine.domain;

import java.util.List;
import java.util.Objects;

/**
 * A submission that has passed every structural check, ready to be written.
 *
 * <p>This type exists to make validation unskippable. Nothing downstream accepts a
 * {@link WorkflowDefinition}; the only way to reach the repositories is through a
 * {@link DagValidator}, so "we forgot to validate this path" is a compile error rather than a
 * runtime surprise.
 *
 * <p>{@code tasks} already carries the correct initial status and dependency count for each task,
 * computed during the same pass that proved the graph acyclic.
 */
public record ValidatedDag(NewWorkflow workflow, List<NewTask> tasks, List<NameEdge> edges) {

    public ValidatedDag {
        Objects.requireNonNull(workflow, "workflow");
        tasks = List.copyOf(tasks);
        edges = List.copyOf(edges);
    }

    /** Tasks that are runnable the moment the workflow is written, subject to any trigger time. */
    public List<NewTask> rootTasks() {
        return tasks.stream().filter(task -> task.status() == TaskStatus.PENDING).toList();
    }
}
