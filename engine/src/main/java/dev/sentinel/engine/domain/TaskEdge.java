package dev.sentinel.engine.domain;

import java.util.Objects;
import java.util.UUID;

/**
 * One DAG edge: {@code taskId} cannot start until {@code dependsOnTaskId} has completed.
 *
 * <p>Both ends always belong to the same workflow. That rule is what makes {@code workflow_id} a
 * valid shard key, so it is validated at submission and enforced by the primary key here.
 */
public record TaskEdge(UUID taskId, UUID dependsOnTaskId) {

    public TaskEdge {
        Objects.requireNonNull(taskId, "taskId");
        Objects.requireNonNull(dependsOnTaskId, "dependsOnTaskId");
        if (taskId.equals(dependsOnTaskId)) {
            throw new IllegalArgumentException("a task cannot depend on itself: " + taskId);
        }
    }
}
