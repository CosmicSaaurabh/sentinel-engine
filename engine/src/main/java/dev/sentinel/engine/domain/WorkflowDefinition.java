package dev.sentinel.engine.domain;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * A whole workflow as submitted: a name, an optional idempotency key, an optional future trigger
 * time, and the tasks with their dependencies.
 *
 * @param submissionKey optional client-supplied idempotency key. Resubmitting the same key returns
 *        the existing execution instead of starting a second one, which is what makes a client safe
 *        to retry a submission whose response it never saw
 * @param scheduledAt optional earliest start (FR8). It is applied to the tasks that have no
 *        dependencies, since those are the only ones the trigger time can meaningfully delay
 */
public record WorkflowDefinition(
        String name, String submissionKey, Instant scheduledAt, List<TaskDefinition> tasks) {

    public WorkflowDefinition {
        Objects.requireNonNull(name, "name");
        tasks = tasks == null ? List.of() : List.copyOf(tasks);
    }

    public static WorkflowDefinition of(String name, TaskDefinition... tasks) {
        return new WorkflowDefinition(name, null, null, List.of(tasks));
    }
}
