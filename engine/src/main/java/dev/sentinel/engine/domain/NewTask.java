package dev.sentinel.engine.domain;

import java.time.Instant;
import java.util.Objects;

/**
 * A task about to be inserted as part of a workflow submission.
 *
 * @param status either {@link TaskStatus#PENDING} for a task with no parents or
 *        {@link TaskStatus#BLOCKED} for one with parents. No other status is insertable
 * @param pendingDependencies number of parents, computed once during DAG validation rather than
 *        re-derived per completion
 * @param notBefore optional earliest claim time. Null means the database's {@code now()}
 */
public record NewTask(
        String name,
        String taskType,
        TaskStatus status,
        String input,
        int maxAttempts,
        int pendingDependencies,
        Instant notBefore) {

    public static final int DEFAULT_MAX_ATTEMPTS = 10;

    public NewTask {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(status, "status");
        TaskType.requireValid(taskType);
        if (name.isBlank()) {
            throw new IllegalArgumentException("task name must not be blank");
        }
        if (status != TaskStatus.PENDING && status != TaskStatus.BLOCKED) {
            throw new IllegalArgumentException(
                    "a task may only be inserted as PENDING or BLOCKED, but was " + status);
        }
        if ((status == TaskStatus.BLOCKED) != (pendingDependencies > 0)) {
            throw new IllegalArgumentException(
                    "a task is BLOCKED exactly when it has unmet dependencies, but status was "
                            + status + " with " + pendingDependencies + " pending dependencies");
        }
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1, was " + maxAttempts);
        }
        input = input == null ? "{}" : input;
    }

    public static NewTask runnable(String name, String taskType, String input) {
        return new NewTask(name, taskType, TaskStatus.PENDING, input, DEFAULT_MAX_ATTEMPTS, 0, null);
    }

    public static NewTask blocked(String name, String taskType, String input, int parentCount) {
        return new NewTask(name, taskType, TaskStatus.BLOCKED, input, DEFAULT_MAX_ATTEMPTS, parentCount, null);
    }
}
