package dev.sentinel.engine.domain;

import java.util.regex.Pattern;

/**
 * Validation for a task's routing key.
 *
 * <p>The character set is constrained because task types are joined into a delimited string when
 * they are passed to Postgres array functions, and because they are a public routing identifier
 * that ends up in metric labels. The same rule is enforced by a {@code CHECK} constraint, so the
 * database is the backstop rather than the only line of defence.
 */
public final class TaskType {

    public static final int MAX_LENGTH = 128;

    private static final Pattern VALID = Pattern.compile("^[A-Za-z0-9._-]{1," + MAX_LENGTH + "}$");

    private TaskType() {
    }

    public static boolean isValid(String taskType) {
        return taskType != null && VALID.matcher(taskType).matches();
    }

    public static String requireValid(String taskType) {
        if (!isValid(taskType)) {
            throw new IllegalArgumentException(
                    "task type must match " + VALID.pattern() + " but was: " + taskType);
        }
        return taskType;
    }
}
