package dev.sentinel.engine.domain;

/**
 * Picks the retry behaviour for a task type.
 *
 * <p>A separate lookup rather than a method on {@link RetryPolicy}, so that the completion path
 * learns nothing new: it asks once and uses what it gets, exactly as it did when there was a single
 * global policy.
 */
@FunctionalInterface
public interface RetryPolicyResolver {

    /** Never null. A task type with no override gets the default policy. */
    RetryPolicy forTaskType(String taskType);
}
