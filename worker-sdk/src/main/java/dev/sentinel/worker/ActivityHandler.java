package dev.sentinel.worker;

/**
 * The business logic for one task type. This is the only interface most users implement.
 *
 * <p>A handler receives everything it needs in the {@link ActivityContext} and returns a
 * {@link TaskResult}. It does not talk to the engine, renew its own lease, or decide whether to
 * retry: the SDK owns all of that, so a handler stays ordinary Java that can be unit tested by
 * calling it.
 *
 * <h2>Throwing is allowed and expected</h2>
 *
 * <p>The signature declares {@code throws Exception} on purpose. Requiring handlers to catch
 * everything themselves produces the worst possible outcome, which is handlers that wrap their body
 * in {@code catch (Exception e) {}} to satisfy the compiler and swallow real failures. The SDK
 * catches, classifies as retryable, records the exception's class and message, and reports it.
 *
 * <p>An uncaught exception therefore fails the task, not the worker. The thread survives, the
 * permit is released, and the worker carries on.
 *
 * <h2>What a handler must do for itself</h2>
 *
 * <ul>
 *   <li><strong>Be idempotent, using {@link ActivityContext#taskId()}.</strong> Execution is
 *       at-least-once. The same task can genuinely run twice, and only the handler can make the
 *       second run harmless.</li>
 *   <li><strong>Have its own timeouts.</strong> The SDK will not interrupt a handler that never
 *       returns. Such a handler holds one of the worker's concurrency permits until the process
 *       ends.</li>
 * </ul>
 */
@FunctionalInterface
public interface ActivityHandler {

    /**
     * Runs one task.
     *
     * @return what happened. Returning null is treated as a retryable failure rather than as
     *         success, because a null here is far more likely to be a forgotten return path than a
     *         deliberate statement that the task worked
     * @throws Exception any failure, which the SDK reports as retryable
     */
    TaskResult handle(ActivityContext context) throws Exception;
}
