package dev.sentinel.worker;

import java.util.Objects;

/**
 * What a handler decided about its task.
 *
 * <p>Three outcomes, and the difference between the last two is the one worth understanding.
 *
 * <p>{@link #failed} means "this might work next time": a timeout, a connection reset, a downstream
 * service returning a 5xx. The engine will retry it with backoff until the attempt budget runs out.
 *
 * <p>{@link #permanentlyFailed} means "this will never work": the input is malformed, the record
 * does not exist, the downstream service rejected the request as invalid. The engine gives up
 * immediately rather than spending nine more attempts to reach the same answer, which frees worker
 * capacity and, more importantly, gets the caller a terminal answer instead of a long silence.
 *
 * <p>Classifying honestly matters. Marking a genuine bug as retryable turns one failure into ten,
 * and marking a transient network blip as permanent fails a workflow that would have succeeded a
 * second later.
 */
public sealed interface TaskResult {

    /** The task succeeded. */
    static TaskResult completed(String output) {
        return new Completed(output == null ? "null" : output);
    }

    /** The task succeeded and produced nothing worth passing on. */
    static TaskResult completed() {
        return new Completed("null");
    }

    /** The task failed in a way that may succeed on a later attempt. */
    static TaskResult failed(String message) {
        return new Failed(message, false, null);
    }

    /** The task failed in a way no retry can fix. */
    static TaskResult permanentlyFailed(String message) {
        return new Failed(message, true, null);
    }

    /**
     * Built by the SDK when a handler throws, so the exception type is reported as its own field.
     *
     * <p>Keeping the class out of the message means "how often does this task type throw
     * SocketTimeoutException" is a query rather than a text search, which is the difference between
     * noticing a pattern and never noticing it.
     */
    static TaskResult fromException(Throwable thrown) {
        return new Failed(thrown.getMessage(), false, thrown.getClass().getName());
    }

    /**
     * A successful outcome.
     *
     * @param output a JSON document, which downstream tasks can read. Capped by the engine at
     *        256 KB; anything larger belongs in storage the client owns, passed on as a reference
     */
    record Completed(String output) implements TaskResult {

        public Completed {
            Objects.requireNonNull(output, "output");
        }
    }

    /**
     * An unsuccessful outcome.
     *
     * @param message operator-facing detail, recorded against the task and visible when querying
     *        the workflow
     * @param permanent whether the engine should stop retrying immediately
     * @param errorClass the exception type when this came from a throw, otherwise null
     */
    record Failed(String message, boolean permanent, String errorClass) implements TaskResult {

        public Failed {
            message = message == null ? "" : message;
        }
    }
}
