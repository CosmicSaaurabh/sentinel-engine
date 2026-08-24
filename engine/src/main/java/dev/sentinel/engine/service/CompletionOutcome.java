package dev.sentinel.engine.service;

/**
 * What a worker's report actually did.
 *
 * <p>The distinction exists because of ack loss. If the engine commits a completion and then dies,
 * or the response is lost on the way back, the worker retries the report in good faith. The second
 * report changes nothing, and treating that as an error would make a healthy retry look like a
 * failure. It is not the same thing as a rejected report, which means the worker no longer owns the
 * task at all and is signalled by an exception.
 */
public enum CompletionOutcome {

    /** The report changed the task's state. */
    RECORDED,

    /** The same worker already reported this outcome; nothing changed and nothing is wrong. */
    ALREADY_RECORDED
}
