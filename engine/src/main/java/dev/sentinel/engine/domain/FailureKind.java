package dev.sentinel.engine.domain;

/**
 * Whether a task failure is worth retrying.
 *
 * <p>Retrying a {@link #PERMANENT} failure burns the whole attempt budget to reach the same
 * outcome, so a permanent failure goes terminal after one attempt instead of ten.
 */
public enum FailureKind {

    /** Transient: timeout, connection reset, downstream 5xx. Retried with backoff. */
    RETRYABLE,

    /** Deterministic: validation error, downstream 4xx-style rejection. Goes terminal immediately. */
    PERMANENT
}
