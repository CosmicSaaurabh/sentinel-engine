package dev.sentinel.engine.domain;

/**
 * How a failure came to be known.
 *
 * <p>Wider than {@link FailureKind} by exactly one value, and that value carries most of the
 * diagnostic weight. A reported failure says something about the task; a lapsed lease says
 * something about the worker or the network, because it means nobody was there to report at all.
 * Collapsing them into one category would hide the difference between "this task is broken" and
 * "this fleet is unhealthy", which call for completely different responses.
 */
public enum FailureRecordKind {

    RETRYABLE,
    PERMANENT,
    LEASE_EXPIRED;

    public static FailureRecordKind of(FailureKind kind) {
        return switch (kind) {
            case RETRYABLE -> RETRYABLE;
            case PERMANENT -> PERMANENT;
        };
    }
}
