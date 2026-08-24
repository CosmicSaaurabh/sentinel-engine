package dev.sentinel.engine.domain;

import java.util.Objects;

/**
 * The last failure recorded against a task, and whether it was worth retrying.
 *
 * @param message operator-facing failure detail, truncated by the caller before it reaches here
 * @param kind whether this failure was retried or went straight to terminal
 */
public record TaskFailure(String message, FailureKind kind) {

    /** Failure messages are unbounded on the wire; the column is not, so they are clipped. */
    public static final int MAX_MESSAGE_LENGTH = 4_000;

    public TaskFailure {
        Objects.requireNonNull(kind, "kind");
        message = truncate(message);
    }

    private static String truncate(String message) {
        if (message == null) {
            return "";
        }
        return message.length() <= MAX_MESSAGE_LENGTH ? message : message.substring(0, MAX_MESSAGE_LENGTH);
    }
}
