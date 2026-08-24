package dev.sentinel.engine.domain;

import java.time.Instant;
import java.util.Objects;

/**
 * A workflow about to be inserted.
 *
 * @param name human-facing workflow name
 * @param submissionKey optional client-supplied idempotency key; resubmitting the same key returns
 *        the existing execution rather than starting a second one
 * @param scheduledAt optional future trigger time (FR8). Null means "start now", expressed as the
 *        database's {@code now()} rather than the caller's clock
 */
public record NewWorkflow(String name, String submissionKey, Instant scheduledAt) {

    public NewWorkflow {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("workflow name must not be blank");
        }
    }

    public static NewWorkflow immediate(String name) {
        return new NewWorkflow(name, null, null);
    }
}
