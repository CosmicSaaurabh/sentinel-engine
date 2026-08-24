package dev.sentinel.engine.domain;

import java.time.Instant;
import java.util.Objects;

/**
 * Ownership of a {@link TaskStatus#RUNNING} task by one worker, until a deadline.
 *
 * <p>A lease is not proof of ownership on its own. The proof is the task's fencing token, which
 * increments on every claim; the lease only says when the engine stops waiting for this worker.
 *
 * @param ownerWorkerId the worker that claimed the task
 * @param expiresAt when the reaper becomes free to re-queue the task, from the database clock
 */
public record Lease(String ownerWorkerId, Instant expiresAt) {

    public Lease {
        Objects.requireNonNull(ownerWorkerId, "ownerWorkerId");
        Objects.requireNonNull(expiresAt, "expiresAt");
        if (ownerWorkerId.isBlank()) {
            throw new IllegalArgumentException("ownerWorkerId must not be blank");
        }
    }

    /**
     * Whether the lease has passed its deadline relative to the supplied instant.
     *
     * <p>Callers must pass a database-sourced instant. Comparing against a JVM clock reintroduces
     * exactly the skew the schema avoids by generating every timestamp with {@code now()}.
     */
    public boolean hasExpiredAt(Instant databaseNow) {
        return expiresAt.isBefore(databaseNow);
    }
}
