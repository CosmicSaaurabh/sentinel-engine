package dev.sentinel.engine.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * A workflow execution as stored.
 *
 * <p>Every timestamp here originates from the database clock. Nothing in this record is ever
 * populated from {@code Instant.now()} on an engine instance, because lease and scheduling maths
 * downstream assumes a single clock.
 */
public record Workflow(
        UUID id,
        String name,
        WorkflowStatus status,
        String submissionKey,
        Instant scheduledAt,
        Instant startedAt,
        Instant finishedAt,
        Instant createdAt,
        Instant updatedAt) {

    public Workflow {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(scheduledAt, "scheduledAt");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");
    }

    public Optional<String> submissionKeyIfPresent() {
        return Optional.ofNullable(submissionKey);
    }
}
