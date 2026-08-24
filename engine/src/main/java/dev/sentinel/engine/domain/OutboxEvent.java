package dev.sentinel.engine.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * A history event written atomically with the state change it describes.
 *
 * @param relayedAt null until a relay has shipped the event onward. The partial index that drives
 *        the relay covers only unrelayed rows, so a delivered event stops costing index space
 */
public record OutboxEvent(
        UUID id,
        UUID workflowId,
        UUID taskId,
        OutboxEventType eventType,
        String payload,
        Instant createdAt,
        Instant relayedAt) {

    public OutboxEvent {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(workflowId, "workflowId");
        Objects.requireNonNull(eventType, "eventType");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(createdAt, "createdAt");
    }

    public Optional<UUID> taskIdIfPresent() {
        return Optional.ofNullable(taskId);
    }

    public boolean isRelayed() {
        return relayedAt != null;
    }
}
