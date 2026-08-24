package dev.sentinel.engine.error;

import java.io.Serial;
import java.util.UUID;

/** A workflow or task id that does not exist. */
public final class EntityNotFoundException extends SentinelException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final transient UUID entityId;

    private EntityNotFoundException(String entityKind, UUID entityId) {
        super("%s %s does not exist".formatted(entityKind, entityId));
        this.entityId = entityId;
    }

    public static EntityNotFoundException workflow(UUID workflowId) {
        return new EntityNotFoundException("workflow", workflowId);
    }

    public static EntityNotFoundException task(UUID taskId) {
        return new EntityNotFoundException("task", taskId);
    }

    public UUID entityId() {
        return entityId;
    }
}
