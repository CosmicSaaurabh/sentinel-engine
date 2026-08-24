package dev.sentinel.engine.error;

import java.io.Serial;
import java.util.UUID;

/**
 * A guarded update matched no row, and the reason is not a recognised benign race.
 *
 * <p>Every state change in this engine is a conditional {@code UPDATE} whose {@code WHERE} clause
 * restates the precondition. Zero affected rows is therefore never something to log and move past:
 * it means the caller's belief about the world was wrong. This exception is how that surfaces
 * loudly instead of as a silent no-op.
 */
public final class IllegalTransitionException extends SentinelException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final transient UUID entityId;

    public IllegalTransitionException(String operation, UUID entityId, String detail) {
        super("%s was rejected for %s: %s".formatted(operation, entityId, detail));
        this.entityId = entityId;
    }

    public UUID entityId() {
        return entityId;
    }
}
