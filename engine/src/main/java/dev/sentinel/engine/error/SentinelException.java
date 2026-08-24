package dev.sentinel.engine.error;

import java.io.Serial;

/**
 * Base for every failure this engine raises deliberately.
 *
 * <p>Unchecked on purpose: none of these are recoverable at the call site. They are classified at
 * the transport boundary into a gRPC status and returned to the caller, which is the one place
 * that knows how to talk about failure to the outside world.
 */
public abstract class SentinelException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    protected SentinelException(String message) {
        super(message);
    }

    protected SentinelException(String message, Throwable cause) {
        super(message, cause);
    }
}
