package dev.sentinel.worker;

import java.io.Serial;
import java.time.Duration;

/**
 * The engine is shedding load and refused this call.
 *
 * <p>Carries the engine's own retry hint, which is already jittered server-side so that a refused
 * fleet does not return in a single wave. Honouring the hint rather than retrying immediately is
 * the difference between backing off and joining a stampede.
 */
public final class EngineOverloadedException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final transient Duration retryAfter;

    public EngineOverloadedException(String message, Duration retryAfter) {
        super(message);
        this.retryAfter = retryAfter;
    }

    public Duration retryAfter() {
        return retryAfter;
    }
}
