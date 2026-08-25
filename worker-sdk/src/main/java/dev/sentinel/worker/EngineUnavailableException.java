package dev.sentinel.worker;

import java.io.Serial;

/**
 * The engine could not be reached, or reported that it cannot reach its own database.
 *
 * <p>Always worth retrying with backoff. This is the engine declining to guess rather than failing:
 * it chooses to be briefly unavailable over risking a wrong answer about task state.
 */
public final class EngineUnavailableException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    public EngineUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
