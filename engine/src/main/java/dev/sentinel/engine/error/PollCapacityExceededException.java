package dev.sentinel.engine.error;

import java.io.Serial;
import java.time.Duration;

/**
 * Too many polls are already waiting on this engine instance.
 *
 * <p>A long poll holds a request open, and holding requests open is how an unbounded queue gets
 * built by accident. Virtual threads make a waiting poll cheap, which is exactly what makes it
 * tempting to accept an unlimited number of them; cheap is not free, and an engine with fifty
 * thousand parked waiters is an engine that will fall over in a way no metric predicted.
 *
 * <p>Shedding the overflow keeps the load visible. A rejected worker backs off and may reach a
 * different instance; a silently queued one just waits.
 */
public final class PollCapacityExceededException extends SentinelException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final transient Duration retryAfter;

    public PollCapacityExceededException(int waitingLimit, Duration retryAfter) {
        super("this engine instance already has %d polls waiting; retry after %s"
                .formatted(waitingLimit, retryAfter));
        this.retryAfter = retryAfter;
    }

    public Duration retryAfter() {
        return retryAfter;
    }
}
