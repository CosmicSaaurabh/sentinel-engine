package dev.sentinel.engine.error;

import java.io.Serial;
import java.time.Duration;

/**
 * The engine is at capacity and is refusing new work.
 *
 * <p>Refusing outright rather than accepting and queueing is the whole point. Nothing was
 * acknowledged, so there is no durability promise to honour, and the client keeps the choice about
 * what to do next. Accepting work the engine cannot run would turn a visible rejection into an
 * invisible backlog, which is how a busy system becomes an unavailable one.
 *
 * @param retryAfter how long the client should wait, already jittered by the caller so that a
 *        rejected fleet does not return in one synchronised wave
 */
public final class AdmissionRejectedException extends SentinelException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final transient Duration retryAfter;

    public AdmissionRejectedException(long inFlight, long capacity, Duration retryAfter) {
        super("engine is at capacity: %d of %d task slots in flight, retry after %s"
                .formatted(inFlight, capacity, retryAfter));
        this.retryAfter = retryAfter;
    }

    public Duration retryAfter() {
        return retryAfter;
    }
}
