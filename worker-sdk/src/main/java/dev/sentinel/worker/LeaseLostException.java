package dev.sentinel.worker;

import java.io.Serial;
import java.util.UUID;

/**
 * This worker no longer owns the task it tried to act on.
 *
 * <p>It happens when a worker stalls long enough for its lease to expire, the engine re-queues the
 * task, and another worker claims it. A long garbage-collection pause or a suspended laptop is
 * enough.
 *
 * <p><strong>Never retry after this.</strong> It is not a transient failure; it is a decision the
 * engine has already made and another worker has already acted on. Retrying is arguing with a
 * settled outcome. The correct response, which the SDK performs automatically, is to abandon the
 * task locally and go back to polling.
 */
public final class LeaseLostException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final transient UUID taskId;

    public LeaseLostException(UUID taskId, String message) {
        super(message);
        this.taskId = taskId;
    }

    public UUID taskId() {
        return taskId;
    }
}
