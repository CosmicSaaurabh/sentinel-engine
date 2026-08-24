package dev.sentinel.engine.error;

import java.io.Serial;
import java.util.UUID;

/**
 * A worker tried to act on a task it no longer owns.
 *
 * <p>The scenario this exists for: a worker stalls long enough for its lease to expire, the reaper
 * re-queues the task, another worker claims and finishes it, and then the original worker wakes up
 * and reports. Its fencing token is now behind the row's, so its write matches nothing.
 *
 * <p>The engine's answer is deterministic rejection rather than a best guess, which is what keeps
 * at-least-once execution from turning into duplicated bookkeeping.
 */
public final class StaleFencingTokenException extends SentinelException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final transient UUID taskId;
    private final long presentedToken;
    private final long currentToken;

    public StaleFencingTokenException(UUID taskId, String workerId, long presentedToken, long currentToken) {
        super("worker %s presented fencing token %d for task %s but the current token is %d; the lease was lost"
                .formatted(workerId, presentedToken, taskId, currentToken));
        this.taskId = taskId;
        this.presentedToken = presentedToken;
        this.currentToken = currentToken;
    }

    public UUID taskId() {
        return taskId;
    }

    public long presentedToken() {
        return presentedToken;
    }

    public long currentToken() {
        return currentToken;
    }
}
