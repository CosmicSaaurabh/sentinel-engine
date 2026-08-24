package dev.sentinel.engine.domain;

/**
 * Lifecycle of a workflow execution.
 *
 * <p>There is deliberately no {@code PENDING}. A workflow scheduled for the future is
 * {@link #RUNNING} from submission; the delay lives on its root tasks' {@code next_attempt_at},
 * because the queue already knows how to not claim a task before its time.
 */
public enum WorkflowStatus {

    RUNNING,
    COMPLETED,
    FAILED;

    public boolean isTerminal() {
        return this != RUNNING;
    }
}
