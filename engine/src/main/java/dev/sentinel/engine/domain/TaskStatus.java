package dev.sentinel.engine.domain;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Lifecycle of a single task instance.
 *
 * <p>{@link #BLOCKED} and {@link #PENDING} are separate states rather than one state plus a
 * dependency check. The claim index is partial on {@code status = 'PENDING'}, so {@code PENDING}
 * has to mean exactly "claimable now or at next_attempt_at". If blocked tasks shared that status,
 * every claim would walk index entries for tasks it can never take.
 *
 * <p>The names here must stay identical to the {@code tasks_status_check} constraint in
 * {@code V2__core_schema.sql}. {@code TaskStatusSchemaAlignmentTest} enforces that.
 */
public enum TaskStatus {

    /** Waiting on at least one parent task. Not claimable, not in the claim index. */
    BLOCKED,

    /** Claimable at or after {@code next_attempt_at}. The only status the claim query looks at. */
    PENDING,

    /** Owned by a worker under a lease. Exactly these rows carry an owner and a lease deadline. */
    RUNNING,

    /** Finished successfully. */
    COMPLETED,

    /** Finished unsuccessfully and will not be retried: attempts exhausted, or a permanent failure. */
    FAILED,

    /** Never ran and never will, because an ancestor failed. Satisfies FR5 deterministically. */
    CANCELLED;

    /**
     * Legal transitions. Held in the domain rather than only in SQL so that tests can enumerate
     * every illegal pair instead of listing the ones somebody remembered to think about.
     */
    private static final Map<TaskStatus, Set<TaskStatus>> ALLOWED = Map.of(
            BLOCKED, EnumSet.of(PENDING, CANCELLED),
            PENDING, EnumSet.of(RUNNING, CANCELLED),
            RUNNING, EnumSet.of(COMPLETED, FAILED, PENDING),
            COMPLETED, EnumSet.noneOf(TaskStatus.class),
            FAILED, EnumSet.noneOf(TaskStatus.class),
            CANCELLED, EnumSet.noneOf(TaskStatus.class));

    /** A terminal task never changes status again, whatever any late-arriving worker claims. */
    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == CANCELLED;
    }

    public boolean canTransitionTo(TaskStatus next) {
        return ALLOWED.get(this).contains(next);
    }

    public Set<TaskStatus> allowedTransitions() {
        return Set.copyOf(ALLOWED.get(this));
    }
}
