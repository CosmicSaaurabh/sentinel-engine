package dev.sentinel.engine.repository.support;

import dev.sentinel.engine.error.IllegalTransitionException;
import java.util.UUID;

/**
 * Assertions on the affected-row count of a guarded update.
 *
 * <p>Every state change in this engine is a conditional {@code UPDATE} that restates its
 * precondition in the {@code WHERE} clause. That design only pays off if callers actually look at
 * how many rows changed. An ignored row count turns a rejected transition into a silent no-op,
 * which is the exact failure this pattern exists to prevent, so it is a review-blocking defect
 * here.
 *
 * <p>Two outcomes are meaningfully different and are handled differently:
 * <ul>
 *   <li>zero rows can be legitimate control flow (a race the caller expects), so
 *       {@link #applied} reports it rather than throwing;</li>
 *   <li>more than one row is never legitimate for a single-row guard and always means the
 *       predicate is wrong, so it throws immediately rather than corrupting more state.</li>
 * </ul>
 */
public final class GuardedUpdate {

    private GuardedUpdate() {
    }

    /**
     * @return true when the guard matched, false when the precondition did not hold
     * @throws IllegalStateException if a single-row guard matched several rows, which is a bug in
     *         the predicate rather than a runtime condition
     */
    public static boolean applied(int rowsAffected, String operation, UUID entityId) {
        if (rowsAffected > 1) {
            throw new IllegalStateException(
                    "%s affected %d rows for %s; a single-row guard must never match more than one row"
                            .formatted(operation, rowsAffected, entityId));
        }
        return rowsAffected == 1;
    }

    /** For transitions where the precondition failing means the caller is simply wrong. */
    public static void requireApplied(int rowsAffected, String operation, UUID entityId, String detail) {
        if (!applied(rowsAffected, operation, entityId)) {
            throw new IllegalTransitionException(operation, entityId, detail);
        }
    }

    /** For bulk statements where the caller knows how many rows must change. */
    public static void requireExactly(int expected, int rowsAffected, String operation, UUID entityId) {
        if (rowsAffected != expected) {
            throw new IllegalTransitionException(
                    operation, entityId, "expected %d rows to change but %d did".formatted(expected, rowsAffected));
        }
    }
}
