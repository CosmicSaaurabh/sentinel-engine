package dev.sentinel.engine.error;

import java.io.Serial;
import java.util.List;

/**
 * A submitted DAG is structurally wrong.
 *
 * <p>Carries every violation found rather than the first one. A client fixing a generated workflow
 * definition one error per round trip is a miserable experience, and validation here is cheap
 * enough that there is no reason to stop early.
 *
 * <p>Nothing is written to the database when this is thrown. Structural validation happens entirely
 * before the submission transaction opens.
 */
public final class InvalidWorkflowException extends SentinelException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final transient List<String> violations;

    public InvalidWorkflowException(List<String> violations) {
        super("workflow definition is invalid: " + String.join("; ", violations));
        this.violations = List.copyOf(violations);
    }

    public List<String> violations() {
        return violations;
    }
}
