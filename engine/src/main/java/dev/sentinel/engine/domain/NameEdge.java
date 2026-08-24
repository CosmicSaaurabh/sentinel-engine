package dev.sentinel.engine.domain;

import java.util.Objects;

/**
 * A dependency edge before task ids exist.
 *
 * <p>Submission inserts tasks first, gets their generated ids back, and only then can turn these
 * into {@link TaskEdge} rows. Keeping the two types separate means an unresolved edge cannot be
 * passed where a resolved one is expected.
 */
public record NameEdge(String taskName, String dependsOnName) {

    public NameEdge {
        Objects.requireNonNull(taskName, "taskName");
        Objects.requireNonNull(dependsOnName, "dependsOnName");
    }
}
