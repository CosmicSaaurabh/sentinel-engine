package dev.sentinel.engine.domain;

import java.util.List;
import java.util.Objects;

/**
 * One task as the client describes it at submission time.
 *
 * <p>Deliberately name-based rather than id-based. The client has no ids yet, and naming
 * dependencies within the submission is what makes it structurally impossible to declare a
 * dependency on a task in a different workflow. That rule is not a convention here, it is the
 * property the sharding strategy rests on, so the wire format enforces it rather than a check
 * somewhere downstream.
 *
 * @param maxAttempts null means "use the engine default", so a client that does not care does not
 *        have to know the number
 * @param dependsOn names of tasks in this same submission that must complete first
 */
public record TaskDefinition(
        String name, String taskType, String input, Integer maxAttempts, List<String> dependsOn) {

    public TaskDefinition {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(taskType, "taskType");
        dependsOn = dependsOn == null ? List.of() : List.copyOf(dependsOn);
        input = input == null ? "{}" : input;
    }

    public static TaskDefinition of(String name, String taskType, String... dependsOn) {
        return new TaskDefinition(name, taskType, "{}", null, List.of(dependsOn));
    }
}
