package dev.sentinel.engine.domain;

import dev.sentinel.engine.error.InvalidWorkflowException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Proves a submitted workflow is a well-formed DAG, and computes the initial scheduling state as a
 * by-product.
 *
 * <p>Pure domain logic: no Spring, no database, no clock. It runs entirely before the submission
 * transaction opens, so a malformed workflow costs one rejected request rather than a rolled-back
 * write.
 *
 * <h2>Why Kahn's algorithm</h2>
 *
 * <p>Cycle detection could be done with a depth-first search and a colouring scheme, which is the
 * more common textbook answer. Kahn's algorithm is used instead because it computes something this
 * engine needs anyway: the in-degree of every task, which is exactly the {@code pendingDependencies}
 * counter the scheduler decrements at runtime. Detecting cycles and initialising the counters
 * therefore cost one pass rather than two, and the counter can never disagree with the graph it was
 * derived from.
 *
 * <p>The algorithm repeatedly removes tasks that have no unmet dependencies. If tasks remain when
 * nothing more can be removed, every one of them is waiting on another that is also waiting, which
 * is precisely a cycle, and those remaining names are reported.
 *
 * <h2>Decisions on ambiguous input</h2>
 *
 * <ul>
 *   <li><strong>Zero tasks: rejected.</strong> An empty workflow can never leave RUNNING, since
 *       nothing will ever complete to trigger settlement. It would be a permanent no-op that looks
 *       like a stuck workflow.</li>
 *   <li><strong>One task: accepted.</strong> A single-step workflow is a legitimate and common
 *       shape, and gains durability and retries just like any other.</li>
 *   <li><strong>Disconnected components: accepted.</strong> A workflow is a forest, not necessarily
 *       one connected tree. Independent branches submitted together are a normal way to express
 *       "run these, tell me when all of them are done".</li>
 *   <li><strong>Duplicate edges: deduplicated, not rejected.</strong> Declaring the same dependency
 *       twice is harmless intent, but counting it twice would leave the child permanently blocked
 *       with a counter that can never reach zero. Silently correcting is safer than trusting the
 *       caller to be tidy.</li>
 * </ul>
 */
public final class DagValidator {

    private final int maxTasks;
    private final int maxEdges;
    private final java.util.function.ToIntFunction<String> defaultMaxAttempts;

    /**
     * @param defaultMaxAttempts attempt budget for a task type that did not specify its own.
     *        Resolved here, at submission, and then stored on the row, so that a later
     *        configuration change cannot retroactively exhaust a task already in flight
     */
    public DagValidator(int maxTasks, int maxEdges, java.util.function.ToIntFunction<String> defaultMaxAttempts) {
        this.maxTasks = maxTasks;
        this.maxEdges = maxEdges;
        this.defaultMaxAttempts = defaultMaxAttempts;
    }

    /**
     * @throws InvalidWorkflowException listing every violation found, never just the first
     */
    public ValidatedDag validate(WorkflowDefinition definition) {
        List<String> violations = new ArrayList<>();

        if (definition.name().isBlank()) {
            violations.add("workflow name must not be blank");
        }
        if (definition.tasks().isEmpty()) {
            violations.add("a workflow must contain at least one task; an empty workflow could never complete");
        }
        if (definition.tasks().size() > maxTasks) {
            violations.add("a workflow may contain at most %d tasks but this one has %d"
                    .formatted(maxTasks, definition.tasks().size()));
        }

        Set<String> names = collectNames(definition, violations);
        Set<NameEdge> edges = collectEdges(definition, names, violations);

        if (edges.size() > maxEdges) {
            violations.add("a workflow may contain at most %d dependencies but this one has %d"
                    .formatted(maxEdges, edges.size()));
        }

        // Only attempt the topological sort once the graph is known to be well formed. Running it
        // over dangling edges would report a spurious cycle and bury the real error.
        Map<String, Integer> parentCounts = Map.of();
        if (violations.isEmpty()) {
            parentCounts = parentCountsByTask(definition, edges);
            findCycle(definition, edges, parentCounts).ifPresent(violations::add);
        }

        if (!violations.isEmpty()) {
            throw new InvalidWorkflowException(violations);
        }

        return buildValidatedDag(definition, edges, parentCounts);
    }

    private Set<String> collectNames(WorkflowDefinition definition, List<String> violations) {
        Set<String> names = new LinkedHashSet<>();
        Set<String> duplicates = new TreeSet<>();

        for (TaskDefinition task : definition.tasks()) {
            if (task.name().isBlank()) {
                violations.add("task names must not be blank");
                continue;
            }
            if (!names.add(task.name())) {
                duplicates.add(task.name());
            }
            if (!TaskType.isValid(task.taskType())) {
                violations.add("task '%s' has an invalid task type '%s'; task types are limited to "
                        .formatted(task.name(), task.taskType())
                        + "letters, digits, dot, underscore and dash");
            }
            if (task.maxAttempts() != null && task.maxAttempts() < 1) {
                violations.add("task '%s' has maxAttempts %d; it must be at least 1"
                        .formatted(task.name(), task.maxAttempts()));
            }
        }
        duplicates.forEach(name ->
                violations.add("task name '%s' is used more than once; names identify tasks within a workflow"
                        .formatted(name)));
        return names;
    }

    private Set<NameEdge> collectEdges(
            WorkflowDefinition definition, Set<String> names, List<String> violations) {

        Set<NameEdge> edges = new LinkedHashSet<>();
        for (TaskDefinition task : definition.tasks()) {
            for (String parent : task.dependsOn()) {
                if (parent.equals(task.name())) {
                    violations.add("task '%s' depends on itself".formatted(task.name()));
                    continue;
                }
                if (!names.contains(parent)) {
                    violations.add(
                            "task '%s' depends on '%s', which is not part of this workflow; a dependency "
                                    .formatted(task.name(), parent)
                                    + "may never cross a workflow boundary");
                    continue;
                }
                // Deduplication happens here, in the set. A repeated declaration is intent, not
                // an extra blocker, and counting it twice would block the child forever.
                edges.add(new NameEdge(task.name(), parent));
            }
        }
        return edges;
    }

    private static Map<String, Integer> parentCountsByTask(
            WorkflowDefinition definition, Set<NameEdge> edges) {

        Map<String, Integer> counts = new HashMap<>();
        definition.tasks().forEach(task -> counts.put(task.name(), 0));
        edges.forEach(edge -> counts.merge(edge.taskName(), 1, Integer::sum));
        return counts;
    }

    /**
     * Kahn's algorithm. Returns a description of the cycle if one exists.
     *
     * <p>Works on a copy of the in-degree map, because the real counts are needed afterwards to
     * initialise the tasks.
     */
    private static java.util.Optional<String> findCycle(
            WorkflowDefinition definition, Set<NameEdge> edges, Map<String, Integer> parentCounts) {

        Map<String, List<String>> childrenByParent = new HashMap<>();
        edges.forEach(edge -> childrenByParent
                .computeIfAbsent(edge.dependsOnName(), key -> new ArrayList<>())
                .add(edge.taskName()));

        Map<String, Integer> remaining = new HashMap<>(parentCounts);
        Deque<String> ready = new ArrayDeque<>();
        remaining.forEach((name, count) -> {
            if (count == 0) {
                ready.add(name);
            }
        });

        Set<String> settled = new HashSet<>();
        while (!ready.isEmpty()) {
            String name = ready.removeFirst();
            settled.add(name);
            for (String child : childrenByParent.getOrDefault(name, List.of())) {
                if (remaining.merge(child, -1, Integer::sum) == 0) {
                    ready.add(child);
                }
            }
        }

        if (settled.size() == definition.tasks().size()) {
            return java.util.Optional.empty();
        }

        // Whatever is left is, by definition, waiting on something that is itself waiting.
        Set<String> inCycle = new TreeSet<>(remaining.keySet());
        inCycle.removeAll(settled);
        return java.util.Optional.of(
                "the dependency graph contains a cycle involving: " + String.join(", ", inCycle));
    }

    private ValidatedDag buildValidatedDag(
            WorkflowDefinition definition, Set<NameEdge> edges, Map<String, Integer> parentCounts) {

        List<NewTask> tasks = new ArrayList<>(definition.tasks().size());
        for (TaskDefinition task : definition.tasks()) {
            int parents = parentCounts.getOrDefault(task.name(), 0);
            int maxAttempts = task.maxAttempts() == null
                    ? defaultMaxAttempts.applyAsInt(task.taskType())
                    : task.maxAttempts();

            // A trigger time delays the tasks that would otherwise start immediately. Applying it
            // to blocked tasks would be meaningless: they are already waiting on their parents,
            // which are themselves delayed.
            java.time.Instant notBefore = parents == 0 ? definition.scheduledAt() : null;

            tasks.add(new NewTask(
                    task.name(),
                    task.taskType(),
                    parents == 0 ? TaskStatus.PENDING : TaskStatus.BLOCKED,
                    task.input(),
                    maxAttempts,
                    parents,
                    notBefore));
        }

        NewWorkflow workflow =
                new NewWorkflow(definition.name(), definition.submissionKey(), definition.scheduledAt());
        return new ValidatedDag(workflow, tasks, List.copyOf(edges));
    }
}
