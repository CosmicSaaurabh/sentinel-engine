package dev.sentinel.engine.support;

import dev.sentinel.engine.domain.NewTask;
import dev.sentinel.engine.domain.NewWorkflow;
import dev.sentinel.engine.domain.TaskEdge;
import dev.sentinel.engine.repository.TaskDependencyRepository;
import dev.sentinel.engine.repository.TaskRepository;
import dev.sentinel.engine.repository.WorkflowRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Builders for the DAG shapes these tests keep needing. */
public final class DagFixtures {

    public static final String TASK_TYPE = "test.echo";

    private DagFixtures() {
    }

    /** A submitted workflow and the generated id of each of its tasks, by task name. */
    public record SubmittedDag(UUID workflowId, Map<String, UUID> taskIds) {

        public UUID taskId(String name) {
            UUID id = taskIds.get(name);
            if (id == null) {
                throw new IllegalArgumentException("no task named " + name + " in " + taskIds.keySet());
            }
            return id;
        }
    }

    /** One workflow, one immediately claimable task. */
    public static SubmittedDag singleTask(
            WorkflowRepository workflows, TaskRepository tasks, String taskName) {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("single")).id();
        Map<String, UUID> ids = tasks.insertAll(
                workflowId, List.of(NewTask.runnable(taskName, TASK_TYPE, "{\"n\":1}")));
        return new SubmittedDag(workflowId, ids);
    }

    /** A chain: names[0] is runnable, every later task blocks on the one before it. */
    public static SubmittedDag chain(
            WorkflowRepository workflows,
            TaskRepository tasks,
            TaskDependencyRepository dependencies,
            String... names) {

        UUID workflowId = workflows.insert(NewWorkflow.immediate("chain")).id();

        List<NewTask> newTasks = new ArrayList<>();
        for (int i = 0; i < names.length; i++) {
            newTasks.add(i == 0
                    ? NewTask.runnable(names[i], TASK_TYPE, "{}")
                    : NewTask.blocked(names[i], TASK_TYPE, "{}", 1));
        }
        Map<String, UUID> ids = tasks.insertAll(workflowId, newTasks);

        List<TaskEdge> edges = new ArrayList<>();
        for (int i = 1; i < names.length; i++) {
            edges.add(new TaskEdge(ids.get(names[i]), ids.get(names[i - 1])));
        }
        dependencies.insertAll(workflowId, edges);

        return new SubmittedDag(workflowId, ids);
    }

    /**
     * A diamond: {@code root} fans out to {@code left} and {@code right}, both of which
     * {@code join} depends on. The shape that catches counter bugs and recursion bugs, because
     * {@code join} is reachable from {@code root} by two different paths.
     */
    public static SubmittedDag diamond(
            WorkflowRepository workflows, TaskRepository tasks, TaskDependencyRepository dependencies) {

        UUID workflowId = workflows.insert(NewWorkflow.immediate("diamond")).id();

        Map<String, UUID> ids = tasks.insertAll(workflowId, List.of(
                NewTask.runnable("root", TASK_TYPE, "{}"),
                NewTask.blocked("left", TASK_TYPE, "{}", 1),
                NewTask.blocked("right", TASK_TYPE, "{}", 1),
                NewTask.blocked("join", TASK_TYPE, "{}", 2)));

        dependencies.insertAll(workflowId, List.of(
                new TaskEdge(ids.get("left"), ids.get("root")),
                new TaskEdge(ids.get("right"), ids.get("root")),
                new TaskEdge(ids.get("join"), ids.get("left")),
                new TaskEdge(ids.get("join"), ids.get("right"))));

        return new SubmittedDag(workflowId, ids);
    }
}
