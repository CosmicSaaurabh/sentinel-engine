package dev.sentinel.engine.support;

import dev.sentinel.engine.domain.TaskDefinition;
import dev.sentinel.engine.domain.WorkflowDefinition;
import java.util.List;

/** Workflow definitions the service tests keep needing. */
public final class Workflows {

    public static final String TASK_TYPE = "test.echo";

    private Workflows() {
    }

    public static WorkflowDefinition singleTask() {
        return WorkflowDefinition.of("single", TaskDefinition.of("only", TASK_TYPE));
    }

    /**
     * root fans out to left and right, both of which join depends on.
     *
     * <p>The shape the task breakdown calls for: it exercises fan-out, fan-in, the dependency
     * counter, and the failure branch where a task downstream of a failure must never run.
     */
    public static WorkflowDefinition diamond() {
        return WorkflowDefinition.of("diamond",
                TaskDefinition.of("root", TASK_TYPE),
                TaskDefinition.of("left", TASK_TYPE, "root"),
                TaskDefinition.of("right", TASK_TYPE, "root"),
                TaskDefinition.of("join", TASK_TYPE, "left", "right"));
    }

    public static WorkflowDefinition chain(String... names) {
        List<TaskDefinition> tasks = new java.util.ArrayList<>();
        for (int i = 0; i < names.length; i++) {
            tasks.add(i == 0
                    ? TaskDefinition.of(names[i], TASK_TYPE)
                    : TaskDefinition.of(names[i], TASK_TYPE, names[i - 1]));
        }
        return new WorkflowDefinition("chain", null, null, tasks);
    }
}
