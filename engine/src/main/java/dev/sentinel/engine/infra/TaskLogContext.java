package dev.sentinel.engine.infra;

import java.util.UUID;
import java.util.function.Supplier;
import org.slf4j.MDC;

/**
 * Puts workflow and task identifiers into the logging context for the duration of a piece of work.
 *
 * <h2>Why every log line needs them</h2>
 *
 * <p>An engine handling thousands of workflows at once produces logs where every interesting line
 * is interleaved with hundreds of unrelated ones. "Task failed, retrying in 4s" is nearly useless
 * on its own: the question is always which task, in which workflow, and what else happened to it.
 * Without identifiers on every line, answering that means correlating by timestamp, which stops
 * working precisely when there is enough traffic to matter.
 *
 * <h2>Why it is scoped rather than set</h2>
 *
 * <p>MDC is thread-local, and engine threads are pooled and reused. A value left behind after a
 * request finishes reappears on the next unrelated piece of work that lands on the same thread,
 * which is worse than having no context at all: the logs are then confidently wrong, and every
 * conclusion drawn from them is suspect.
 *
 * <p>So the only way to set context here is around a piece of work, and the previous value is
 * restored rather than cleared, because these can nest.
 */
public final class TaskLogContext {

    public static final String WORKFLOW_ID = "workflowId";
    public static final String TASK_ID = "taskId";
    public static final String TASK_TYPE = "taskType";
    public static final String WORKER_ID = "workerId";

    private TaskLogContext() {
    }

    public static <T> T forTask(UUID workflowId, UUID taskId, Supplier<T> work) {
        String previousWorkflow = MDC.get(WORKFLOW_ID);
        String previousTask = MDC.get(TASK_ID);
        try {
            put(WORKFLOW_ID, workflowId);
            put(TASK_ID, taskId);
            return work.get();
        } finally {
            restore(WORKFLOW_ID, previousWorkflow);
            restore(TASK_ID, previousTask);
        }
    }

    public static void runForWorkflow(UUID workflowId, Runnable work) {
        String previous = MDC.get(WORKFLOW_ID);
        try {
            put(WORKFLOW_ID, workflowId);
            work.run();
        } finally {
            restore(WORKFLOW_ID, previous);
        }
    }

    private static void put(String key, UUID value) {
        if (value != null) {
            MDC.put(key, value.toString());
        }
    }

    private static void restore(String key, String previous) {
        if (previous == null) {
            MDC.remove(key);
        } else {
            MDC.put(key, previous);
        }
    }
}
