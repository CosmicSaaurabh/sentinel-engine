package dev.sentinel.worker;

import java.util.Map;
import org.slf4j.MDC;

/**
 * Puts task identifiers into the logging context while a handler runs, and takes them out again.
 *
 * <h2>Why the handler's own logs need this</h2>
 *
 * <p>Handler code logs whatever the application logs. Without context, a line like "charging card"
 * from a worker running eight tasks at once is unattributable, and the trace id is the only thing
 * that connects it to what the engine recorded about the same task.
 *
 * <p>The SDK cannot make the application's logs structured, and does not try. What it can do is
 * ensure that if the application uses slf4j at all, every line it writes inside a handler carries
 * the ids for free.
 *
 * <h2>Why it must be restored, not cleared</h2>
 *
 * <p>MDC is thread-local and activity threads are pooled. A value left behind reappears on the next
 * unrelated task that lands on the same thread, which is worse than no context: the logs are then
 * confidently wrong, and every conclusion drawn from them is suspect. Implementing
 * {@link AutoCloseable} makes the restore impossible to forget, since the only way to set the
 * context is a try-with-resources.
 */
final class LogContext implements AutoCloseable {

    static final String WORKFLOW_ID = "workflowId";
    static final String TASK_ID = "taskId";
    static final String TASK_NAME = "taskName";
    static final String TASK_TYPE = "taskType";
    static final String ATTEMPT = "attempt";
    static final String TRACE_ID = "traceId";

    private final Map<String, String> previous;

    private LogContext(Map<String, String> previous) {
        this.previous = previous;
    }

    static LogContext forTask(EngineClient.AssignedTask task) {
        Map<String, String> previous = new java.util.HashMap<>();
        for (String key : new String[] {WORKFLOW_ID, TASK_ID, TASK_NAME, TASK_TYPE, ATTEMPT, TRACE_ID}) {
            previous.put(key, MDC.get(key));
        }

        MDC.put(WORKFLOW_ID, task.workflowId().toString());
        MDC.put(TASK_ID, task.taskId().toString());
        MDC.put(TASK_NAME, task.name());
        MDC.put(TASK_TYPE, task.taskType());
        MDC.put(ATTEMPT, Integer.toString(task.attempt()));

        traceIdOf(task.traceContext()).ifPresent(traceId -> MDC.put(TRACE_ID, traceId));
        return new LogContext(previous);
    }

    /**
     * Pulls the trace id out of a W3C traceparent.
     *
     * <p>Parsed by position rather than with a library, because the format is fixed at
     * {@code version-traceid-spanid-flags} and pulling one field out of it does not justify a
     * dependency in a library whose whole point is having almost none. Anything that does not look
     * like a traceparent is ignored rather than rejected: a malformed header from somewhere upstream
     * must not fail a task.
     */
    private static java.util.Optional<String> traceIdOf(String traceParent) {
        if (traceParent == null) {
            return java.util.Optional.empty();
        }
        String[] parts = traceParent.split("-");
        return parts.length >= 3 && !parts[1].isBlank()
                ? java.util.Optional.of(parts[1])
                : java.util.Optional.empty();
    }

    @Override
    public void close() {
        previous.forEach((key, value) -> {
            if (value == null) {
                MDC.remove(key);
            } else {
                MDC.put(key, value);
            }
        });
    }
}
