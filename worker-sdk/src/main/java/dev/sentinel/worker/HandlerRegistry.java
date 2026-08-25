package dev.sentinel.worker;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Which handler runs which task type.
 *
 * <p>The registered type names are also what the worker advertises when it polls, so the engine
 * only ever hands it work it can actually run. A worker with no handlers registered would claim
 * nothing, which the SDK rejects at startup as a configuration mistake rather than letting it look
 * like a healthy but permanently idle worker.
 */
final class HandlerRegistry {

    private final Map<String, ActivityHandler> handlers = new ConcurrentHashMap<>();

    void register(String taskType, ActivityHandler handler) {
        Objects.requireNonNull(taskType, "taskType");
        Objects.requireNonNull(handler, "handler");
        if (taskType.isBlank()) {
            throw new IllegalArgumentException("taskType must not be blank");
        }
        ActivityHandler previous = handlers.putIfAbsent(taskType, handler);
        if (previous != null) {
            // Silently replacing would mean a copy-paste error in registration surfaces much later
            // as "the wrong code ran", which is far harder to diagnose than a startup failure.
            throw new IllegalStateException("a handler is already registered for task type " + taskType);
        }
    }

    ActivityHandler handlerFor(String taskType) {
        return handlers.get(taskType);
    }

    List<String> registeredTypes() {
        return List.copyOf(handlers.keySet());
    }

    boolean isEmpty() {
        return handlers.isEmpty();
    }
}
