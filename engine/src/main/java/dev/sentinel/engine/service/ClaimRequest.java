package dev.sentinel.engine.service;

import dev.sentinel.engine.domain.TaskType;
import java.util.List;
import java.util.Objects;

/**
 * A worker asking for work.
 *
 * @param workerId stable identity of the worker process, used for lease ownership and for choosing
 *        a counter shard. Must survive a reconnect, or the engine cannot tell a returning worker
 *        from a new one
 * @param taskTypes routing keys this worker can execute. An empty list means the worker can run
 *        nothing, which is answered with an empty result rather than an error
 * @param batchSize how many tasks the worker has capacity for right now, clamped by the engine
 */
public record ClaimRequest(String workerId, List<String> taskTypes, int batchSize) {

    public ClaimRequest {
        Objects.requireNonNull(workerId, "workerId");
        Objects.requireNonNull(taskTypes, "taskTypes");
        if (workerId.isBlank()) {
            throw new IllegalArgumentException("workerId must not be blank");
        }
        taskTypes.forEach(TaskType::requireValid);
        taskTypes = List.copyOf(taskTypes);
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be at least 1, was " + batchSize);
        }
    }
}
