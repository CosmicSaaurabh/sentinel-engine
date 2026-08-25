package dev.sentinel.worker;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Everything the SDK needs from the engine, expressed without gRPC in the signatures.
 *
 * <p>An interface rather than a direct dependency on the generated stubs, and the reason is
 * testability of the parts most likely to be wrong. The SDK's genuinely tricky behaviour is its
 * threading, its permit accounting, its backoff and its shutdown ordering. Testing those through a
 * real transport means a server, a port, and sleeping until things happen. Behind this interface
 * they can be tested against a fake in microseconds, deterministically, including failure paths
 * that are awkward to provoke over a network.
 */
interface EngineClient extends AutoCloseable {

    /**
     * Asks for work, blocking until the engine answers.
     *
     * <p>An empty list is the ordinary answer to an empty queue, not an error. The engine has
     * already done the waiting, so the caller should ask again straight away rather than sleeping
     * on top of a wait that already happened.
     *
     * @throws EngineUnavailableException if the engine could not be reached
     * @throws EngineOverloadedException if the engine is shedding load, carrying its retry hint
     */
    List<AssignedTask> poll(List<String> taskTypes, int batchSize, Duration maxWait, int maxConcurrency);

    /**
     * Renews every supplied lease in one call, and reports which ones were lost.
     *
     * <p>Losing a lease is not an error at this level: a worker holding sixteen tasks needs to know
     * which one it lost and that the other fifteen are fine, so this reports rather than throws.
     */
    HeartbeatOutcome heartbeat(Map<UUID, Long> fencingTokensByTaskId);

    /**
     * @throws LeaseLostException if this worker no longer owns the task, which is final rather than
     *         worth retrying
     */
    void complete(UUID taskId, long fencingToken, String output);

    /**
     * @param errorClass the exception type, when the failure came from a throw. Null otherwise
     * @throws LeaseLostException as above
     */
    void fail(UUID taskId, long fencingToken, String message, boolean permanent, String errorClass);

    @Override
    void close();

    /** A task the engine has handed to this worker. */
    record AssignedTask(
            UUID taskId,
            UUID workflowId,
            String name,
            String taskType,
            String input,
            int attempt,
            int maxAttempts,
            long fencingToken) {
    }

    /**
     * @param lost tasks this worker must stop executing and must not report on
     */
    record HeartbeatOutcome(List<UUID> renewed, List<UUID> lost) {

        // Public because a record nested in an interface is implicitly public, and a canonical
        // constructor may not narrow the record's own access.
        public HeartbeatOutcome {
            renewed = List.copyOf(renewed);
            lost = List.copyOf(lost);
        }
    }
}
