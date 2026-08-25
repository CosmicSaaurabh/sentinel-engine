package dev.sentinel.worker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A stand-in for the engine.
 *
 * <p>The SDK's genuinely tricky behaviour is its threading, its permit accounting, its backoff and
 * its shutdown ordering. Exercising those through a real gRPC server means a server, a port, and
 * sleeping until things happen; behind {@link EngineClient} they can be driven deterministically in
 * milliseconds, including failure paths that are awkward to provoke over a network.
 *
 * <p>{@link #poll} blocks on the queue rather than returning immediately, which is what the real
 * engine's long poll does. Without that, a test's poller thread would spin hot and the timings
 * would say nothing useful.
 */
final class FakeEngineClient implements EngineClient {

    private final BlockingQueue<AssignedTask> available = new LinkedBlockingQueue<>();

    final List<Completion> completions = Collections.synchronizedList(new ArrayList<>());
    final List<Failure> failures = Collections.synchronizedList(new ArrayList<>());
    final List<Integer> requestedBatchSizes = Collections.synchronizedList(new ArrayList<>());

    final AtomicInteger pollCount = new AtomicInteger();
    final AtomicInteger heartbeatCount = new AtomicInteger();

    /** Leases the engine has taken away; reported on the next heartbeat. */
    final Set<UUID> revokedLeases = ConcurrentHashMap.newKeySet();

    /** When set, every poll throws this instead of answering. */
    final AtomicReference<RuntimeException> pollError = new AtomicReference<>();

    /** When set, the next report throws this once, then clears. */
    final AtomicReference<RuntimeException> reportErrorOnce = new AtomicReference<>();

    private volatile boolean closed;

    // ------------------------------------------------------------------
    // Test controls
    // ------------------------------------------------------------------

    AssignedTask offer(String taskType) {
        return offer(taskType, "{}", 1, 10);
    }

    AssignedTask offer(String taskType, String input, int attempt, int maxAttempts) {
        AssignedTask task = new AssignedTask(
                UUID.randomUUID(), UUID.randomUUID(), "task-" + available.size(),
                taskType, input, attempt, maxAttempts, 1L,
                // A plausible W3C traceparent, so the log-context parsing is exercised rather than
                // always taking the null path.
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        available.add(task);
        return task;
    }

    boolean isClosed() {
        return closed;
    }

    // ------------------------------------------------------------------
    // EngineClient
    // ------------------------------------------------------------------

    @Override
    public List<AssignedTask> poll(List<String> taskTypes, int batchSize, Duration maxWait, int maxConcurrency) {
        pollCount.incrementAndGet();
        requestedBatchSizes.add(batchSize);

        RuntimeException error = pollError.get();
        if (error != null) {
            throw error;
        }

        try {
            AssignedTask first = available.poll(maxWait.toMillis(), TimeUnit.MILLISECONDS);
            if (first == null) {
                return List.of();
            }
            List<AssignedTask> batch = new ArrayList<>();
            batch.add(first);
            available.drainTo(batch, batchSize - 1);
            return List.copyOf(batch);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return List.of();
        }
    }

    @Override
    public HeartbeatOutcome heartbeat(Map<UUID, Long> fencingTokensByTaskId) {
        heartbeatCount.incrementAndGet();
        List<UUID> renewed = new ArrayList<>();
        List<UUID> lost = new ArrayList<>();
        fencingTokensByTaskId.keySet().forEach(taskId -> {
            if (revokedLeases.contains(taskId)) {
                lost.add(taskId);
            } else {
                renewed.add(taskId);
            }
        });
        return new HeartbeatOutcome(renewed, lost);
    }

    @Override
    public void complete(UUID taskId, long fencingToken, String output) {
        throwReportErrorIfArmed();
        if (revokedLeases.contains(taskId)) {
            throw new LeaseLostException(taskId, "lease lost");
        }
        completions.add(new Completion(taskId, output));
    }

    @Override
    public void fail(UUID taskId, long fencingToken, String message, boolean permanent, String errorClass) {
        throwReportErrorIfArmed();
        if (revokedLeases.contains(taskId)) {
            throw new LeaseLostException(taskId, "lease lost");
        }
        failures.add(new Failure(taskId, message, permanent, errorClass));
    }

    @Override
    public void close() {
        closed = true;
    }

    private void throwReportErrorIfArmed() {
        RuntimeException error = reportErrorOnce.getAndSet(null);
        if (error != null) {
            throw error;
        }
    }

    record Completion(UUID taskId, String output) {
    }

    record Failure(UUID taskId, String message, boolean permanent, String errorClass) {
    }
}
