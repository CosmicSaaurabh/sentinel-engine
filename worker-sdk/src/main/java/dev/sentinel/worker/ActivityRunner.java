package dev.sentinel.worker;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs handler code and reports the outcome.
 *
 * <h2>The pool has no queue, and that is the point</h2>
 *
 * <p>The executor is created with a {@link SynchronousQueue}, which holds nothing: a task either
 * gets a thread immediately or is rejected. That looks needlessly strict until you notice what a
 * queued task would be.
 *
 * <p>A task reaches this class only after being claimed, and a claimed task holds a lease. Sitting
 * in a queue makes no progress towards renewing anything the engine cares about, so queue depth
 * converts directly into lease expiry, re-queue, and the same work running twice. A bounded queue
 * would not fix that; it would only decide how much duplicate execution to allow.
 *
 * <p>So the worker never claims work it cannot start immediately. {@link TaskPoller} holds a permit
 * for every task it asks for, and this class releases each permit when its task ends. Rejection is
 * therefore unreachable, and {@code AbortPolicy} is chosen precisely so that an accounting bug
 * fails loudly instead of quietly reintroducing the queue.
 */
final class ActivityRunner implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ActivityRunner.class);

    private static final int REPORT_ATTEMPTS = 3;
    private static final Duration REPORT_RETRY_DELAY = Duration.ofMillis(500);

    private final HandlerRegistry registry;
    private final HeartbeatScheduler heartbeats;
    private final EngineClient client;
    private final Semaphore permits;
    private final ThreadPoolExecutor executor;
    private final AtomicInteger running = new AtomicInteger();

    /**
     * Set when the worker gave up waiting for these tasks during shutdown.
     *
     * <p>From that point a handler that eventually finishes must stay silent. Its outcome is no
     * longer the worker's to report: the engine will re-queue the task when the lease lapses, and a
     * late report would either be rejected or, worse, land after another worker has already been
     * given the task.
     */
    private volatile boolean abandoned;

    ActivityRunner(
            HandlerRegistry registry,
            HeartbeatScheduler heartbeats,
            EngineClient client,
            Semaphore permits,
            int concurrency) {

        this.registry = registry;
        this.heartbeats = heartbeats;
        this.client = client;
        this.permits = permits;

        AtomicInteger threadNumber = new AtomicInteger(1);
        this.executor = new ThreadPoolExecutor(
                concurrency, concurrency,
                0L, TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                runnable -> {
                    Thread thread = new Thread(runnable, "sentinel-activity-" + threadNumber.getAndIncrement());
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.AbortPolicy());
        // Every thread is created up front so the first tasks do not pay for thread creation, and
        // so a worker's thread footprint is visible immediately rather than growing under load.
        this.executor.prestartAllCoreThreads();
    }

    /**
     * Starts a task.
     *
     * <p>Permit ownership transfers on success and only on success. The caller holds a permit for
     * this task; once the task is accepted by the pool, this class owns that permit and releases it
     * when the task ends. If dispatch throws, ownership never transferred and the caller still
     * holds it.
     *
     * <p>Stating that rule explicitly matters: an earlier version released the permit here as well
     * as in the caller's cleanup, which would have handed the worker one extra permit every time
     * dispatch failed and quietly let it run beyond its configured concurrency.
     */
    void dispatch(EngineClient.AssignedTask task) {
        heartbeats.track(task.taskId(), task.fencingToken());
        try {
            executor.execute(() -> run(task));
        } catch (RejectedExecutionException e) {
            // Unreachable unless permit accounting is broken. Undo only what this method did; the
            // permit is still the caller's to release.
            heartbeats.release(task.taskId());
            throw new IllegalStateException(
                    "activity pool rejected task " + task.taskId() + "; permit accounting is wrong", e);
        }
    }

    private void run(EngineClient.AssignedTask task) {
        running.incrementAndGet();
        try {
            TaskResult result = invokeHandler(task);
            report(task, result);
        } finally {
            running.decrementAndGet();
            heartbeats.release(task.taskId());
            // Released in a finally so that no failure path can leak capacity. A leaked permit
            // silently shrinks the worker: eight concurrent tasks become seven, then six, with
            // nothing in the logs to say so.
            permits.release();
        }
    }

    /**
     * Calls user code, and converts anything it does into a result.
     *
     * <p>This is the only place in the SDK allowed to catch {@link Throwable}. An uncaught
     * exception must never kill an activity thread: a thread that dies quietly takes its permit
     * with it, and a worker configured for eight tasks slowly becomes one that runs seven, then
     * six, with no error anywhere pointing at the cause.
     */
    private TaskResult invokeHandler(EngineClient.AssignedTask task) {
        ActivityHandler handler = registry.handlerFor(task.taskType());
        if (handler == null) {
            // The engine only offers types this worker advertised, so this means the registry and
            // the advertisement disagree. Permanent, because retrying reaches the same worker pool.
            return TaskResult.permanentlyFailed("no handler registered for task type " + task.taskType());
        }

        ActivityContext context = new ActivityContext(
                task.taskId(),
                task.workflowId(),
                task.name(),
                task.taskType(),
                task.input(),
                task.attempt(),
                task.maxAttempts(),
                () -> heartbeats.isLost(task.taskId()));

        try {
            TaskResult result = handler.handle(context);
            if (result == null) {
                // Far more likely to be a forgotten return path than a deliberate claim of success.
                return TaskResult.failed("handler returned null for task type " + task.taskType());
            }
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return TaskResult.failed("handler was interrupted");
        } catch (Exception e) {
            log.warn("handler for {} threw on task {}", task.taskType(), task.taskId(), e);
            return TaskResult.failed(e.getClass().getSimpleName() + ": " + e.getMessage());
        } catch (Error e) {
            // Reported so the task does not silently vanish, then rethrown: the process may be
            // genuinely dying, and swallowing that would be worse than the task failing.
            log.error("handler for {} hit an error on task {}", task.taskType(), task.taskId(), e);
            reportQuietly(task, TaskResult.failed(e.getClass().getSimpleName() + ": " + e.getMessage()));
            throw e;
        }
    }

    private void report(EngineClient.AssignedTask task, TaskResult result) {
        if (abandoned) {
            log.warn("not reporting task {}: the worker stopped waiting for it during shutdown, so "
                    + "its lease is lapsing and the engine will re-queue it", task.taskId());
            return;
        }
        if (heartbeats.isLost(task.taskId())) {
            // Reporting would be rejected anyway, and the task is already running elsewhere.
            // Saying so plainly is more useful than a FAILED_PRECONDITION in the logs.
            log.warn("not reporting task {}: its lease was lost while the handler was running",
                    task.taskId());
            return;
        }

        for (int attempt = 1; attempt <= REPORT_ATTEMPTS; attempt++) {
            try {
                send(task, result);
                return;
            } catch (LeaseLostException e) {
                log.warn("report for task {} was rejected: {}", task.taskId(), e.getMessage());
                return;
            } catch (EngineUnavailableException | EngineOverloadedException e) {
                if (attempt == REPORT_ATTEMPTS) {
                    // Giving up is safe rather than lossy: the lease expires and the engine
                    // re-queues the task. The cost is one lease duration of latency and a repeated
                    // execution, which is exactly what at-least-once means and what the task's
                    // idempotency key exists to absorb.
                    log.warn("could not report task {} after {} attempts; letting its lease expire",
                            task.taskId(), REPORT_ATTEMPTS, e);
                    return;
                }
                sleepBeforeReportRetry(attempt);
            }
        }
    }

    private void reportQuietly(EngineClient.AssignedTask task, TaskResult result) {
        try {
            send(task, result);
        } catch (RuntimeException ignored) {
            log.warn("could not report task {} while handling an Error", task.taskId());
        }
    }

    private void send(EngineClient.AssignedTask task, TaskResult result) {
        switch (result) {
            case TaskResult.Completed completed ->
                    client.complete(task.taskId(), task.fencingToken(), completed.output());
            case TaskResult.Failed failed ->
                    client.fail(task.taskId(), task.fencingToken(), failed.message(), failed.permanent());
        }
    }

    private void sleepBeforeReportRetry(int attempt) {
        try {
            Thread.sleep(REPORT_RETRY_DELAY.toMillis() * attempt);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    int runningTasks() {
        return running.get();
    }

    /**
     * Waits for in-flight handlers to finish.
     *
     * <p>{@code shutdown()} rather than {@code shutdownNow()}: it stops the pool accepting new work
     * but leaves running handlers alone.
     *
     * @return true if everything finished within the timeout
     */
    boolean awaitDrain(Duration timeout) throws InterruptedException {
        executor.shutdown();
        return executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Stops reporting on whatever is still running. Called when the drain timeout expires. */
    void abandonRemaining() {
        abandoned = true;
    }

    /**
     * Releases the pool without interrupting handlers.
     *
     * <p>Deliberately not {@code shutdownNow()}. Interrupting arbitrary user code halfway through a
     * side effect is not obviously safer than letting it finish, and an interrupted handler throws,
     * which the wrapper would classify as a failure and report. That is precisely the outcome
     * shutdown is designed to avoid: telling the engine a still-executing task is available starts
     * a second copy alongside the first.
     *
     * <p>The activity threads are daemon threads, so anything still running cannot keep the JVM
     * alive. A handler that never returns is the user's responsibility, as its javadoc says.
     */
    @Override
    public void close() {
        executor.shutdown();
    }

    /** Exposed for tests that need to observe the pool rather than infer its state. */
    ExecutorService executor() {
        return executor;
    }
}
