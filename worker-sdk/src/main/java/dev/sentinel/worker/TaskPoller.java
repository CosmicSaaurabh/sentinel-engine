package dev.sentinel.worker;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The loop that asks the engine for work and hands it to {@link ActivityRunner}.
 *
 * <h2>It never runs handler code</h2>
 *
 * <p>If this thread executed a handler, one slow task would stop the worker claiming anything else,
 * and a handler that blocked forever would wedge the worker permanently while it still looked
 * healthy from outside. The poller claims and hands off; that is all it does.
 *
 * <h2>Capacity is asked for, not queued into</h2>
 *
 * <p>Each iteration blocks until at least one permit is free, then takes every other free permit
 * and asks the engine for exactly that many tasks. Anything the engine does not give back is
 * returned immediately.
 *
 * <p>The alternative, claiming eagerly and letting tasks wait for a thread, is what the SDK
 * deliberately refuses to do: a waiting task is already claimed and already holds a lease it is
 * making no progress on.
 *
 * <p>Blocking on a permit rather than polling for one also means an entirely busy worker makes no
 * requests at all, which is the correct behaviour and costs nothing.
 *
 * <h2>Three answers, three reactions</h2>
 *
 * <ul>
 *   <li><strong>Tasks.</strong> Dispatch them and immediately ask again.</li>
 *   <li><strong>Empty.</strong> Ask again immediately. The engine already held the request open for
 *       the whole wait, so sleeping now would be waiting twice for the same thing.</li>
 *   <li><strong>An error.</strong> Back off. Transport failures use exponential backoff with full
 *       jitter; a shed request honours the engine's own retry hint.</li>
 * </ul>
 */
final class TaskPoller implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(TaskPoller.class);

    private static final Duration BACKOFF_BASE = Duration.ofMillis(100);
    private static final Duration BACKOFF_MAX = Duration.ofSeconds(30);

    private final EngineClient client;
    private final HandlerRegistry registry;
    private final ActivityRunner runner;
    private final Semaphore permits;
    private final WorkerOptions options;
    private final AtomicBoolean running;
    private final BackoffPolicy backoff = new BackoffPolicy(BACKOFF_BASE, BACKOFF_MAX);

    TaskPoller(
            EngineClient client,
            HandlerRegistry registry,
            ActivityRunner runner,
            Semaphore permits,
            WorkerOptions options,
            AtomicBoolean running) {

        this.client = client;
        this.registry = registry;
        this.runner = runner;
        this.permits = permits;
        this.options = options;
        this.running = running;
    }

    @Override
    public void run() {
        List<String> taskTypes = registry.registeredTypes();
        log.info("worker {} polling for task types {}", options.workerId(), taskTypes);

        while (running.get()) {
            // Permits this thread is holding and has not handed to a task. Decremented the moment
            // a dispatch succeeds, so the cleanup below is correct even if a later dispatch throws.
            int held = 0;
            try {
                // Blocks until the worker has spare capacity. A fully busy worker makes no
                // requests at all, which is exactly right.
                permits.acquire();
                held = 1 + permits.drainPermits();

                int batchSize = Math.min(held, options.maxBatchSize());
                if (held > batchSize) {
                    permits.release(held - batchSize);
                    held = batchSize;
                }

                for (EngineClient.AssignedTask task : pollOnce(taskTypes, batchSize)) {
                    runner.dispatch(task);
                    held--;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (RuntimeException e) {
                // Nothing may escape this loop. A poller thread that dies leaves a worker that is
                // running, connected, and permanently idle: the worst kind of failure, because
                // every external signal says it is healthy.
                log.error("unexpected error in the poll loop; backing off and continuing", e);
                if (!sleep(backoff.nextDelay())) {
                    break;
                }
            } finally {
                // Whatever happened, permits that did not become running tasks go back. Releasing
                // too few shrinks the worker silently; releasing too many lets it exceed its own
                // concurrency limit. Both are invisible without this being exactly right.
                if (held > 0) {
                    permits.release(held);
                }
            }
        }
        log.info("worker {} stopped polling", options.workerId());
    }

    /**
     * One request, with the backoff decisions that belong to it.
     *
     * @return the tasks to dispatch, empty if there were none or if the call failed
     */
    private List<EngineClient.AssignedTask> pollOnce(List<String> taskTypes, int batchSize) {
        try {
            List<EngineClient.AssignedTask> tasks =
                    client.poll(taskTypes, batchSize, options.pollWait(), options.concurrency());
            backoff.reset();
            if (tasks.isEmpty()) {
                // Ordinary. The engine held the request open for the whole wait, so it has already
                // done the sleeping on our behalf; sleeping again would be waiting twice.
                return List.of();
            }
            return tasks;
        } catch (EngineOverloadedException e) {
            // The engine told us when to come back, and jittered it already. Honouring the hint
            // instead of retrying immediately is the difference between backing off and being part
            // of a stampede.
            log.warn("engine is shedding load, waiting {}ms", e.retryAfter().toMillis());
            sleep(e.retryAfter());
            return List.of();
        } catch (EngineUnavailableException e) {
            Duration delay = backoff.nextDelay();
            log.warn("engine unavailable (attempt {}), retrying in {}ms: {}",
                    backoff.consecutiveFailures(), delay.toMillis(), e.getMessage());
            sleep(delay);
            return List.of();
        }
    }

    /** @return false if interrupted, meaning the loop should stop */
    private boolean sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
