package dev.sentinel.worker;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Sentinel worker: the thing a client starts to run task handlers.
 *
 * <p>This is the whole public surface. Register a handler per task type, start it, and close it
 * when the application shuts down.
 *
 * <pre>{@code
 * try (SentinelWorker worker = SentinelWorker.builder()
 *         .engineTarget("localhost:9090")
 *         .concurrency(8)
 *         .register("billing.charge", context -> {
 *             chargeCard(context.input(), context.taskId());   // taskId as the idempotency key
 *             return TaskResult.completed("{\"charged\":true}");
 *         })
 *         .build()) {
 *
 *     worker.start();
 *     awaitShutdownSignal();
 * }
 * }</pre>
 *
 * <h2>What it starts</h2>
 *
 * <table border="1">
 *   <caption>Threads owned by one worker</caption>
 *   <tr><th>Thread</th><th>Count</th><th>Does</th></tr>
 *   <tr><td>{@code sentinel-poller}</td><td>1</td><td>Asks for work, hands it off. Never runs handlers</td></tr>
 *   <tr><td>{@code sentinel-activity-N}</td><td>{@code concurrency}</td><td>Runs handler code</td></tr>
 *   <tr><td>{@code sentinel-heartbeat}</td><td>1</td><td>Renews every in-flight lease</td></tr>
 * </table>
 *
 * <p>All are daemon threads, so a forgotten {@link #close()} cannot keep a JVM alive. That is a
 * backstop, not the mechanism: closing properly is what drains work cleanly.
 *
 * <h2>Shutdown, and why it is ordered the way it is</h2>
 *
 * <p>{@link #close()} follows a fixed sequence:
 *
 * <ol>
 *   <li>Stop polling, so no new work enters the worker.</li>
 *   <li><strong>Keep heartbeating.</strong> Tasks still running still hold leases and must keep
 *       them. Stopping heartbeats first would expire the leases of tasks that were seconds from
 *       finishing successfully.</li>
 *   <li>Wait for in-flight handlers, up to {@code drainTimeout}.</li>
 *   <li>Abandon anything still running, letting its lease expire.</li>
 *   <li>Stop heartbeating, stop the pool, close the channel.</li>
 * </ol>
 *
 * <p>Step 4 is the one that looks wrong and is not. Reporting undrained tasks as failed so they
 * re-queue immediately sounds more helpful, but those tasks are <em>still executing</em>: telling
 * the engine they are available starts a second copy concurrently rather than afterwards.
 * At-least-once quietly becomes at-least-twice-simultaneously, which is far harder for a client's
 * idempotency key to absorb than one lease duration of delay.
 */
public final class SentinelWorker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SentinelWorker.class);

    private final WorkerOptions options;
    private final HandlerRegistry registry;
    private final EngineClient client;
    private final Semaphore permits;
    private final HeartbeatScheduler heartbeats;
    private final ActivityRunner runner;
    private final AtomicBoolean running = new AtomicBoolean();

    private Thread pollerThread;

    private SentinelWorker(WorkerOptions options, HandlerRegistry registry, EngineClient client) {
        this.options = options;
        this.registry = registry;
        this.client = client;
        this.permits = new Semaphore(options.concurrency());
        this.heartbeats = new HeartbeatScheduler(client, options.heartbeatInterval());
        this.runner = new ActivityRunner(registry, heartbeats, client, permits, options.concurrency());
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Starts polling. Returns as soon as the threads are up; work happens in the background. */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("worker " + options.workerId() + " is already running");
        }

        heartbeats.start();
        pollerThread = new Thread(
                new TaskPoller(client, registry, runner, permits, options, running), "sentinel-poller");
        pollerThread.setDaemon(true);
        pollerThread.start();

        log.info("worker {} started with concurrency {} for task types {}",
                options.workerId(), options.concurrency(), registry.registeredTypes());
    }

    public boolean isRunning() {
        return running.get();
    }

    /** Tasks currently executing on this worker. */
    public int runningTasks() {
        return runner.runningTasks();
    }

    public String workerId() {
        return options.workerId();
    }

    public List<String> registeredTaskTypes() {
        return registry.registeredTypes();
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        log.info("worker {} shutting down, draining up to {}", options.workerId(), options.drainTimeout());

        // 1. Stop polling. The poller may be parked on a permit or inside a long poll, so it is
        //    interrupted rather than merely asked to stop.
        if (pollerThread != null) {
            pollerThread.interrupt();
            joinQuietly(pollerThread, Duration.ofSeconds(5));
        }

        // 2 and 3. Heartbeats deliberately keep running while in-flight work drains.
        boolean drained = false;
        try {
            drained = runner.awaitDrain(options.drainTimeout());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 4. Anything left is abandoned, not reported and not interrupted. See the class comment.
        if (!drained) {
            runner.abandonRemaining();
            log.warn("worker {} still had {} task(s) running after {}; abandoning them so their "
                            + "leases expire, rather than reporting work that is still executing",
                    options.workerId(), runner.runningTasks(), options.drainTimeout());
        }

        // 5. Now the leases may lapse.
        heartbeats.close();
        runner.close();
        client.close();
        log.info("worker {} stopped", options.workerId());
    }

    private static void joinQuietly(Thread thread, Duration timeout) {
        try {
            thread.join(timeout.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Fluent construction. Every option has a working default except the engine address. */
    public static final class Builder {

        private final HandlerRegistry registry = new HandlerRegistry();
        private String engineTarget;
        private String workerId;
        private int concurrency = WorkerOptions.DEFAULT_CONCURRENCY;
        private Duration pollWait = WorkerOptions.DEFAULT_POLL_WAIT;
        private Duration heartbeatInterval = WorkerOptions.DEFAULT_HEARTBEAT_INTERVAL;
        private Duration drainTimeout = WorkerOptions.DEFAULT_DRAIN_TIMEOUT;
        private int maxBatchSize = WorkerOptions.DEFAULT_MAX_BATCH_SIZE;
        private ManagedChannel channel;
        private EngineClient engineClient;

        private Builder() {
        }

        /** Host and port of the engine, for example {@code localhost:9090}. */
        public Builder engineTarget(String engineTarget) {
            this.engineTarget = engineTarget;
            return this;
        }

        /**
         * Stable identity for this worker process.
         *
         * <p>Worth setting to something meaningful and stable in production, such as the pod name.
         * A generated id changes on every restart, which makes the engine's worker registry read
         * as a stream of strangers rather than a fleet.
         */
        public Builder workerId(String workerId) {
            this.workerId = workerId;
            return this;
        }

        /** How many tasks run at once. Also the size of the activity thread pool. */
        public Builder concurrency(int concurrency) {
            this.concurrency = concurrency;
            return this;
        }

        public Builder pollWait(Duration pollWait) {
            this.pollWait = pollWait;
            return this;
        }

        /** Keep this comfortably below the engine's lease duration; a third of it is the default. */
        public Builder heartbeatInterval(Duration heartbeatInterval) {
            this.heartbeatInterval = heartbeatInterval;
            return this;
        }

        public Builder drainTimeout(Duration drainTimeout) {
            this.drainTimeout = drainTimeout;
            return this;
        }

        public Builder maxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        /** Registers the handler for one task type. At least one is required. */
        public Builder register(String taskType, ActivityHandler handler) {
            registry.register(taskType, handler);
            return this;
        }

        /**
         * Uses a channel the caller built, instead of one built from {@link #engineTarget}.
         *
         * <p>For anything the plain target string cannot express: TLS credentials, client
         * interceptors, a custom name resolver or load-balancing policy, or an in-process transport
         * for tests. The worker takes ownership and closes it on {@link SentinelWorker#close()}.
         */
        public Builder channel(ManagedChannel channel) {
            this.channel = channel;
            return this;
        }

        /** Injects a client, for the SDK's own tests. Applications use {@link #engineTarget}. */
        Builder engineClient(EngineClient engineClient) {
            this.engineClient = engineClient;
            return this;
        }

        public SentinelWorker build() {
            if (registry.isEmpty()) {
                // Rejected at startup rather than allowed to run. A worker with no handlers would
                // advertise no task types, claim nothing, and look perfectly healthy while doing
                // nothing at all, which is a miserable thing to debug.
                throw new IllegalStateException(
                        "a worker must register at least one handler, or it will never claim work");
            }

            String resolvedWorkerId = workerId != null ? workerId : "worker-" + UUID.randomUUID();
            EngineClient resolvedClient = engineClient != null
                    ? engineClient
                    : new GrpcEngineClient(openChannel(), resolvedWorkerId);

            WorkerOptions options = new WorkerOptions(
                    engineTarget != null ? engineTarget : "supplied-channel",
                    resolvedWorkerId,
                    concurrency,
                    pollWait,
                    heartbeatInterval,
                    drainTimeout,
                    maxBatchSize);

            return new SentinelWorker(options, registry, resolvedClient);
        }

        private ManagedChannel openChannel() {
            if (channel != null) {
                return channel;
            }
            if (engineTarget == null || engineTarget.isBlank()) {
                throw new IllegalStateException(
                        "either engineTarget (for example localhost:9090) or channel is required");
            }
            // Plaintext because the MVP is scoped to a trusted network. Transport security is an
            // explicit open item in HLD-002 and must be settled before this runs anywhere else.
            return ManagedChannelBuilder.forTarget(engineTarget).usePlaintext().build();
        }
    }
}
