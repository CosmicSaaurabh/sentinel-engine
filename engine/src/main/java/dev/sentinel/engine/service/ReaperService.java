package dev.sentinel.engine.service;

import dev.sentinel.engine.domain.OutboxEventType;
import dev.sentinel.engine.domain.RecordedFailure;
import dev.sentinel.engine.infra.ReaperProperties;
import dev.sentinel.engine.repository.CapacityCounterRepository;
import dev.sentinel.engine.repository.OutboxRepository;
import dev.sentinel.engine.repository.TaskFailureRepository;
import dev.sentinel.engine.repository.TaskRepository;
import dev.sentinel.engine.repository.TaskRepository.ExpiredLease;
import dev.sentinel.engine.repository.WorkflowRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Recovers tasks whose worker stopped saying anything.
 *
 * <p>This is the component that makes a lease mean something. Without it, {@code lease_expires_at}
 * is a column nobody reads, and a task claimed by a worker that then dies stays {@code RUNNING}
 * forever with no error anywhere.
 *
 * <h2>Leaderless by construction</h2>
 *
 * <p>Every engine instance runs this concurrently, with no election and no coordination. That falls
 * out of the same property that makes claiming leaderless: the re-queue is a guarded {@code UPDATE},
 * so two instances reaping the same task means one of them affects zero rows, and the batch is
 * selected with {@code SKIP LOCKED}, so concurrent reapers divide the work rather than colliding.
 *
 * <p>There is deliberately no split-brain to design against, because there is nothing for two
 * reapers to disagree about.
 *
 * <h2>Why the exhausted branch runs first</h2>
 *
 * <p>{@link #reapOnce()} fails exhausted tasks before re-queueing the rest, and the order is not
 * cosmetic. Re-queueing first would move a task to {@code PENDING}, where the exhausted-attempts
 * query can no longer see it. It would then be claimed, immediately violate
 * {@code attempt < max_attempts}, and become invisible in the queue forever. Doing the terminal
 * case first means every task is considered by exactly one branch.
 */
@Service
public class ReaperService {

    private static final Logger log = LoggerFactory.getLogger(ReaperService.class);

    private final TaskRepository tasks;
    private final TaskFailureRepository failures;
    private final WorkflowRepository workflows;
    private final OutboxRepository outbox;
    private final CapacityCounterRepository capacity;
    private final ReaperProperties properties;
    private final TransactionTemplate transactionTemplate;

    private final Counter requeued;
    private final Counter failedTerminally;
    private final Counter passErrors;

    private volatile int shardCount;

    public ReaperService(
            TaskRepository tasks,
            TaskFailureRepository failures,
            WorkflowRepository workflows,
            OutboxRepository outbox,
            CapacityCounterRepository capacity,
            ReaperProperties properties,
            TransactionTemplate transactionTemplate,
            MeterRegistry meterRegistry) {

        this.tasks = tasks;
        this.failures = failures;
        this.workflows = workflows;
        this.outbox = outbox;
        this.capacity = capacity;
        this.properties = properties;
        this.transactionTemplate = transactionTemplate;

        this.requeued = Counter.builder("sentinel.reaper.requeued")
                .description("tasks returned to the queue after their lease expired")
                .register(meterRegistry);
        this.failedTerminally = Counter.builder("sentinel.reaper.failed")
                .description("tasks failed because their lease expired with no attempts left")
                .register(meterRegistry);
        this.passErrors = Counter.builder("sentinel.reaper.pass_errors")
                .description("reaper passes that threw; recovery is retried on the next tick")
                .register(meterRegistry);

        io.micrometer.core.instrument.Gauge.builder(
                        "sentinel.reaper.expired_backlog", tasks, TaskRepository::countExpiredLeases)
                .description("leases that have expired and not yet been reaped; growth means the "
                        + "reaper is losing ground and recovery latency is climbing")
                .register(meterRegistry);
    }

    /**
     * One pass, directly callable.
     *
     * <p>Separated from the schedule so that recovery behaviour can be tested by calling it rather
     * than by sleeping until a timer fires.
     */
    public ReapResult reapOnce() {
        ReapResult result = transactionTemplate.execute(status -> {
            // Terminal first. See the class comment for why the order is load-bearing.
            List<ExpiredLease> exhausted = tasks.failExhaustedExpiredLeases(properties.batchSize());
            List<ExpiredLease> recoverable =
                    tasks.requeueExpiredLeases(properties.batchSize(), properties.requeueDelay());

            recordFailures(exhausted, recoverable);
            releaseCapacity(exhausted.size() + recoverable.size());
            failWorkflowsOf(exhausted);

            return new ReapResult(recoverable.size(), exhausted.size(),
                    exhausted.size() == properties.batchSize()
                            || recoverable.size() == properties.batchSize());
        });

        ReapResult reaped = result == null ? ReapResult.empty() : result;
        if (reaped.requeued() > 0) {
            requeued.increment(reaped.requeued());
        }
        if (reaped.failed() > 0) {
            failedTerminally.increment(reaped.failed());
        }
        if (!reaped.isEmpty()) {
            log.info("reaper re-queued {} task(s) and failed {} whose attempts were spent",
                    reaped.requeued(), reaped.failed());
        }
        return reaped;
    }

    /**
     * Counts a pass that threw.
     *
     * <p>Called by the scheduler rather than from inside {@link #reapOnce()}, because the failure
     * being counted is "a pass did not complete", which only the caller that wrapped it can see. A
     * persistently failing reaper is otherwise entirely silent: every task it should have recovered
     * simply stays stranded.
     */
    public void recordPassError() {
        passErrors.increment();
    }

    private void recordFailures(List<ExpiredLease> exhausted, List<ExpiredLease> recoverable) {
        List<RecordedFailure> records = new ArrayList<>(exhausted.size() + recoverable.size());
        exhausted.forEach(lease -> records.add(toRecord(lease)));
        recoverable.forEach(lease -> records.add(toRecord(lease)));

        if (!records.isEmpty()) {
            failures.recordAll(records);
            appendOutboxEvents(exhausted, recoverable);
        }
    }

    private static RecordedFailure toRecord(ExpiredLease lease) {
        return RecordedFailure.leaseExpired(
                lease.taskId(), lease.workflowId(), lease.attempt(), lease.ownerWorkerId());
    }

    private void appendOutboxEvents(List<ExpiredLease> exhausted, List<ExpiredLease> recoverable) {
        byWorkflow(recoverable).forEach((workflowId, taskIds) ->
                outbox.appendAll(workflowId, taskIds, OutboxEventType.TASK_LEASE_EXPIRED,
                        "{\"outcome\":\"requeued\"}"));
        byWorkflow(exhausted).forEach((workflowId, taskIds) ->
                outbox.appendAll(workflowId, taskIds, OutboxEventType.TASK_LEASE_EXPIRED,
                        "{\"outcome\":\"failed\"}"));
    }

    private static Map<UUID, List<UUID>> byWorkflow(List<ExpiredLease> leases) {
        return leases.stream().collect(Collectors.groupingBy(
                ExpiredLease::workflowId,
                Collectors.mapping(ExpiredLease::taskId, Collectors.toList())));
    }

    /**
     * A task that failed terminally fails its workflow, exactly as a reported failure does.
     *
     * <p>Anything not yet started is cancelled, because the workflow is doomed and running more of
     * it is wasted work. Running tasks are left alone: their workers may already have caused
     * external side effects, and calling them cancelled would make the record disagree with the
     * world.
     */
    private void failWorkflowsOf(List<ExpiredLease> exhausted) {
        exhausted.stream()
                .map(ExpiredLease::workflowId)
                .distinct()
                .forEach(workflowId -> {
                    if (workflows.markFailed(workflowId)) {
                        List<UUID> cancelled = tasks.cancelNotStarted(workflowId);
                        if (!cancelled.isEmpty()) {
                            outbox.appendAll(workflowId, cancelled, OutboxEventType.TASK_CANCELLED,
                                    "{\"reason\":\"workflow_failed\"}");
                        }
                        outbox.append(workflowId, null, OutboxEventType.WORKFLOW_FAILED,
                                "{\"cause\":\"lease_expired\"}");
                    }
                });
    }

    /**
     * Returns the capacity the reaped tasks were holding.
     *
     * <p>Spread across shards rather than taken from one, because a mass expiry would otherwise
     * drive a single counter row deeply negative and make the aggregate briefly wrong in a way that
     * looks like a bug rather than like drift.
     */
    private void releaseCapacity(int count) {
        if (count == 0) {
            return;
        }
        int shards = shardCount();
        for (int i = 0; i < count; i++) {
            capacity.addInFlight(i % shards, -1);
        }
    }

    private int shardCount() {
        int cached = shardCount;
        if (cached == 0) {
            cached = capacity.shardCount();
            shardCount = cached;
        }
        return cached;
    }

    /**
     * @param sawFullBatch true when a pass filled its batch, meaning there is probably more waiting.
     *        The scheduler uses this to run again immediately rather than waiting out the interval,
     *        so a large backlog drains in bounded chunks instead of at one batch per tick
     */
    public record ReapResult(int requeued, int failed, boolean sawFullBatch) {

        static ReapResult empty() {
            return new ReapResult(0, 0, false);
        }

        public boolean isEmpty() {
            return requeued == 0 && failed == 0;
        }
    }
}
