package dev.sentinel.engine.service;

import dev.sentinel.engine.domain.ClaimedTask;
import dev.sentinel.engine.domain.OutboxEventType;
import dev.sentinel.engine.infra.ClaimProperties;
import dev.sentinel.engine.repository.CapacityCounterRepository;
import dev.sentinel.engine.repository.OutboxRepository;
import dev.sentinel.engine.repository.TaskRepository;
import dev.sentinel.engine.repository.WorkflowRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Hands runnable tasks to workers.
 *
 * <p>This is the hottest path in the engine and the one place where a mistake produces the failure
 * the whole system exists to prevent: the same task running on two machines at once. Three
 * properties are what make that impossible, and each is worth stating explicitly because none of
 * them is visible from the call site.
 *
 * <p><strong>Selection and ownership are one statement.</strong> The claim never reads candidates
 * and then writes ownership, so there is no window between choosing a task and owning it for a
 * second claimer to slip into.
 *
 * <p><strong>The transaction is deliberately tiny.</strong> Five statements, no application logic
 * between them, and absolutely no task execution. A claim transaction that stayed open while work
 * ran would hold row locks for the duration of the work, block {@code VACUUM} from cleaning the
 * highest-churn table in the system, and turn every slow task into a queue-wide stall.
 *
 * <p><strong>An empty result is ordinary.</strong> {@code SKIP LOCKED} with a {@code LIMIT} can
 * legitimately return fewer tasks than asked for, or none, when other claimers hold the rows it
 * walked past. Callers must back off rather than immediately retrying, or an idle fleet turns into
 * a self-inflicted load test against Postgres.
 *
 * <h2>Why this is not a leader</h2>
 *
 * <p>Any engine instance can run this concurrently with any other, with no election and no shared
 * memory between them, because the guarded update is the only arbitration needed. That is what
 * makes the engine horizontally scalable without a coordination protocol.
 */
@Service
public class TaskClaimService {

    private static final Logger log = LoggerFactory.getLogger(TaskClaimService.class);

    private final TaskRepository tasks;
    private final WorkflowRepository workflows;
    private final OutboxRepository outbox;
    private final CapacityCounterRepository capacity;
    private final ClaimProperties properties;
    private final TransactionTemplate transactionTemplate;

    private final Timer claimTimer;
    private final Counter claimedTasks;
    private final Counter emptyPolls;

    /**
     * Cached because the shard count only changes by migration, and re-reading it would add a round
     * trip to every claim to learn something that cannot have changed.
     */
    private volatile int shardCount;

    public TaskClaimService(
            TaskRepository tasks,
            WorkflowRepository workflows,
            OutboxRepository outbox,
            CapacityCounterRepository capacity,
            ClaimProperties properties,
            TransactionTemplate transactionTemplate,
            MeterRegistry meterRegistry) {
        this.tasks = tasks;
        this.workflows = workflows;
        this.outbox = outbox;
        this.capacity = capacity;
        this.properties = properties;
        this.transactionTemplate = transactionTemplate;
        this.claimTimer = Timer.builder("sentinel.claim.duration")
                .description("time to claim a batch, including commit")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meterRegistry);
        this.claimedTasks = Counter.builder("sentinel.claim.tasks")
                .description("tasks handed to workers")
                .register(meterRegistry);
        this.emptyPolls = Counter.builder("sentinel.claim.empty_polls")
                .description("polls that found no runnable work, expected to dominate an idle fleet")
                .register(meterRegistry);
    }

    /**
     * Claims up to the requested number of tasks for one worker.
     *
     * @return the claimed tasks, possibly empty, never null. Each carries the fencing token the
     *         worker must echo on every later call about that task
     */
    public List<ClaimedTask> claim(ClaimRequest request) {
        if (request.taskTypes().isEmpty()) {
            emptyPolls.increment();
            return List.of();
        }

        int batchSize = properties.effectiveBatchSize(request.batchSize());
        long startedAt = System.nanoTime();

        List<ClaimedTask> claimed = transactionTemplate.execute(status -> claimInTransaction(request, batchSize));

        // Elapsed time, deliberately from a monotonic JVM source rather than the database clock.
        // The database-clock rule exists for values that are compared across processes, such as
        // lease deadlines; a local duration is neither shared nor compared, and nanoTime cannot
        // jump backwards the way a wall clock can.
        claimTimer.record(System.nanoTime() - startedAt, java.util.concurrent.TimeUnit.NANOSECONDS);

        List<ClaimedTask> result = claimed == null ? List.of() : claimed;
        if (result.isEmpty()) {
            emptyPolls.increment();
        } else {
            claimedTasks.increment(result.size());
            log.debug("worker {} claimed {} task(s)", request.workerId(), result.size());
        }
        return result;
    }

    /**
     * The transaction body. Every statement here is either the claim itself or bookkeeping that
     * must be atomic with it, and each is one statement per poll rather than one per task.
     */
    private List<ClaimedTask> claimInTransaction(ClaimRequest request, int batchSize) {
        List<ClaimedTask> claimed =
                tasks.claimBatch(request.workerId(), request.taskTypes(), batchSize, properties.leaseDuration());

        if (claimed.isEmpty()) {
            return List.of();
        }

        List<UUID> workflowIds = claimed.stream().map(ClaimedTask::workflowId).distinct().toList();
        workflows.markStartedIfUnset(workflowIds);

        if (properties.recordClaimEvents()) {
            Map<UUID, List<UUID>> taskIdsByWorkflow = claimed.stream()
                    .collect(Collectors.groupingBy(
                            ClaimedTask::workflowId,
                            Collectors.mapping(ClaimedTask::id, Collectors.toList())));
            taskIdsByWorkflow.forEach((workflowId, taskIds) -> outbox.appendAll(
                    workflowId, taskIds, OutboxEventType.TASK_CLAIMED, claimPayload(request.workerId())));
        }

        // Admission control's view of load. Deliberately inside the transaction, so a rollback
        // cannot leave the counter believing work is in flight that never started.
        capacity.addInFlight(capacity.shardFor(request.workerId(), shardCount()), claimed.size());

        return claimed;
    }

    private int shardCount() {
        int cached = shardCount;
        if (cached == 0) {
            cached = capacity.shardCount();
            shardCount = cached;
        }
        return cached;
    }

    private static String claimPayload(String workerId) {
        return "{\"workerId\":\"" + workerId + "\"}";
    }
}
