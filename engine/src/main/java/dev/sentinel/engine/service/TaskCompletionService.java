package dev.sentinel.engine.service;

import dev.sentinel.engine.domain.FailureKind;
import dev.sentinel.engine.domain.OutboxEventType;
import dev.sentinel.engine.domain.RetryPolicy;
import dev.sentinel.engine.domain.Task;
import dev.sentinel.engine.domain.TaskFailure;
import dev.sentinel.engine.domain.TaskStatus;
import dev.sentinel.engine.error.EntityNotFoundException;
import dev.sentinel.engine.error.StaleFencingTokenException;
import dev.sentinel.engine.infra.ClaimProperties;
import dev.sentinel.engine.repository.CapacityCounterRepository;
import dev.sentinel.engine.repository.OutboxRepository;
import dev.sentinel.engine.repository.TaskDependencyRepository;
import dev.sentinel.engine.repository.TaskRepository;
import dev.sentinel.engine.repository.WorkflowRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Handles what a worker reports back, and everything that has to happen atomically with it.
 *
 * <h2>The unit of work</h2>
 *
 * <p>Recording a completion is never just one update. The task's status changes, its children lose
 * a blocker, some of them become runnable, the workflow may reach its terminal state, the capacity
 * counter drops, and history is written. All of it happens in one transaction, and the reason is
 * the dependency counter: if the parent's completion committed and the children's decrement did
 * not, the counter would permanently over-count and those children would never run. There is no
 * repair process for that, because nothing downstream can tell a stuck counter from a task that is
 * legitimately still waiting.
 *
 * <h2>Ownership</h2>
 *
 * <p>Every write here is fenced. A worker presents the token it was given at claim time, and the
 * guarded update matches only if that token is still current. Zero affected rows is then
 * disambiguated by reading the row back, which separates two very different situations that look
 * identical from the update's point of view: a duplicate report from the rightful owner, and a
 * report from a worker whose lease was taken away.
 */
@Service
public class TaskCompletionService {

    private static final Logger log = LoggerFactory.getLogger(TaskCompletionService.class);

    private final TaskRepository tasks;
    private final TaskDependencyRepository dependencies;
    private final WorkflowRepository workflows;
    private final OutboxRepository outbox;
    private final CapacityCounterRepository capacity;
    private final RetryPolicy retryPolicy;
    private final ClaimProperties claimProperties;
    private final TransactionTemplate transactionTemplate;

    private final Counter completed;
    private final Counter retried;
    private final Counter failed;
    private final Counter fenced;

    private volatile int shardCount;

    public TaskCompletionService(
            TaskRepository tasks,
            TaskDependencyRepository dependencies,
            WorkflowRepository workflows,
            OutboxRepository outbox,
            CapacityCounterRepository capacity,
            RetryPolicy retryPolicy,
            ClaimProperties claimProperties,
            TransactionTemplate transactionTemplate,
            MeterRegistry meterRegistry) {
        this.tasks = tasks;
        this.dependencies = dependencies;
        this.workflows = workflows;
        this.outbox = outbox;
        this.capacity = capacity;
        this.retryPolicy = retryPolicy;
        this.claimProperties = claimProperties;
        this.transactionTemplate = transactionTemplate;
        this.completed = Counter.builder("sentinel.task.completed").register(meterRegistry);
        this.retried = Counter.builder("sentinel.task.retried").register(meterRegistry);
        this.failed = Counter.builder("sentinel.task.failed").register(meterRegistry);
        this.fenced = Counter.builder("sentinel.task.fenced_out")
                .description("reports rejected because the worker had lost its lease")
                .register(meterRegistry);
    }

    // ------------------------------------------------------------------
    // Heartbeat
    // ------------------------------------------------------------------

    /**
     * Extends a lease.
     *
     * <p>Single statement, no transaction wrapper, because there is nothing to make atomic with it.
     * This is the highest-frequency call in the system and it stays as close to one round trip as
     * possible.
     *
     * @throws StaleFencingTokenException if the worker no longer owns the task, which is the
     *         worker's signal to stop working on it
     */
    public void heartbeat(UUID taskId, String workerId, long fencingToken) {
        if (!tasks.renewLease(taskId, workerId, fencingToken, claimProperties.leaseDuration())) {
            fenced.increment();
            throw fencingFailure(taskId, workerId, fencingToken);
        }
    }

    /**
     * Renews every lease a worker holds, and reports per-lease outcomes.
     *
     * <p>Deliberately does not throw when a lease has been lost. A worker running sixteen tasks
     * that has been reaped on one of them still needs to know that the other fifteen are healthy,
     * and failing the whole call would hide that. The worker's contract is to stop executing the
     * ones it is told it has lost, and to keep going with the rest.
     *
     * @param fencingTokensByTaskId what the worker believes it holds
     */
    public HeartbeatResult heartbeatAll(String workerId, Map<UUID, Long> fencingTokensByTaskId) {
        if (fencingTokensByTaskId.isEmpty()) {
            return new HeartbeatResult(List.of(), List.of());
        }

        List<UUID> renewed = tasks.renewLeases(workerId, fencingTokensByTaskId, claimProperties.leaseDuration());
        List<UUID> lost = fencingTokensByTaskId.keySet().stream()
                .filter(taskId -> !renewed.contains(taskId))
                .toList();

        if (!lost.isEmpty()) {
            fenced.increment(lost.size());
            log.warn("worker {} has lost {} lease(s): {}", workerId, lost.size(), lost);
        }
        return new HeartbeatResult(renewed, lost);
    }

    /**
     * @param renewed leases that were extended
     * @param lost leases the worker no longer holds and must stop working on
     */
    public record HeartbeatResult(List<UUID> renewed, List<UUID> lost) {

        public HeartbeatResult {
            renewed = List.copyOf(renewed);
            lost = List.copyOf(lost);
        }
    }

    // ------------------------------------------------------------------
    // Success
    // ------------------------------------------------------------------

    public CompletionOutcome complete(UUID taskId, String workerId, long fencingToken, String output) {
        return transactionTemplate.execute(status -> {
            if (!tasks.markCompleted(taskId, workerId, fencingToken, output)) {
                return classifyRejection(taskId, workerId, fencingToken, TaskStatus.COMPLETED);
            }

            Task task = requireTask(taskId);
            releaseCapacity(workerId);
            outbox.append(task.workflowId(), taskId, OutboxEventType.TASK_COMPLETED, output);

            unblockChildren(task);
            settleWorkflowIfFinished(task.workflowId());

            completed.increment();
            return CompletionOutcome.RECORDED;
        });
    }

    /**
     * Decrements the blocker count of this task's children and schedules any that are now runnable.
     *
     * <p>The children are locked in ascending id order first. That is the deadlock defence: two
     * parents of overlapping children can complete at the same moment on two different engine
     * instances, and without a shared acquisition order one transaction can hold child X waiting for
     * Y while the other holds Y waiting for X. Postgres resolves that by killing one of them, which
     * would turn an ordinary completion into a spurious failure.
     *
     * <p>Note what this does not do: it never asks "are all of this child's parents complete?". That
     * query costs work proportional to the number of parents and runs on every single completion, so
     * a task with a fan-in of hundreds would make every one of its parents' completions expensive.
     * Decrementing a counter is constant work per edge regardless of fan-in.
     */
    private void unblockChildren(Task task) {
        List<UUID> childIds = tasks.lockChildrenOfCompletedTask(task.workflowId(), task.id());
        List<UUID> nowRunnable = tasks.decrementDependenciesAndFlip(childIds);
        if (!nowRunnable.isEmpty()) {
            outbox.appendAll(task.workflowId(), nowRunnable, OutboxEventType.TASK_SCHEDULED, "{}");
            log.debug("task {} completing made {} task(s) runnable", task.id(), nowRunnable.size());
        }
    }

    // ------------------------------------------------------------------
    // Failure
    // ------------------------------------------------------------------

    /**
     * Records a failure, and decides between another attempt and a terminal outcome.
     *
     * <p>The read before the write is safe despite looking like a read-then-act race. The decision
     * it makes only ever leads to a guarded write, which re-checks ownership at write time. If the
     * world changed underneath the read, the write matches nothing and the report is rejected, so a
     * stale read can never produce a wrong write.
     */
    public CompletionOutcome fail(
            UUID taskId, String workerId, long fencingToken, FailureKind kind, String message) {

        TaskFailure failure = new TaskFailure(message, kind);

        return transactionTemplate.execute(status -> {
            Task task = tasks.findById(taskId).orElseThrow(() -> EntityNotFoundException.task(taskId));

            boolean retry = retryPolicy.shouldRetry(task.attempt(), task.maxAttempts(), kind);
            return retry
                    ? scheduleRetry(task, workerId, fencingToken, failure)
                    : failTerminally(task, workerId, fencingToken, failure);
        });
    }

    private CompletionOutcome scheduleRetry(
            Task task, String workerId, long fencingToken, TaskFailure failure) {

        Duration delay = retryPolicy.delayBefore(task.attempt(), failure.kind());
        if (!tasks.scheduleRetry(task.id(), workerId, fencingToken, failure, delay)) {
            return classifyRejection(task.id(), workerId, fencingToken, TaskStatus.PENDING);
        }

        releaseCapacity(workerId);
        outbox.append(task.workflowId(), task.id(), OutboxEventType.TASK_RETRY_SCHEDULED,
                "{\"attempt\":%d,\"delayMillis\":%d}".formatted(task.attempt(), delay.toMillis()));

        retried.increment();
        log.info("task {} failed on attempt {} of {}, retrying in {}ms",
                task.id(), task.attempt(), task.maxAttempts(), delay.toMillis());
        return CompletionOutcome.RECORDED;
    }

    /**
     * Fails a task for good, and propagates that failure through the workflow.
     *
     * <p>Two separate cancellations happen, and keeping them separate is deliberate rather than
     * redundant. The descendants of this task are cancelled because they can never run: their
     * dependency will never be satisfied. Everything else not yet started is cancelled because the
     * workflow as a whole has failed and running more of it would be wasted work. Recording them as
     * two steps means the history says why each task was cancelled, instead of leaving an operator
     * to guess.
     *
     * <p>Tasks already {@code RUNNING} are left alone in both cases. Their workers may already have
     * caused an external side effect, and marking them cancelled would make the recorded history
     * disagree with what actually happened in the world.
     */
    private CompletionOutcome failTerminally(
            Task task, String workerId, long fencingToken, TaskFailure failure) {

        if (!tasks.markFailed(task.id(), workerId, fencingToken, failure)) {
            return classifyRejection(task.id(), workerId, fencingToken, TaskStatus.FAILED);
        }

        releaseCapacity(workerId);
        outbox.append(task.workflowId(), task.id(), OutboxEventType.TASK_FAILED,
                "{\"kind\":\"%s\",\"attempt\":%d}".formatted(failure.kind(), task.attempt()));

        List<UUID> descendants = dependencies.findDescendants(task.workflowId(), task.id());
        recordCancellations(task.workflowId(), tasks.cancelAll(descendants), "ancestor_failed");

        workflows.markFailed(task.workflowId());
        recordCancellations(task.workflowId(), tasks.cancelNotStarted(task.workflowId()), "workflow_failed");
        outbox.append(task.workflowId(), null, OutboxEventType.WORKFLOW_FAILED,
                "{\"failedTask\":\"%s\"}".formatted(task.id()));

        failed.increment();
        log.info("task {} failed terminally after {} attempt(s); workflow {} failed",
                task.id(), task.attempt(), task.workflowId());
        return CompletionOutcome.RECORDED;
    }

    private void recordCancellations(UUID workflowId, List<UUID> taskIds, String reason) {
        if (!taskIds.isEmpty()) {
            outbox.appendAll(workflowId, taskIds, OutboxEventType.TASK_CANCELLED,
                    "{\"reason\":\"%s\"}".formatted(reason));
        }
    }

    // ------------------------------------------------------------------
    // Shared
    // ------------------------------------------------------------------

    private void settleWorkflowIfFinished(UUID workflowId) {
        if (workflows.completeIfAllTasksTerminal(workflowId)) {
            outbox.append(workflowId, null, OutboxEventType.WORKFLOW_COMPLETED, "{}");
            log.info("workflow {} completed", workflowId);
        }
    }

    /**
     * Works out why a guarded update matched nothing.
     *
     * <p>Two possibilities look identical to the update itself and mean opposite things. If the task
     * already holds the outcome this worker is reporting and still carries the token it presented,
     * this is a duplicate report from the rightful owner, which is exactly what an ack lost in
     * transit produces and is not an error. Anything else means the worker lost its lease.
     */
    private CompletionOutcome classifyRejection(
            UUID taskId, String workerId, long fencingToken, TaskStatus expectedStatus) {

        Task task = tasks.findById(taskId).orElseThrow(() -> EntityNotFoundException.task(taskId));

        if (task.status() == expectedStatus && task.fencingToken() == fencingToken) {
            log.debug("duplicate {} report for task {} from worker {}", expectedStatus, taskId, workerId);
            return CompletionOutcome.ALREADY_RECORDED;
        }

        fenced.increment();
        throw new StaleFencingTokenException(taskId, workerId, fencingToken, task.fencingToken());
    }

    private StaleFencingTokenException fencingFailure(UUID taskId, String workerId, long fencingToken) {
        Task task = tasks.findById(taskId).orElseThrow(() -> EntityNotFoundException.task(taskId));
        return new StaleFencingTokenException(taskId, workerId, fencingToken, task.fencingToken());
    }

    private Task requireTask(UUID taskId) {
        return tasks.findById(taskId).orElseThrow(() -> EntityNotFoundException.task(taskId));
    }

    /**
     * Gives a task slot back to admission control.
     *
     * <p>Inside the same transaction as the state change, so a rollback cannot leave the counter
     * believing a slot is free when the task is still running.
     */
    private void releaseCapacity(String workerId) {
        capacity.addInFlight(capacity.shardFor(workerId, shardCount()), -1);
    }

    private int shardCount() {
        int cached = shardCount;
        if (cached == 0) {
            cached = capacity.shardCount();
            shardCount = cached;
        }
        return cached;
    }
}
