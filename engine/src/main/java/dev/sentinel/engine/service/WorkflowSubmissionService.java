package dev.sentinel.engine.service;

import dev.sentinel.engine.domain.DagValidator;
import dev.sentinel.engine.domain.OutboxEventType;
import dev.sentinel.engine.domain.TaskEdge;
import dev.sentinel.engine.domain.TaskStatus;
import dev.sentinel.engine.domain.ValidatedDag;
import dev.sentinel.engine.domain.Workflow;
import dev.sentinel.engine.domain.WorkflowDefinition;
import dev.sentinel.engine.repository.OutboxRepository;
import dev.sentinel.engine.repository.TaskDependencyRepository;
import dev.sentinel.engine.repository.TaskRepository;
import dev.sentinel.engine.repository.WorkflowRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Accepts a workflow definition and turns it into durable, schedulable state.
 *
 * <h2>Order of operations, and why</h2>
 *
 * <p>Validation happens entirely outside the transaction. A malformed DAG is by far the most likely
 * kind of bad submission, and rejecting it should not involve opening a transaction at all.
 *
 * <p>Everything that does touch the database happens in exactly one transaction: the workflow row,
 * every task row, every dependency edge, and the history events. There is no intermediate state in
 * which a workflow exists with only some of its tasks. That matters more than it might seem: a
 * half-written DAG would have tasks whose dependency counters refer to edges that were never
 * inserted, so the scheduler would either run them too early or never run them at all.
 */
@Service
public class WorkflowSubmissionService {

    private static final Logger log = LoggerFactory.getLogger(WorkflowSubmissionService.class);

    private final DagValidator validator;
    private final AdmissionController admissionController;
    private final WorkflowRepository workflows;
    private final TaskRepository tasks;
    private final TaskDependencyRepository dependencies;
    private final OutboxRepository outbox;
    private final TransactionTemplate transactionTemplate;
    private final Counter submitted;
    private final Counter deduplicated;

    public WorkflowSubmissionService(
            DagValidator validator,
            AdmissionController admissionController,
            WorkflowRepository workflows,
            TaskRepository tasks,
            TaskDependencyRepository dependencies,
            OutboxRepository outbox,
            TransactionTemplate transactionTemplate,
            MeterRegistry meterRegistry) {
        this.validator = validator;
        this.admissionController = admissionController;
        this.workflows = workflows;
        this.tasks = tasks;
        this.dependencies = dependencies;
        this.outbox = outbox;
        this.transactionTemplate = transactionTemplate;
        this.submitted = Counter.builder("sentinel.workflow.submitted")
                .description("workflows accepted and written")
                .register(meterRegistry);
        this.deduplicated = Counter.builder("sentinel.workflow.deduplicated")
                .description("submissions that matched an existing idempotency key")
                .register(meterRegistry);
    }

    /**
     * @throws dev.sentinel.engine.error.InvalidWorkflowException if the DAG is malformed
     * @throws dev.sentinel.engine.error.AdmissionRejectedException if the fleet is saturated
     */
    public SubmissionResult submit(WorkflowDefinition definition) {
        ValidatedDag dag = validator.validate(definition);

        return transactionTemplate.execute(status -> {
            Optional<Workflow> existing = findExistingSubmission(definition.submissionKey());
            if (existing.isPresent()) {
                deduplicated.increment();
                log.debug("submission key {} already produced workflow {}",
                        definition.submissionKey(), existing.get().id());
                return new SubmissionResult(existing.get(), true);
            }

            // Capacity is checked inside the transaction so the decision and the write see the same
            // snapshot. Checking outside would leave a window in which the engine fills up between
            // the check and the insert.
            admissionController.requireCapacity();

            try {
                return new SubmissionResult(write(dag), false);
            } catch (DuplicateKeyException e) {
                // Two concurrent submissions of the same idempotency key. The unique index is what
                // actually prevents the duplicate; this branch just turns the losing writer's error
                // into the same answer the winner got. Checking first and inserting second could
                // never have prevented this on its own, which is why the index exists.
                deduplicated.increment();
                return new SubmissionResult(
                        findExistingSubmission(definition.submissionKey()).orElseThrow(() -> e), true);
            }
        });
    }

    /**
     * @param workflow the execution the caller should refer to from now on, whether it was created
     *        by this call or by an earlier one
     * @param deduplicated true when an existing submission key matched and no new work was created.
     *        The caller gets the same workflow either way; this only says which happened, which is
     *        what a client needs to know when deciding whether its retry actually did anything
     */
    public record SubmissionResult(Workflow workflow, boolean deduplicated) {
    }

    private Optional<Workflow> findExistingSubmission(String submissionKey) {
        return submissionKey == null ? Optional.empty() : workflows.findBySubmissionKey(submissionKey);
    }

    private Workflow write(ValidatedDag dag) {
        Workflow workflow = workflows.insert(dag.workflow());
        Map<String, UUID> taskIdsByName = tasks.insertAll(workflow.id(), dag.tasks());

        List<TaskEdge> edges = dag.edges().stream()
                .map(edge -> new TaskEdge(
                        requireId(taskIdsByName, edge.taskName()),
                        requireId(taskIdsByName, edge.dependsOnName())))
                .toList();
        dependencies.insertAll(workflow.id(), edges);

        outbox.append(workflow.id(), null, OutboxEventType.WORKFLOW_SUBMITTED, submissionPayload(dag));

        // Root tasks are claimable the moment this transaction commits, so their scheduling is part
        // of the same history as the submission itself.
        List<UUID> rootTaskIds = dag.tasks().stream()
                .filter(task -> task.status() == TaskStatus.PENDING)
                .map(task -> requireId(taskIdsByName, task.name()))
                .toList();
        outbox.appendAll(workflow.id(), rootTaskIds, OutboxEventType.TASK_SCHEDULED, "{}");

        submitted.increment();
        log.info("workflow {} submitted with {} task(s), {} runnable immediately",
                workflow.id(), dag.tasks().size(), rootTaskIds.size());
        return workflow;
    }

    private static UUID requireId(Map<String, UUID> taskIdsByName, String name) {
        UUID id = taskIdsByName.get(name);
        if (id == null) {
            // Unreachable if validation did its job. Kept as a loud failure rather than a silent
            // null, because a missing id here would corrupt the dependency graph.
            throw new IllegalStateException("no id was generated for task '" + name + "'");
        }
        return id;
    }

    private static String submissionPayload(ValidatedDag dag) {
        return "{\"taskCount\":%d,\"edgeCount\":%d}".formatted(dag.tasks().size(), dag.edges().size());
    }
}
