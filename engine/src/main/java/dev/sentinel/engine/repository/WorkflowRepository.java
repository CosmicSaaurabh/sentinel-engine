package dev.sentinel.engine.repository;

import dev.sentinel.engine.domain.NewWorkflow;
import dev.sentinel.engine.domain.Workflow;
import dev.sentinel.engine.repository.support.GuardedUpdate;
import dev.sentinel.engine.repository.support.RowMappers;
import dev.sentinel.engine.repository.support.SqlValues;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Persistence for workflow executions.
 *
 * <p>This layer contains SQL and mapping only. It makes no decision about whether a transition
 * should happen; it reports whether the guard matched and leaves the interpretation to the service
 * that owns the unit of work.
 */
@Repository
public class WorkflowRepository {

    private static final String COLUMNS =
            "id, name, status, submission_key, scheduled_at, started_at, finished_at, created_at, updated_at";

    private final JdbcClient jdbcClient;

    public WorkflowRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    public Workflow insert(NewWorkflow workflow) {
        return jdbcClient.sql("""
                INSERT INTO workflows (name, submission_key, scheduled_at)
                VALUES (:name, :submissionKey, coalesce(CAST(:scheduledAt AS timestamptz), now()))
                RETURNING
                """ + COLUMNS)
                .param("name", workflow.name())
                .param("submissionKey", workflow.submissionKey())
                .param("scheduledAt", SqlValues.timestamp(workflow.scheduledAt()))
                .query(RowMappers.WORKFLOW)
                .single();
    }

    public Optional<Workflow> findById(UUID workflowId) {
        return jdbcClient.sql("SELECT " + COLUMNS + " FROM workflows WHERE id = :id")
                .param("id", workflowId)
                .query(RowMappers.WORKFLOW)
                .optional();
    }

    /**
     * Looks up a previous submission by its client-supplied idempotency key.
     *
     * <p>This is what makes a client safe to retry an ambiguous submission: the second attempt
     * finds the first execution instead of starting a duplicate one.
     */
    public Optional<Workflow> findBySubmissionKey(String submissionKey) {
        return jdbcClient.sql("SELECT " + COLUMNS + " FROM workflows WHERE submission_key = :key")
                .param("key", submissionKey)
                .query(RowMappers.WORKFLOW)
                .optional();
    }

    /** Records the first moment any of this workflow's tasks started running. Idempotent. */
    public boolean markStartedIfUnset(UUID workflowId) {
        int rows = jdbcClient.sql("""
                UPDATE workflows
                   SET started_at = now(), updated_at = now()
                 WHERE id = :id AND started_at IS NULL
                """)
                .param("id", workflowId)
                .update();
        return GuardedUpdate.applied(rows, "markWorkflowStarted", workflowId);
    }

    /**
     * Records first-start for several workflows in one statement.
     *
     * <p>A claim batch can span workflows, and this runs inside the claim transaction, so it has to
     * cost one statement per poll rather than one per claimed task.
     */
    public int markStartedIfUnset(java.util.List<UUID> workflowIds) {
        if (workflowIds.isEmpty()) {
            return 0;
        }
        return jdbcClient.sql("""
                UPDATE workflows
                   SET started_at = now(), updated_at = now()
                 WHERE id IN (:ids) AND started_at IS NULL
                """)
                .param("ids", workflowIds)
                .update();
    }

    /**
     * Settles the workflow as completed, but only if no task is still in flight.
     *
     * <p>The {@code NOT EXISTS} check is inside the same statement as the update on purpose. Two
     * tasks finishing at the same instant will both run this; the {@code status = 'RUNNING'} guard
     * means exactly one wins, and the loser affecting zero rows is a benign race rather than an
     * error, which is why this returns a boolean instead of throwing.
     */
    public boolean completeIfAllTasksTerminal(UUID workflowId) {
        int rows = jdbcClient.sql("""
                UPDATE workflows w
                   SET status = 'COMPLETED', finished_at = now(), updated_at = now()
                 WHERE w.id = :id
                   AND w.status = 'RUNNING'
                   AND NOT EXISTS (
                        SELECT 1 FROM tasks t
                         WHERE t.workflow_id = w.id
                           AND t.status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED'))
                   AND NOT EXISTS (
                        SELECT 1 FROM tasks t
                         WHERE t.workflow_id = w.id
                           AND t.status = 'FAILED')
                """)
                .param("id", workflowId)
                .update();
        return GuardedUpdate.applied(rows, "completeWorkflow", workflowId);
    }

    /** Settles the workflow as failed. Also a benign race between two failing tasks. */
    public boolean markFailed(UUID workflowId) {
        int rows = jdbcClient.sql("""
                UPDATE workflows
                   SET status = 'FAILED', finished_at = now(), updated_at = now()
                 WHERE id = :id AND status = 'RUNNING'
                """)
                .param("id", workflowId)
                .update();
        return GuardedUpdate.applied(rows, "failWorkflow", workflowId);
    }
}
