package dev.sentinel.engine.repository;

import dev.sentinel.engine.domain.ClaimedTask;
import dev.sentinel.engine.domain.NewTask;
import dev.sentinel.engine.domain.Task;
import dev.sentinel.engine.domain.TaskFailure;
import dev.sentinel.engine.domain.TaskStatus;
import dev.sentinel.engine.repository.support.GuardedUpdate;
import dev.sentinel.engine.repository.support.RowMappers;
import dev.sentinel.engine.repository.support.SqlValues;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Persistence for task instances, which are also the queue entries.
 *
 * <p>Two rules hold for every statement in this class and are worth stating once rather than
 * repeating at each method.
 *
 * <p><strong>Every transition is guarded.</strong> No method reads a row, decides, and then writes.
 * Each conditional {@code UPDATE} restates its precondition in the {@code WHERE} clause so the
 * decision and the write happen in the same atomic step. That is what makes {@code READ COMMITTED}
 * sufficient throughout, and what makes it safe for several engine instances to run the same
 * operation concurrently with no coordination between them.
 *
 * <p><strong>Every timestamp comes from {@code now()}.</strong> No caller passes in a JVM instant
 * for a deadline. Lease expiry compares two values that must originate from one clock.
 */
@Repository
public class TaskRepository {

    private static final String COLUMNS = """
            id, workflow_id, name, task_type, status, input, output, last_error, last_error_kind,
            attempt, max_attempts, pending_dependencies, next_attempt_at, lease_expires_at,
            owner_worker_id, fencing_token, created_at, updated_at
            """;

    // The workflow's trace context is joined in rather than denormalised onto the task, because it
    // is written once per workflow and read once per claim; copying it to every task row would
    // trade a cheap join for a wider row on the highest-churn table in the system.
    private static final String CLAIM_COLUMNS =
            "t.id, t.workflow_id, t.name, t.task_type, t.input, t.attempt, t.max_attempts, "
                    + "t.fencing_token, t.lease_expires_at, "
                    + "(SELECT w.trace_context FROM workflows w WHERE w.id = t.workflow_id) AS trace_context";

    /**
     * How many rows one multi-row {@code INSERT} carries.
     *
     * <p>Postgres allows at most 65535 bind parameters in a single statement, and each task here
     * binds seven. A large DAG submitted as one statement would hit that ceiling as a confusing
     * driver error rather than as a clear limit, so insertion chunks instead. Chunking also caps
     * how much SQL text has to be parsed for one submission.
     */
    private static final int INSERT_CHUNK_SIZE = 500;

    private static final org.springframework.jdbc.core.RowMapper<ExpiredLease> EXPIRED_LEASE =
            (rs, rowNum) -> new ExpiredLease(
                    rs.getObject("id", UUID.class),
                    rs.getObject("workflow_id", UUID.class),
                    rs.getInt("attempt"),
                    rs.getString("owner_worker_id"));

    private final JdbcClient jdbcClient;

    public TaskRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    // ------------------------------------------------------------------
    // Writes at submission time
    // ------------------------------------------------------------------

    /**
     * Inserts every task of a workflow in one statement and returns the generated ids by task name.
     *
     * <p>One statement rather than a loop because the caller needs the ids back to build the
     * dependency edges, and a per-task round trip would multiply submission latency by the DAG's
     * size. The row count is bounded by DAG-size validation upstream, so the generated SQL cannot
     * grow without limit.
     *
     * @return task name to generated id, in insertion order
     */
    public Map<String, UUID> insertAll(UUID workflowId, List<NewTask> tasks) {
        if (tasks.isEmpty()) {
            throw new IllegalArgumentException("a workflow must contain at least one task");
        }

        Map<String, UUID> idsByName = new LinkedHashMap<>();
        for (int start = 0; start < tasks.size(); start += INSERT_CHUNK_SIZE) {
            List<NewTask> chunk = tasks.subList(start, Math.min(start + INSERT_CHUNK_SIZE, tasks.size()));
            idsByName.putAll(insertChunk(workflowId, chunk));
        }
        return idsByName;
    }

    private Map<String, UUID> insertChunk(UUID workflowId, List<NewTask> tasks) {
        StringBuilder sql = new StringBuilder("""
                INSERT INTO tasks (workflow_id, name, task_type, status, input,
                                   max_attempts, pending_dependencies, next_attempt_at)
                VALUES
                """);
        Map<String, Object> params = new HashMap<>();
        params.put("workflowId", workflowId);

        for (int i = 0; i < tasks.size(); i++) {
            NewTask task = tasks.get(i);
            if (i > 0) {
                sql.append(",\n");
            }
            sql.append("(:workflowId, :name").append(i)
                    .append(", :type").append(i)
                    .append(", :status").append(i)
                    .append(", CAST(:input").append(i).append(" AS jsonb)")
                    .append(", :maxAttempts").append(i)
                    .append(", :pendingDeps").append(i)
                    .append(", coalesce(CAST(:notBefore").append(i).append(" AS timestamptz), now()))");

            params.put("name" + i, task.name());
            params.put("type" + i, task.taskType());
            params.put("status" + i, task.status().name());
            params.put("input" + i, task.input());
            params.put("maxAttempts" + i, task.maxAttempts());
            params.put("pendingDeps" + i, task.pendingDependencies());
            params.put("notBefore" + i, SqlValues.timestamp(task.notBefore()));
        }
        sql.append("\nRETURNING id, name");

        List<Map.Entry<String, UUID>> inserted = jdbcClient.sql(sql.toString())
                .params(params)
                .query((rs, rowNum) -> Map.entry(rs.getString("name"), rs.getObject("id", UUID.class)))
                .list();

        GuardedUpdate.requireExactly(tasks.size(), inserted.size(), "insertTasks", workflowId);

        Map<String, UUID> chunkIds = new LinkedHashMap<>();
        inserted.forEach(entry -> chunkIds.put(entry.getKey(), entry.getValue()));
        return chunkIds;
    }

    // ------------------------------------------------------------------
    // Reads
    // ------------------------------------------------------------------

    public Optional<Task> findById(UUID taskId) {
        return jdbcClient.sql("SELECT " + COLUMNS + " FROM tasks WHERE id = :id")
                .param("id", taskId)
                .query(RowMappers.TASK)
                .optional();
    }

    public List<Task> findByWorkflowId(UUID workflowId) {
        return jdbcClient.sql("SELECT " + COLUMNS + " FROM tasks WHERE workflow_id = :id ORDER BY id")
                .param("id", workflowId)
                .query(RowMappers.TASK)
                .list();
    }

    public Map<TaskStatus, Integer> countByStatus(UUID workflowId) {
        Map<TaskStatus, Integer> counts = new java.util.EnumMap<>(TaskStatus.class);
        jdbcClient.sql("SELECT status, count(*) AS n FROM tasks WHERE workflow_id = :id GROUP BY status")
                .param("id", workflowId)
                .query((rs, rowNum) -> Map.entry(TaskStatus.valueOf(rs.getString("status")), rs.getInt("n")))
                .list()
                .forEach(entry -> counts.put(entry.getKey(), entry.getValue()));
        return counts;
    }

    // ------------------------------------------------------------------
    // The claim
    // ------------------------------------------------------------------

    /**
     * Takes ownership of up to {@code batchSize} runnable tasks in a single statement.
     *
     * <p>Selection and ownership happen together so there is no window in which a task has been
     * chosen but not yet claimed. {@code SKIP LOCKED} is what lets many claimers run concurrently:
     * a claimer that meets a row another transaction is already taking walks past it instead of
     * blocking, so throughput rises with concurrency instead of collapsing into a queue behind the
     * head of the btree.
     *
     * <p>Under-delivering is correct, not a bug. High contention can return fewer rows than asked
     * for, or none at all, and callers must treat an empty result as ordinary rather than
     * hot-spinning on it.
     *
     * <p>{@code attempt < max_attempts} is in the predicate so that the claim can never push
     * {@code attempt} past its bound. The database would reject that anyway via
     * {@code tasks_attempt_bounds}; the guard means a missed retry check surfaces as an unclaimable
     * task rather than a failed statement.
     */
    public List<ClaimedTask> claimBatch(String workerId, List<String> taskTypes, int batchSize, Duration leaseDuration) {
        if (taskTypes.isEmpty() || batchSize <= 0) {
            return List.of();
        }
        return jdbcClient.sql("""
                WITH claimable AS (
                    SELECT id
                      FROM tasks
                     WHERE status = 'PENDING'
                       AND next_attempt_at <= now()
                       AND task_type IN (:taskTypes)
                       AND attempt < max_attempts
                     ORDER BY next_attempt_at, id
                     LIMIT :batchSize
                       FOR UPDATE SKIP LOCKED
                )
                UPDATE tasks t
                   SET status           = 'RUNNING',
                       owner_worker_id  = :workerId,
                       fencing_token    = t.fencing_token + 1,
                       lease_expires_at = now() + (interval '1 second' * :leaseSeconds),
                       attempt          = t.attempt + 1,
                       updated_at       = now()
                  FROM claimable c
                 WHERE t.id = c.id
                RETURNING
                """ + CLAIM_COLUMNS)
                .param("taskTypes", taskTypes)
                .param("batchSize", batchSize)
                .param("workerId", workerId)
                .param("leaseSeconds", leaseDuration.toSeconds())
                .query(RowMappers.CLAIMED_TASK)
                .list();
    }

    // ------------------------------------------------------------------
    // Worker-driven transitions, all fenced
    // ------------------------------------------------------------------

    /**
     * Extends a lease.
     *
     * <p>This is the highest-frequency write in the system, and it touches exactly one column.
     * {@code lease_expires_at} is indexed nowhere, so this update stays eligible to be a HOT update
     * and does no index maintenance at all. Adding an index that covers this column would quietly
     * make every heartbeat in the fleet more expensive.
     *
     * @return false if the caller no longer owns the task, which is a definitive answer rather than
     *         a transient condition
     */
    public boolean renewLease(UUID taskId, String workerId, long fencingToken, Duration leaseDuration) {
        int rows = jdbcClient.sql("""
                UPDATE tasks
                   SET lease_expires_at = now() + (interval '1 second' * :leaseSeconds),
                       updated_at = now()
                 WHERE id = :id
                   AND status = 'RUNNING'
                   AND owner_worker_id = :workerId
                   AND fencing_token = :fencingToken
                """)
                .param("id", taskId)
                .param("workerId", workerId)
                .param("fencingToken", fencingToken)
                .param("leaseSeconds", leaseDuration.toSeconds())
                .update();
        return GuardedUpdate.applied(rows, "renewLease", taskId);
    }

    /**
     * Renews several leases held by one worker, in a single statement.
     *
     * <p>A worker running sixteen tasks renews sixteen leases in one round trip rather than
     * sixteen. Heartbeats are the highest-frequency traffic in the system, and each one otherwise
     * costs a whole round trip to update a single timestamp.
     *
     * <p>Like the single-lease version, this writes only {@code lease_expires_at}, which is indexed
     * nowhere, so the update stays eligible to be a HOT update and does no index maintenance.
     *
     * @return the ids actually renewed. Anything the caller asked about and does not get back has
     *         been lost to another worker, and the caller must be told which is which rather than
     *         having the whole call fail because of one dead lease
     */
    public List<UUID> renewLeases(String workerId, Map<UUID, Long> fencingTokensByTaskId, Duration leaseDuration) {
        if (fencingTokensByTaskId.isEmpty()) {
            return List.of();
        }

        StringBuilder values = new StringBuilder();
        Map<String, Object> params = new HashMap<>();
        params.put("workerId", workerId);
        params.put("leaseSeconds", leaseDuration.toSeconds());

        int index = 0;
        for (Map.Entry<UUID, Long> lease : fencingTokensByTaskId.entrySet()) {
            if (index > 0) {
                values.append(", ");
            }
            values.append("(CAST(:taskId").append(index).append(" AS uuid), CAST(:fence")
                    .append(index).append(" AS bigint))");
            params.put("taskId" + index, lease.getKey());
            params.put("fence" + index, lease.getValue());
            index++;
        }

        return jdbcClient.sql("""
                UPDATE tasks t
                   SET lease_expires_at = now() + (interval '1 second' * :leaseSeconds),
                       updated_at = now()
                  FROM (VALUES %s) AS requested(task_id, fencing_token)
                 WHERE t.id = requested.task_id
                   AND t.status = 'RUNNING'
                   AND t.owner_worker_id = :workerId
                   AND t.fencing_token = requested.fencing_token
                RETURNING t.id
                """.formatted(values))
                .params(params)
                .query((rs, rowNum) -> rs.getObject("id", UUID.class))
                .list();
    }

    /** Records success. Clears the lease, which the {@code tasks_lease_consistency} check requires. */
    public boolean markCompleted(UUID taskId, String workerId, long fencingToken, String output) {
        int rows = jdbcClient.sql("""
                UPDATE tasks
                   SET status = 'COMPLETED',
                       output = CAST(:output AS jsonb),
                       owner_worker_id = NULL,
                       lease_expires_at = NULL,
                       updated_at = now()
                 WHERE id = :id
                   AND status = 'RUNNING'
                   AND owner_worker_id = :workerId
                   AND fencing_token = :fencingToken
                """)
                .param("id", taskId)
                .param("workerId", workerId)
                .param("fencingToken", fencingToken)
                .param("output", output == null ? "null" : output)
                .update();
        return GuardedUpdate.applied(rows, "markCompleted", taskId);
    }

    /** Records terminal failure: attempts exhausted, or a failure classified as permanent. */
    public boolean markFailed(UUID taskId, String workerId, long fencingToken, TaskFailure failure) {
        int rows = jdbcClient.sql("""
                UPDATE tasks
                   SET status = 'FAILED',
                       last_error = :error,
                       last_error_kind = :errorKind,
                       owner_worker_id = NULL,
                       lease_expires_at = NULL,
                       updated_at = now()
                 WHERE id = :id
                   AND status = 'RUNNING'
                   AND owner_worker_id = :workerId
                   AND fencing_token = :fencingToken
                """)
                .param("id", taskId)
                .param("workerId", workerId)
                .param("fencingToken", fencingToken)
                .param("error", failure.message())
                .param("errorKind", failure.kind().name())
                .update();
        return GuardedUpdate.applied(rows, "markFailed", taskId);
    }

    /**
     * Returns a failed task to the queue after a delay.
     *
     * <p>The delay is applied to {@code next_attempt_at} rather than held in a scheduler, so a
     * backoff survives an engine restart with no in-memory timer to rebuild. The claim query
     * already refuses to take a task before its time, so the queue itself enforces the wait.
     */
    public boolean scheduleRetry(
            UUID taskId, String workerId, long fencingToken, TaskFailure failure, Duration delay) {
        int rows = jdbcClient.sql("""
                UPDATE tasks
                   SET status = 'PENDING',
                       last_error = :error,
                       last_error_kind = :errorKind,
                       next_attempt_at = now() + (interval '1 second' * :delaySeconds),
                       owner_worker_id = NULL,
                       lease_expires_at = NULL,
                       updated_at = now()
                 WHERE id = :id
                   AND status = 'RUNNING'
                   AND owner_worker_id = :workerId
                   AND fencing_token = :fencingToken
                   AND attempt < max_attempts
                """)
                .param("id", taskId)
                .param("workerId", workerId)
                .param("fencingToken", fencingToken)
                .param("error", failure.message())
                .param("errorKind", failure.kind().name())
                .param("delaySeconds", delay.toSeconds())
                .update();
        return GuardedUpdate.applied(rows, "scheduleRetry", taskId);
    }

    // ------------------------------------------------------------------
    // Scheduler-driven transitions
    // ------------------------------------------------------------------

    /**
     * Locks the direct children of a completed task, in ascending id order.
     *
     * <p>The ordering is the deadlock defence, not a convenience. Two parents sharing children can
     * complete concurrently on two engine instances; without a common acquisition order, one
     * transaction can hold child X while waiting for Y as the other holds Y and waits for X, and
     * Postgres resolves that by killing one of them. Sorting by id gives every transaction the same
     * order, so the cycle cannot form.
     *
     * <p>Must be called inside the same transaction as the decrement that follows it.
     */
    public List<UUID> lockChildrenOfCompletedTask(UUID workflowId, UUID parentTaskId) {
        return jdbcClient.sql("""
                SELECT c.id
                  FROM tasks c
                  JOIN task_dependencies d ON d.task_id = c.id
                 WHERE d.workflow_id = :workflowId
                   AND d.depends_on_task_id = :parentTaskId
                 ORDER BY c.id
                   FOR UPDATE OF c
                """)
                .param("workflowId", workflowId)
                .param("parentTaskId", parentTaskId)
                .query((rs, rowNum) -> rs.getObject("id", UUID.class))
                .list();
    }

    /**
     * Decrements the blocker count of the given children and flips to {@code PENDING} the ones that
     * just reached zero.
     *
     * <p>A counter rather than a "do all my parents look complete" subquery, because the subquery
     * re-derives on every completion something the previous completion already established, at a
     * cost proportional to the number of parents.
     *
     * <p>The counter cannot drift, because this runs in the same transaction as the guarded update
     * that moved the parent to {@code COMPLETED}. That update matches exactly one row exactly once;
     * if it matches nothing, the transaction never reaches this statement.
     *
     * @return ids that became claimable
     */
    public List<UUID> decrementDependenciesAndFlip(List<UUID> childTaskIds) {
        if (childTaskIds.isEmpty()) {
            return List.of();
        }
        return jdbcClient.sql("""
                UPDATE tasks
                   SET pending_dependencies = pending_dependencies - 1,
                       status = CASE WHEN pending_dependencies - 1 = 0 THEN 'PENDING' ELSE status END,
                       updated_at = now()
                 WHERE id IN (:childIds)
                   AND status = 'BLOCKED'
                RETURNING id, status
                """)
                .param("childIds", childTaskIds)
                .query((rs, rowNum) -> Map.entry(
                        rs.getObject("id", UUID.class), TaskStatus.valueOf(rs.getString("status"))))
                .list()
                .stream()
                .filter(entry -> entry.getValue() == TaskStatus.PENDING)
                .map(Map.Entry::getKey)
                .toList();
    }

    /**
     * Cancels tasks that can no longer run because an ancestor failed.
     *
     * <p>Only non-started tasks are cancellable. A task already {@code RUNNING} is left alone: its
     * worker is mid-execution and may already have caused an external side effect, so the engine
     * lets it finish and report rather than pretending it never happened.
     *
     * @return the ids actually cancelled, which is what history should record. Returning the input
     *         list instead would claim running tasks were cancelled when they were not
     */
    public List<UUID> cancelAll(List<UUID> taskIds) {
        if (taskIds.isEmpty()) {
            return List.of();
        }
        return jdbcClient.sql("""
                UPDATE tasks
                   SET status = 'CANCELLED', updated_at = now()
                 WHERE id IN (:ids)
                   AND status IN ('BLOCKED', 'PENDING')
                RETURNING id
                """)
                .param("ids", taskIds)
                .query((rs, rowNum) -> rs.getObject("id", UUID.class))
                .list();
    }

    /**
     * Cancels every task in a workflow that has not started.
     *
     * <p>Used when a workflow reaches terminal failure. Tasks already {@code RUNNING} are left
     * alone for the same reason they survive branch cancellation: their worker may already have
     * caused an external side effect, and pretending otherwise would make the recorded history
     * disagree with the world.
     *
     * @return ids of the tasks that were cancelled
     */
    public List<UUID> cancelNotStarted(UUID workflowId) {
        return jdbcClient.sql("""
                UPDATE tasks
                   SET status = 'CANCELLED', updated_at = now()
                 WHERE workflow_id = :workflowId
                   AND status IN ('BLOCKED', 'PENDING')
                RETURNING id
                """)
                .param("workflowId", workflowId)
                .query((rs, rowNum) -> rs.getObject("id", UUID.class))
                .list();
    }

    // ------------------------------------------------------------------
    // Reaper
    // ------------------------------------------------------------------

    /**
     * Returns tasks whose lease expired to the queue, up to a bounded batch.
     *
     * <p>Bounded and {@code SKIP LOCKED} for the same reason the claim is: a hundred workers dying
     * at once must not turn into one enormous transaction that locks thousands of rows and stalls
     * every claimer. Several engine instances reaping at the same time divide the work rather than
     * colliding, which is what keeps the reaper leaderless.
     *
     * <p><strong>The delay is jittered per task, not per batch.</strong> {@code random()} is
     * evaluated once per row, so a recovered batch is spread across the window rather than becoming
     * claimable at one instant. A single delay for the whole batch would remove the lock contention
     * and then recreate the stampede one step later, which is the more damaging half of the
     * problem.
     *
     * <p>The previous owner and attempt number are read from the CTE rather than from
     * {@code RETURNING} on the updated row, because the update has already cleared the owner by
     * then. They are needed to record who was holding the lease when it lapsed.
     */
    public List<ExpiredLease> requeueExpiredLeases(int batchSize, Duration retryDelay) {
        return jdbcClient.sql("""
                WITH expired AS (
                    SELECT id, workflow_id, attempt, owner_worker_id
                      FROM tasks
                     WHERE status = 'RUNNING'
                       AND lease_expires_at < now()
                       AND attempt < max_attempts
                     ORDER BY id
                     LIMIT :batchSize
                       FOR UPDATE SKIP LOCKED
                )
                UPDATE tasks t
                   SET status = 'PENDING',
                       owner_worker_id = NULL,
                       lease_expires_at = NULL,
                       next_attempt_at = now()
                           + (interval '1 second' * :delaySeconds * (0.5 + random())),
                       last_error = 'lease expired before the worker reported',
                       last_error_kind = 'RETRYABLE',
                       updated_at = now()
                  FROM expired e
                 WHERE t.id = e.id
                RETURNING e.id, e.workflow_id, e.attempt, e.owner_worker_id
                """)
                .param("batchSize", batchSize)
                .param("delaySeconds", retryDelay.toSeconds())
                .query(EXPIRED_LEASE)
                .list();
    }

    /**
     * Fails tasks whose lease expired and whose attempts are spent.
     *
     * <p>Separate from {@link #requeueExpiredLeases} because the two outcomes are different
     * decisions, and folding them into one {@code CASE} would hide which one happened from metrics
     * and from anyone reading the SQL.
     */
    public List<ExpiredLease> failExhaustedExpiredLeases(int batchSize) {
        return jdbcClient.sql("""
                WITH expired AS (
                    SELECT id, workflow_id, attempt, owner_worker_id
                      FROM tasks
                     WHERE status = 'RUNNING'
                       AND lease_expires_at < now()
                       AND attempt >= max_attempts
                     ORDER BY id
                     LIMIT :batchSize
                       FOR UPDATE SKIP LOCKED
                )
                UPDATE tasks t
                   SET status = 'FAILED',
                       owner_worker_id = NULL,
                       lease_expires_at = NULL,
                       last_error = 'lease expired after the final attempt',
                       last_error_kind = 'RETRYABLE',
                       updated_at = now()
                  FROM expired e
                 WHERE t.id = e.id
                RETURNING e.id, e.workflow_id, e.attempt, e.owner_worker_id
                """)
                .param("batchSize", batchSize)
                .query(EXPIRED_LEASE)
                .list();
    }

    /**
     * How many leases have expired and not yet been reaped.
     *
     * <p>Exposed as a gauge. If expiries arrive faster than the reaper drains them, recovery
     * latency climbs and nothing else says so: the reaper keeps working, each pass looks healthy,
     * and only this number growing reveals that it is losing ground.
     */
    public long countExpiredLeases() {
        return jdbcClient.sql("""
                SELECT count(*) FROM tasks
                 WHERE status = 'RUNNING' AND lease_expires_at < now()
                """)
                .query(Long.class)
                .single();
    }

    /**
     * A lease that lapsed, as it was just before the reaper touched it.
     *
     * @param ownerWorkerId who was holding it, which is the detail that turns "a task was reaped"
     *        into "this worker stopped reporting", and is gone from the row once it is released
     */
    public record ExpiredLease(UUID taskId, UUID workflowId, int attempt, String ownerWorkerId) {
    }
}
