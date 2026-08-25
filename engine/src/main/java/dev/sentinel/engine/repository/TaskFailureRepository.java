package dev.sentinel.engine.repository;

import dev.sentinel.engine.domain.FailureRecordKind;
import dev.sentinel.engine.domain.RecordedFailure;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * The per-attempt failure record behind the dead-letter view.
 *
 * <p>Writes are deliberately tolerant of duplicates. The table has a unique constraint on
 * {@code (task_id, attempt)}, and a retried report or a reaper pass racing with a live completion
 * can legitimately try to record the same attempt twice. Failing the surrounding transaction over
 * that would turn a benign race into a lost state transition, so the insert ignores the conflict
 * and the constraint does its job silently.
 */
@Repository
public class TaskFailureRepository {

    private static final String COLUMNS =
            "task_id, workflow_id, attempt, worker_id, failure_kind, error_class, error_message, failed_at";

    private final JdbcClient jdbcClient;

    public TaskFailureRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    /** Records several failures in one statement, since the reaper handles a whole batch at once. */
    public int recordAll(List<RecordedFailure> failures) {
        if (failures.isEmpty()) {
            return 0;
        }

        StringBuilder sql = new StringBuilder("""
                INSERT INTO task_failures
                       (task_id, workflow_id, attempt, worker_id, failure_kind, error_class, error_message)
                VALUES
                """);
        Map<String, Object> params = new HashMap<>();

        for (int i = 0; i < failures.size(); i++) {
            RecordedFailure failure = failures.get(i);
            if (i > 0) {
                sql.append(",\n");
            }
            sql.append("(:task").append(i)
                    .append(", :workflow").append(i)
                    .append(", :attempt").append(i)
                    .append(", :worker").append(i)
                    .append(", :kind").append(i)
                    .append(", :class").append(i)
                    .append(", :message").append(i).append(')');

            params.put("task" + i, failure.taskId());
            params.put("workflow" + i, failure.workflowId());
            params.put("attempt" + i, failure.attempt());
            params.put("worker" + i, failure.workerId());
            params.put("kind" + i, failure.kind().name());
            params.put("class" + i, failure.errorClass());
            params.put("message" + i, failure.errorMessage());
        }
        // See the class comment: a duplicate here is a benign race, not a reason to lose a
        // transition that has already been decided.
        sql.append("\nON CONFLICT (task_id, attempt) DO NOTHING");

        return jdbcClient.sql(sql.toString()).params(params).update();
    }

    public int record(RecordedFailure failure) {
        return recordAll(List.of(failure));
    }

    /** The cause chain for one task, oldest attempt first. */
    public List<RecordedFailure> findByTaskId(UUID taskId) {
        return jdbcClient.sql("SELECT " + COLUMNS + " FROM task_failures WHERE task_id = :id ORDER BY attempt")
                .param("id", taskId)
                .query(TaskFailureRepository::map)
                .list();
    }

    public List<RecordedFailure> findByWorkflowId(UUID workflowId) {
        return jdbcClient.sql(
                "SELECT " + COLUMNS + " FROM task_failures WHERE workflow_id = :id ORDER BY failed_at, attempt")
                .param("id", workflowId)
                .query(TaskFailureRepository::map)
                .list();
    }

    private static RecordedFailure map(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
        OffsetDateTime failedAt = rs.getObject("failed_at", OffsetDateTime.class);
        return new RecordedFailure(
                rs.getObject("task_id", UUID.class),
                rs.getObject("workflow_id", UUID.class),
                rs.getInt("attempt"),
                rs.getString("worker_id"),
                FailureRecordKind.valueOf(rs.getString("failure_kind")),
                rs.getString("error_class"),
                rs.getString("error_message"),
                failedAt == null ? null : failedAt.toInstant());
    }

    /** Convenience for tests and diagnostics; the view is the operator-facing surface. */
    public long countForTask(UUID taskId) {
        return jdbcClient.sql("SELECT count(*) FROM task_failures WHERE task_id = :id")
                .param("id", taskId)
                .query(Long.class)
                .single();
    }

    /** Recent failures across all workflows, for an operator asking "what is breaking right now?". */
    public List<RecordedFailure> findSince(Instant since, int limit) {
        return jdbcClient.sql("SELECT " + COLUMNS + """
                  FROM task_failures
                 WHERE failed_at >= :since
                 ORDER BY failed_at DESC
                 LIMIT :limit
                """)
                .param("since", OffsetDateTime.ofInstant(since, java.time.ZoneOffset.UTC))
                .param("limit", limit)
                .query(TaskFailureRepository::map)
                .list();
    }
}
