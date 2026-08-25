package dev.sentinel.engine.repository;

import dev.sentinel.engine.domain.FailureKind;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Reads the dead-letter view.
 *
 * <p>The view rather than a table, for the reason LLD-003 records: terminal failures are already
 * {@code tasks} rows with {@code status = 'FAILED'}, and moving them elsewhere would mean a second
 * write path on the failure path and a second place to look a task up by id.
 *
 * <p>Read-only. Nothing here writes, because there is nothing to write: a task becomes dead-lettered
 * by failing, not by being moved.
 */
@Repository
public class DeadLetterRepository {

    private static final int MAX_LIMIT = 500;

    private final JdbcClient jdbcClient;

    public DeadLetterRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    /** Newest first, because an operator asking "what is broken" means "what broke recently". */
    public List<DeadLetterTask> findRecent(int limit) {
        return jdbcClient.sql("""
                SELECT task_id, workflow_id, workflow_name, task_name, task_type, attempt,
                       max_attempts, last_error, last_error_kind, failed_at, recorded_failures
                  FROM dead_letter_tasks
                 ORDER BY failed_at DESC
                 LIMIT :limit
                """)
                .param("limit", boundedLimit(limit))
                .query(DeadLetterRepository::map)
                .list();
    }

    public List<DeadLetterTask> findByWorkflowId(UUID workflowId, int limit) {
        return jdbcClient.sql("""
                SELECT task_id, workflow_id, workflow_name, task_name, task_type, attempt,
                       max_attempts, last_error, last_error_kind, failed_at, recorded_failures
                  FROM dead_letter_tasks
                 WHERE workflow_id = :workflowId
                 ORDER BY failed_at DESC
                 LIMIT :limit
                """)
                .param("workflowId", workflowId)
                .param("limit", boundedLimit(limit))
                .query(DeadLetterRepository::map)
                .list();
    }

    /**
     * Clamps rather than rejects.
     *
     * <p>A caller asking for everything is asking a reasonable question badly, and answering with
     * an error helps nobody. A caller asking for zero almost certainly forgot to set the field, so
     * that becomes the default rather than an empty result that looks like "nothing is broken".
     */
    private static int boundedLimit(int requested) {
        return requested <= 0 ? 50 : Math.min(requested, MAX_LIMIT);
    }

    private static DeadLetterTask map(ResultSet rs, int rowNum) throws SQLException {
        String errorKind = rs.getString("last_error_kind");
        OffsetDateTime failedAt = rs.getObject("failed_at", OffsetDateTime.class);
        return new DeadLetterTask(
                rs.getObject("task_id", UUID.class),
                rs.getObject("workflow_id", UUID.class),
                rs.getString("workflow_name"),
                rs.getString("task_name"),
                rs.getString("task_type"),
                rs.getInt("attempt"),
                rs.getInt("max_attempts"),
                rs.getString("last_error"),
                errorKind == null ? null : FailureKind.valueOf(errorKind),
                failedAt == null ? null : failedAt.toInstant(),
                rs.getInt("recorded_failures"));
    }

    /**
     * @param recordedFailures how many attempts have a recorded cause. Usually equal to
     *        {@code attempt}, and a gap between them means some attempts ended in a way that was
     *        never recorded, which is itself worth noticing
     */
    public record DeadLetterTask(
            UUID taskId,
            UUID workflowId,
            String workflowName,
            String taskName,
            String taskType,
            int attempt,
            int maxAttempts,
            String lastError,
            FailureKind lastErrorKind,
            Instant failedAt,
            int recordedFailures) {
    }
}
