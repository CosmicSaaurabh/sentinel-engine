package dev.sentinel.engine.repository.support;

import dev.sentinel.engine.domain.ClaimedTask;
import dev.sentinel.engine.domain.FailureKind;
import dev.sentinel.engine.domain.Lease;
import dev.sentinel.engine.domain.OutboxEvent;
import dev.sentinel.engine.domain.OutboxEventType;
import dev.sentinel.engine.domain.Task;
import dev.sentinel.engine.domain.TaskFailure;
import dev.sentinel.engine.domain.TaskStatus;
import dev.sentinel.engine.domain.Workflow;
import dev.sentinel.engine.domain.WorkflowStatus;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import org.springframework.jdbc.core.RowMapper;

/**
 * Result-set to domain mapping, kept in one place so that a column rename breaks compilation in a
 * single file rather than silently in several.
 *
 * <p>Timestamps are read as {@code OffsetDateTime} and converted, because the columns are
 * {@code timestamptz} and every one of them was produced by the database's own clock. No mapper
 * here ever substitutes a JVM clock for a missing value.
 */
public final class RowMappers {

    public static final RowMapper<Workflow> WORKFLOW = RowMappers::mapWorkflow;
    public static final RowMapper<Task> TASK = RowMappers::mapTask;
    public static final RowMapper<ClaimedTask> CLAIMED_TASK = RowMappers::mapClaimedTask;
    public static final RowMapper<OutboxEvent> OUTBOX_EVENT = RowMappers::mapOutboxEvent;

    private RowMappers() {
    }

    private static Workflow mapWorkflow(ResultSet rs, int rowNum) throws SQLException {
        return new Workflow(
                rs.getObject("id", java.util.UUID.class),
                rs.getString("name"),
                WorkflowStatus.valueOf(rs.getString("status")),
                rs.getString("submission_key"),
                instant(rs, "scheduled_at"),
                instant(rs, "started_at"),
                instant(rs, "finished_at"),
                instant(rs, "created_at"),
                instant(rs, "updated_at"));
    }

    private static Task mapTask(ResultSet rs, int rowNum) throws SQLException {
        TaskStatus status = TaskStatus.valueOf(rs.getString("status"));
        String ownerWorkerId = rs.getString("owner_worker_id");
        Instant leaseExpiresAt = instant(rs, "lease_expires_at");
        // The database guarantees these are both present exactly for RUNNING rows
        // (tasks_lease_consistency), so this reconstruction cannot disagree with the row.
        Lease lease = status == TaskStatus.RUNNING ? new Lease(ownerWorkerId, leaseExpiresAt) : null;

        String errorKind = rs.getString("last_error_kind");
        TaskFailure failure =
                errorKind == null ? null : new TaskFailure(rs.getString("last_error"), FailureKind.valueOf(errorKind));

        return new Task(
                rs.getObject("id", java.util.UUID.class),
                rs.getObject("workflow_id", java.util.UUID.class),
                rs.getString("name"),
                rs.getString("task_type"),
                status,
                rs.getString("input"),
                rs.getString("output"),
                failure,
                rs.getInt("attempt"),
                rs.getInt("max_attempts"),
                rs.getInt("pending_dependencies"),
                instant(rs, "next_attempt_at"),
                lease,
                rs.getLong("fencing_token"),
                instant(rs, "created_at"),
                instant(rs, "updated_at"));
    }

    private static ClaimedTask mapClaimedTask(ResultSet rs, int rowNum) throws SQLException {
        return new ClaimedTask(
                rs.getObject("id", java.util.UUID.class),
                rs.getObject("workflow_id", java.util.UUID.class),
                rs.getString("name"),
                rs.getString("task_type"),
                rs.getString("input"),
                rs.getInt("attempt"),
                rs.getInt("max_attempts"),
                rs.getLong("fencing_token"),
                instant(rs, "lease_expires_at"),
                rs.getString("trace_context"));
    }

    private static OutboxEvent mapOutboxEvent(ResultSet rs, int rowNum) throws SQLException {
        return new OutboxEvent(
                rs.getObject("id", java.util.UUID.class),
                rs.getObject("workflow_id", java.util.UUID.class),
                rs.getObject("task_id", java.util.UUID.class),
                OutboxEventType.valueOf(rs.getString("event_type")),
                rs.getString("payload"),
                instant(rs, "created_at"),
                instant(rs, "relayed_at"));
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }
}
