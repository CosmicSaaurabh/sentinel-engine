package dev.sentinel.engine.repository;

import dev.sentinel.engine.domain.OutboxEvent;
import dev.sentinel.engine.domain.OutboxEventType;
import dev.sentinel.engine.repository.support.RowMappers;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * The transactional outbox.
 *
 * <p>This is how the engine avoids the dual-write problem. Writing state to Postgres and history to
 * another store as two separate operations has no atomic version: whichever goes second can fail
 * and leave the two disagreeing forever. Instead the history event is written to this table inside
 * the same transaction as the state change, so either both land or neither does, and a separate
 * relay ships it onward afterwards with retries that are safe because the event is idempotent on
 * its id.
 *
 * <p>Nothing drains this table yet. Events are produced from day one anyway, so that when the
 * history store arrives no state-writing code has to change.
 */
@Repository
public class OutboxRepository {

    private static final String COLUMNS = "id, workflow_id, task_id, event_type, payload, created_at, relayed_at";

    private final JdbcClient jdbcClient;

    public OutboxRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    /** Must be called inside the transaction of the state change it describes, never after it. */
    public UUID append(UUID workflowId, UUID taskId, OutboxEventType eventType, String payload) {
        return jdbcClient.sql("""
                INSERT INTO outbox (workflow_id, task_id, event_type, payload)
                VALUES (:workflowId, :taskId, :eventType, CAST(:payload AS jsonb))
                RETURNING id
                """)
                .param("workflowId", workflowId)
                .param("taskId", taskId)
                .param("eventType", eventType.name())
                .param("payload", payload == null ? "{}" : payload)
                .query((rs, rowNum) -> rs.getObject("id", UUID.class))
                .single();
    }

    /**
     * Appends several events in one statement.
     *
     * <p>Used by the claim path, where a poll can take a whole batch of tasks. One insert per
     * claimed task would make the outbox write scale with batch size inside a transaction that is
     * meant to stay short.
     */
    public int appendAll(UUID workflowId, List<UUID> taskIds, OutboxEventType eventType, String payload) {
        if (taskIds.isEmpty()) {
            return 0;
        }
        StringBuilder sql = new StringBuilder(
                "INSERT INTO outbox (workflow_id, task_id, event_type, payload) VALUES\n");
        java.util.Map<String, Object> params = new java.util.HashMap<>();
        params.put("workflowId", workflowId);
        params.put("eventType", eventType.name());
        params.put("payload", payload == null ? "{}" : payload);

        for (int i = 0; i < taskIds.size(); i++) {
            if (i > 0) {
                sql.append(",\n");
            }
            sql.append("(:workflowId, :task").append(i).append(", :eventType, CAST(:payload AS jsonb))");
            params.put("task" + i, taskIds.get(i));
        }
        return jdbcClient.sql(sql.toString()).params(params).update();
    }

    /**
     * Claims a batch of undelivered events for relay.
     *
     * <p>Deliberately the same {@code SKIP LOCKED} shape as the task claim. Many relay workers
     * draining one shared table of pending items is structurally the same problem as many workers
     * draining a queue, so it gets the same solution rather than a second mechanism to reason
     * about.
     */
    public List<OutboxEvent> claimUnrelayed(int batchSize) {
        return jdbcClient.sql("SELECT " + COLUMNS + """
                  FROM outbox
                 WHERE relayed_at IS NULL
                 ORDER BY created_at, id
                 LIMIT :batchSize
                   FOR UPDATE SKIP LOCKED
                """)
                .param("batchSize", batchSize)
                .query(RowMappers.OUTBOX_EVENT)
                .list();
    }

    public List<OutboxEvent> findByWorkflowId(UUID workflowId) {
        return jdbcClient.sql("SELECT " + COLUMNS + " FROM outbox WHERE workflow_id = :id ORDER BY created_at, id")
                .param("id", workflowId)
                .query(RowMappers.OUTBOX_EVENT)
                .list();
    }

    /** Marking is idempotent: a relay that crashes after shipping but before marking simply reships. */
    public int markRelayed(List<UUID> eventIds) {
        if (eventIds.isEmpty()) {
            return 0;
        }
        return jdbcClient.sql("UPDATE outbox SET relayed_at = now() WHERE id IN (:ids) AND relayed_at IS NULL")
                .param("ids", eventIds)
                .update();
    }

    /**
     * Removes delivered events older than the retention window.
     *
     * <p>Bounded per call so that pruning never becomes one enormous delete holding locks across
     * the table, for the same reason the reaper is batched.
     */
    public int pruneRelayedOlderThan(java.time.Duration retention, int batchSize) {
        return jdbcClient.sql("""
                DELETE FROM outbox
                 WHERE id IN (
                        SELECT id FROM outbox
                         WHERE relayed_at IS NOT NULL
                           AND relayed_at < now() - (interval '1 second' * :retentionSeconds)
                         LIMIT :batchSize)
                """)
                .param("retentionSeconds", retention.toSeconds())
                .param("batchSize", batchSize)
                .update();
    }
}
