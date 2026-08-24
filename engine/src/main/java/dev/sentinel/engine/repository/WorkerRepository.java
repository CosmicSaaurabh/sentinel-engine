package dev.sentinel.engine.repository;

import dev.sentinel.engine.domain.TaskType;
import java.time.Duration;
import java.util.List;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Worker liveness and declared capacity.
 *
 * <p>Read the warning in the migration before relying on this table for anything: it is
 * observability and capacity accounting, never the correctness mechanism. Ownership of a task is
 * proven by that task's fencing token, so a stale or missing row here costs a dashboard its
 * accuracy and can never cause a task to run twice or be lost.
 *
 * <p>Keeping liveness out of the correctness path is deliberate. A push-based "is this worker
 * alive" registry invites split-brain, where a worker looks dead to the registry while it is very
 * much alive and halfway through a side effect. Leases plus fencing sidestep that entirely.
 */
@Repository
public class WorkerRepository {

    private final JdbcClient jdbcClient;

    public WorkerRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    /**
     * Registers a worker or refreshes its heartbeat.
     *
     * <p>Task types are joined with commas and split by Postgres rather than bound as an array,
     * which is safe because the character set of a task type excludes the delimiter and is enforced
     * both here and by a {@code CHECK} constraint.
     */
    public void registerOrHeartbeat(String workerId, List<String> taskTypes, int maxConcurrency) {
        taskTypes.forEach(TaskType::requireValid);
        jdbcClient.sql("""
                INSERT INTO workers (id, task_types, max_concurrency, last_heartbeat_at)
                VALUES (:id, string_to_array(:taskTypes, ','), :maxConcurrency, now())
                ON CONFLICT (id) DO UPDATE
                   SET task_types = excluded.task_types,
                       max_concurrency = excluded.max_concurrency,
                       last_heartbeat_at = now()
                """)
                .param("id", workerId)
                .param("taskTypes", String.join(",", taskTypes))
                .param("maxConcurrency", maxConcurrency)
                .update();
    }

    public List<String> findStale(Duration staleAfter) {
        return jdbcClient.sql("""
                SELECT id FROM workers
                 WHERE last_heartbeat_at < now() - (interval '1 second' * :staleSeconds)
                 ORDER BY last_heartbeat_at
                """)
                .param("staleSeconds", staleAfter.toSeconds())
                .query((rs, rowNum) -> rs.getString("id"))
                .list();
    }

    public int deleteStale(Duration staleAfter) {
        return jdbcClient.sql("""
                DELETE FROM workers
                 WHERE last_heartbeat_at < now() - (interval '1 second' * :staleSeconds)
                """)
                .param("staleSeconds", staleAfter.toSeconds())
                .update();
    }

    public long countAlive(Duration staleAfter) {
        return jdbcClient.sql("""
                SELECT count(*) FROM workers
                 WHERE last_heartbeat_at >= now() - (interval '1 second' * :staleSeconds)
                """)
                .param("staleSeconds", staleAfter.toSeconds())
                .query(Long.class)
                .single();
    }
}
