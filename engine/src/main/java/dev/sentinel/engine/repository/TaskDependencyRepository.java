package dev.sentinel.engine.repository;

import dev.sentinel.engine.domain.TaskEdge;
import dev.sentinel.engine.repository.support.GuardedUpdate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Persistence for DAG edges.
 *
 * <p>Every edge carries {@code workflow_id} even though {@code task_id} already implies it. The
 * redundancy is deliberate: it is the Citus distribution column, and a distributed table has to
 * contain the column it is distributed by. Adding it to a large table later is a migration nobody
 * wants to run.
 *
 * <p>An edge never crosses a workflow boundary. That is not a convention, it is the rule the whole
 * sharding strategy rests on, and it is why a dependency check never has to leave one shard.
 */
@Repository
public class TaskDependencyRepository {

    private final JdbcClient jdbcClient;

    public TaskDependencyRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    /** Inserts every edge of a workflow in one statement, for the same reason task insertion is batched. */
    public int insertAll(UUID workflowId, List<TaskEdge> edges) {
        if (edges.isEmpty()) {
            return 0;
        }

        StringBuilder sql = new StringBuilder(
                "INSERT INTO task_dependencies (workflow_id, task_id, depends_on_task_id) VALUES\n");
        Map<String, Object> params = new HashMap<>();
        params.put("workflowId", workflowId);

        for (int i = 0; i < edges.size(); i++) {
            if (i > 0) {
                sql.append(",\n");
            }
            sql.append("(:workflowId, :task").append(i).append(", :parent").append(i).append(')');
            params.put("task" + i, edges.get(i).taskId());
            params.put("parent" + i, edges.get(i).dependsOnTaskId());
        }

        int rows = jdbcClient.sql(sql.toString()).params(params).update();
        GuardedUpdate.requireExactly(edges.size(), rows, "insertTaskDependencies", workflowId);
        return rows;
    }

    /**
     * Every task reachable downstream of the given task, transitively.
     *
     * <p>Used to cancel a failed branch. The walk is a recursive CTE rather than repeated round
     * trips, and it stays inside one workflow, so its cost is bounded by the DAG size that
     * submission already validated. That bound is what keeps this from becoming unbounded work
     * inside a transaction.
     *
     * <p>{@code UNION} rather than {@code UNION ALL} is load-bearing: a diamond-shaped DAG reaches
     * the same descendant by more than one path, and {@code UNION} both deduplicates and terminates
     * the recursion.
     */
    public List<UUID> findDescendants(UUID workflowId, UUID taskId) {
        return jdbcClient.sql("""
                WITH RECURSIVE descendants AS (
                    SELECT d.task_id
                      FROM task_dependencies d
                     WHERE d.workflow_id = :workflowId
                       AND d.depends_on_task_id = :taskId
                    UNION
                    SELECT d.task_id
                      FROM task_dependencies d
                      JOIN descendants x ON d.depends_on_task_id = x.task_id
                     WHERE d.workflow_id = :workflowId
                )
                SELECT task_id FROM descendants ORDER BY task_id
                """)
                .param("workflowId", workflowId)
                .param("taskId", taskId)
                .query((rs, rowNum) -> rs.getObject("task_id", UUID.class))
                .list();
    }

    public List<TaskEdge> findByWorkflowId(UUID workflowId) {
        return jdbcClient.sql("""
                SELECT task_id, depends_on_task_id
                  FROM task_dependencies
                 WHERE workflow_id = :workflowId
                 ORDER BY task_id, depends_on_task_id
                """)
                .param("workflowId", workflowId)
                .query((rs, rowNum) -> new TaskEdge(
                        rs.getObject("task_id", UUID.class), rs.getObject("depends_on_task_id", UUID.class)))
                .list();
    }
}
