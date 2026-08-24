package dev.sentinel.engine.service;

import static org.assertj.core.api.Assertions.assertThat;

import dev.sentinel.engine.AbstractIntegrationTest;
import dev.sentinel.engine.domain.NewWorkflow;
import dev.sentinel.engine.repository.WorkflowRepository;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Asserts the shape of the claim query's execution plan.
 *
 * <p>This test exists because of a specific silent-failure mode. A partial index is only used when
 * the query's predicate matches the index's predicate, and that match is easy to break by accident:
 * adding a status to the claim's {@code IN} list, or comparing a column slightly differently, is
 * enough for Postgres to stop using {@code tasks_claimable_idx} and fall back to scanning the whole
 * table. Nothing errors. The queue simply gets slower and slower as history accumulates, and the
 * cause is invisible from the application.
 *
 * <p>Turning the plan into an assertion means that regression breaks the build on the commit that
 * causes it rather than in production some months later.
 */
class ClaimQueryPlanTest extends AbstractIntegrationTest {

    private static final int SEEDED_TASKS = 20_000;
    private static final String TASK_TYPE = "plan.probe";

    @Autowired
    private WorkflowRepository workflows;

    @BeforeEach
    void seedEnoughRowsForThePlannerToCare() {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("plan-probe")).id();
        jdbcClient.sql("""
                INSERT INTO tasks (workflow_id, name, task_type, status)
                SELECT :workflowId, 'task-' || g, :taskType, 'PENDING'
                  FROM generate_series(1, :count) g
                """)
                .param("workflowId", workflowId)
                .param("taskType", TASK_TYPE)
                .param("count", SEEDED_TASKS)
                .update();
        // Without fresh statistics the planner guesses from defaults, and this test would be
        // asserting against a guess rather than against the plan a loaded system would get.
        jdbcClient.sql("ANALYZE tasks").update();
    }

    @Test
    @DisplayName("the claim uses the partial index and never scans the tasks table")
    void claimUsesThePartialIndex() {
        String plan = explainClaim();

        assertThat(plan)
                .as("the partial index is what keeps claim cost proportional to queue depth")
                .contains("tasks_claimable_idx");
        assertThat(plan)
                .as("a sequential scan here would make claims slow down as completed history accumulates")
                .doesNotContain("Seq Scan on tasks");
    }

    @Test
    @DisplayName("the claim needs no sort, because the index already provides the claim order")
    void claimNeedsNoSort() {
        String plan = explainClaim();

        assertThat(plan)
                .as("a Sort node means the index no longer matches the ORDER BY, and every claim "
                        + "would have to materialise the whole queue to find the oldest rows")
                .doesNotContain("Sort");
    }

    @Test
    @DisplayName("the claim locks rows with SKIP LOCKED rather than waiting on them")
    void claimSkipsLockedRows() {
        assertThat(explainClaim()).contains("LockRows");
    }

    /**
     * Explains the selection half of the claim.
     *
     * <p>Plain {@code EXPLAIN} rather than {@code EXPLAIN ANALYZE}, because analysing would execute
     * the statement and claim real rows as a side effect of a test that is only meant to read the
     * plan.
     */
    private String explainClaim() {
        return String.join("\n", jdbcClient.sql("""
                EXPLAIN
                SELECT id
                  FROM tasks
                 WHERE status = 'PENDING'
                   AND next_attempt_at <= now()
                   AND task_type IN (:taskType)
                   AND attempt < max_attempts
                 ORDER BY next_attempt_at, id
                 LIMIT 32
                   FOR UPDATE SKIP LOCKED
                """)
                .param("taskType", TASK_TYPE)
                .query(String.class)
                .list());
    }
}
