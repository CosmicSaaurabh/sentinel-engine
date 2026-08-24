package dev.sentinel.engine.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.sentinel.engine.AbstractIntegrationTest;
import dev.sentinel.engine.domain.FailureKind;
import dev.sentinel.engine.domain.NewWorkflow;
import dev.sentinel.engine.domain.TaskStatus;
import dev.sentinel.engine.domain.WorkflowStatus;
import dev.sentinel.engine.repository.WorkflowRepository;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Guards the seam between the Java enums and the database's {@code CHECK} constraints.
 *
 * <p>This seam is stringly typed by necessity: a status is an enum in Java and text in Postgres.
 * Adding a value on one side and forgetting the other produces a failure that only appears when
 * that particular status is first written, which in practice means in production rather than in a
 * test. These tests turn that into a compile-then-fail-immediately problem instead.
 */
class SchemaConstraintsTest extends AbstractIntegrationTest {

    private static final Pattern QUOTED_LITERAL = Pattern.compile("'([A-Z_]+)'");

    @Autowired
    private WorkflowRepository workflows;

    @Test
    @DisplayName("both migrations applied successfully")
    void migrationsApplied() {
        Integer applied = jdbcClient.sql("SELECT count(*) FROM flyway_schema_history WHERE success = true")
                .query(Integer.class)
                .single();
        assertThat(applied).isEqualTo(2);
    }

    @Test
    @DisplayName("tasks_status_check accepts exactly the TaskStatus values")
    void taskStatusEnumMatchesCheckConstraint() {
        assertThat(literalsIn("tasks_status_check"))
                .containsExactlyInAnyOrderElementsOf(names(TaskStatus.values()));
    }

    @Test
    @DisplayName("workflows_status_check accepts exactly the WorkflowStatus values")
    void workflowStatusEnumMatchesCheckConstraint() {
        assertThat(literalsIn("workflows_status_check"))
                .containsExactlyInAnyOrderElementsOf(names(WorkflowStatus.values()));
    }

    @Test
    @DisplayName("tasks_error_kind_check accepts exactly the FailureKind values")
    void failureKindEnumMatchesCheckConstraint() {
        assertThat(literalsIn("tasks_error_kind_check"))
                .containsExactlyInAnyOrderElementsOf(names(FailureKind.values()));
    }

    @Test
    @DisplayName("a RUNNING task without an owner is rejected by the database, not merely by convention")
    void leaseConsistencyIsEnforced() {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("lease-check")).id();

        assertThatThrownBy(() -> jdbcClient.sql("""
                INSERT INTO tasks (workflow_id, name, task_type, status)
                VALUES (:wf, 'orphan-running', 'test.echo', 'RUNNING')
                """)
                .param("wf", workflowId)
                .update())
                .hasMessageContaining("tasks_lease_consistency");
    }

    @Test
    @DisplayName("a completed task may not keep a lease")
    void terminalTaskMayNotHoldALease() {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("lease-check")).id();

        assertThatThrownBy(() -> jdbcClient.sql("""
                INSERT INTO tasks (workflow_id, name, task_type, status, owner_worker_id, lease_expires_at)
                VALUES (:wf, 'completed-with-lease', 'test.echo', 'COMPLETED', 'w1', now())
                """)
                .param("wf", workflowId)
                .update())
                .hasMessageContaining("tasks_lease_consistency");
    }

    @Test
    @DisplayName("attempt may never exceed max_attempts")
    void attemptBoundIsEnforced() {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("attempts")).id();

        assertThatThrownBy(() -> jdbcClient.sql("""
                INSERT INTO tasks (workflow_id, name, task_type, status, attempt, max_attempts)
                VALUES (:wf, 'over-budget', 'test.echo', 'PENDING', 4, 3)
                """)
                .param("wf", workflowId)
                .update())
                .hasMessageContaining("tasks_attempt_bounds");
    }

    @Test
    @DisplayName("a task type outside the permitted character set is rejected")
    void taskTypeFormatIsEnforced() {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("routing")).id();

        assertThatThrownBy(() -> jdbcClient.sql("""
                INSERT INTO tasks (workflow_id, name, task_type, status)
                VALUES (:wf, 'bad-type', 'has,comma', 'PENDING')
                """)
                .param("wf", workflowId)
                .update())
                .hasMessageContaining("tasks_task_type_format");
    }

    @Test
    @DisplayName("two tasks in one workflow may not share a name")
    void taskNamesAreUniquePerWorkflow() {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("names")).id();
        jdbcClient.sql("INSERT INTO tasks (workflow_id, name, task_type, status) VALUES (:wf, 'a', 'test.echo', 'PENDING')")
                .param("wf", workflowId)
                .update();

        assertThatThrownBy(() -> jdbcClient.sql(
                "INSERT INTO tasks (workflow_id, name, task_type, status) VALUES (:wf, 'a', 'test.echo', 'PENDING')")
                .param("wf", workflowId)
                .update())
                .hasMessageContaining("tasks_name_uq");
    }

    @Test
    @DisplayName("the claim index is partial on PENDING, so its size tracks queue depth not table size")
    void claimIndexIsPartial() {
        String definition = jdbcClient.sql("SELECT indexdef FROM pg_indexes WHERE indexname = 'tasks_claimable_idx'")
                .query(String.class)
                .single();
        assertThat(definition).contains("WHERE (status = 'PENDING'::text)");
    }

    @Test
    @DisplayName("no index covers lease_expires_at, which is what keeps heartbeats HOT-eligible")
    void leaseColumnIsNotIndexed() {
        var indexesTouchingLease = jdbcClient.sql("""
                SELECT indexname FROM pg_indexes
                 WHERE tablename = 'tasks' AND indexdef LIKE '%lease_expires_at%'
                """)
                .query(String.class)
                .list();
        assertThat(indexesTouchingLease)
                .as("indexing lease_expires_at would add index maintenance to every heartbeat in the fleet")
                .isEmpty();
    }

    private Set<String> literalsIn(String constraintName) {
        String definition = jdbcClient.sql(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = :name")
                .param("name", constraintName)
                .query(String.class)
                .single();

        Matcher matcher = QUOTED_LITERAL.matcher(definition);
        return matcher.results().map(result -> result.group(1)).collect(Collectors.toSet());
    }

    private static Set<String> names(Enum<?>[] values) {
        return Arrays.stream(values).map(Enum::name).collect(Collectors.toSet());
    }
}
