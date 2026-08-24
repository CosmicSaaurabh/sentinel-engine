package dev.sentinel.engine.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.sentinel.engine.AbstractIntegrationTest;
import dev.sentinel.engine.repository.TaskRepository;
import dev.sentinel.engine.repository.WorkflowRepository;
import dev.sentinel.engine.support.DagFixtures;
import java.sql.Connection;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

/**
 * What happens when claimers outnumber the connection pool.
 *
 * <p>This is the failure mode that hides best. A claimer that cannot get a connection and waits
 * indefinitely looks perfectly healthy from the outside: no error, no log line, no failed request.
 * Latency simply climbs, and the pool becomes an invisible second queue that no metric points at.
 *
 * <p>The engine's answer is a short {@code connection-timeout}: an exhausted pool has to surface as
 * a fast failure the worker can back off from, not as an unbounded wait. Configuring that is easy
 * to do and just as easy to lose in a later config edit, which is why it is pinned here.
 */
@TestPropertySource(properties = {
        "spring.datasource.hikari.maximum-pool-size=1",
        "spring.datasource.hikari.minimum-idle=1",
        "spring.datasource.hikari.connection-timeout=500"
})
class ClaimPoolExhaustionTest extends AbstractIntegrationTest {

    @Autowired
    private TaskClaimService claimService;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private WorkflowRepository workflows;

    @Autowired
    private TaskRepository tasks;

    @Test
    @DisplayName("a claim with no connection available fails fast instead of waiting forever")
    void claimFailsFastWhenThePoolIsExhausted() throws Exception {
        DagFixtures.singleTask(workflows, tasks, "only");

        try (Connection hogged = dataSource.getConnection()) {
            assertThat(hogged.isValid(1)).isTrue();

            long startedAt = System.nanoTime();
            // The root cause is what matters and what is stable: Hikari gives up waiting and
            // throws SQLTransientConnectionException. Spring's wrapper around it differs by where
            // the connection was requested (starting a transaction, versus running a statement),
            // so asserting the wrapper would make this test brittle for no gain.
            assertThatThrownBy(() -> claimService.claim(
                    new ClaimRequest("blocked-worker", List.of(DagFixtures.TASK_TYPE), 10)))
                    .hasRootCauseInstanceOf(SQLTransientConnectionException.class);
            Duration waited = Duration.ofNanos(System.nanoTime() - startedAt);

            assertThat(waited)
                    .as("the worker must learn quickly enough to back off and try another engine instance")
                    .isLessThan(Duration.ofSeconds(5));
        }
    }

    @Test
    @DisplayName("claims resume normally once a connection is free again")
    void claimsRecoverWhenThePoolFreesUp() throws Exception {
        DagFixtures.singleTask(workflows, tasks, "only");

        try (Connection hogged = dataSource.getConnection()) {
            assertThat(hogged.isValid(1)).isTrue();
        }

        assertThat(claimService.claim(new ClaimRequest("worker-1", List.of(DagFixtures.TASK_TYPE), 10)))
                .as("pool exhaustion must be a transient condition, not a poisoned service")
                .hasSize(1);
    }
}
