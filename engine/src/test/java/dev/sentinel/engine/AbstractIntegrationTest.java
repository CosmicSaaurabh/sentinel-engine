package dev.sentinel.engine;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;

/**
 * Base for tests that need a real Postgres.
 *
 * <p>A real database rather than a mock or an in-memory substitute, because almost everything this
 * layer asserts is a genuine Postgres behaviour: {@code SKIP LOCKED} semantics, {@code CHECK}
 * constraints, partial indexes, transactional visibility. A fake would happily agree with code that
 * Postgres would reject.
 *
 * <p>The container is shared across every test class through Spring's context cache, so the cost is
 * paid once per build rather than once per class.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = {
        // The reaper is off by default in tests, and on in production.
        //
        // Recovery is a background process that changes task state on a timer, so leaving it
        // running would make every test that parks a task in RUNNING race against it. That is not a
        // flaky test to be tolerated; it is a test asserting on state something else is concurrently
        // rewriting, which can only ever be resolved by luck.
        //
        // Tests that are about recovery call ReaperService.reapOnce() directly, which is why that
        // method exists separately from the schedule. ReaperSchedulerTest turns the schedule back
        // on to prove the wiring works.
        "sentinel.reaper.enabled=false"
})
public abstract class AbstractIntegrationTest {

    @Autowired
    protected JdbcClient jdbcClient;

    /**
     * Truncating between tests rather than rolling back a transaction is deliberate: several of
     * these tests are about what concurrent transactions see of each other, which a test-owned
     * outer transaction would hide.
     */
    @BeforeEach
    protected void resetDatabase() {
        jdbcClient.sql(
                "TRUNCATE workflows, tasks, task_dependencies, task_failures, outbox, workers CASCADE")
                .update();
        jdbcClient.sql("UPDATE capacity_counters SET in_flight = 0").update();
    }
}
