package dev.sentinel.engine;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

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
        jdbcClient.sql("TRUNCATE workflows, tasks, task_dependencies, outbox, workers CASCADE").update();
        jdbcClient.sql("UPDATE capacity_counters SET in_flight = 0").update();
    }
}
