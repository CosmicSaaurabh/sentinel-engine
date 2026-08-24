package dev.sentinel.engine.repository;

import static org.assertj.core.api.Assertions.assertThat;

import dev.sentinel.engine.AbstractIntegrationTest;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** Worker liveness, which is observability rather than correctness. */
class WorkerRepositoryTest extends AbstractIntegrationTest {

    @Autowired
    private WorkerRepository workers;

    @Test
    @DisplayName("registering twice updates rather than duplicates")
    void registrationIsUpsert() {
        workers.registerOrHeartbeat("worker-1", List.of("type.alpha"), 4);
        workers.registerOrHeartbeat("worker-1", List.of("type.alpha", "type.beta"), 8);

        assertThat(jdbcClient.sql("SELECT count(*) FROM workers").query(Integer.class).single()).isEqualTo(1);
        assertThat(jdbcClient.sql("SELECT max_concurrency FROM workers WHERE id = 'worker-1'")
                .query(Integer.class)
                .single())
                .isEqualTo(8);
        assertThat(jdbcClient.sql("SELECT array_length(task_types, 1) FROM workers WHERE id = 'worker-1'")
                .query(Integer.class)
                .single())
                .isEqualTo(2);
    }

    @Test
    @DisplayName("a worker that stopped heartbeating is reported stale")
    void staleWorkersAreDetected() {
        workers.registerOrHeartbeat("silent", List.of("type.alpha"), 1);
        workers.registerOrHeartbeat("chatty", List.of("type.alpha"), 1);
        jdbcClient.sql("UPDATE workers SET last_heartbeat_at = now() - interval '5 minutes' WHERE id = 'silent'")
                .update();

        assertThat(workers.findStale(Duration.ofMinutes(1))).containsExactly("silent");
        assertThat(workers.countAlive(Duration.ofMinutes(1))).isEqualTo(1);
    }

    @Test
    @DisplayName("stale registrations are prunable without touching any task state")
    void staleWorkersArePrunable() {
        workers.registerOrHeartbeat("silent", List.of("type.alpha"), 1);
        jdbcClient.sql("UPDATE workers SET last_heartbeat_at = now() - interval '5 minutes' WHERE id = 'silent'")
                .update();

        assertThat(workers.deleteStale(Duration.ofMinutes(1))).isEqualTo(1);
        assertThat(workers.countAlive(Duration.ofDays(1))).isZero();
    }
}
