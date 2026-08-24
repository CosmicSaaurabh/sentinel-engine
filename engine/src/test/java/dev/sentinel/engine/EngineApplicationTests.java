package dev.sentinel.engine;

import static org.assertj.core.api.Assertions.assertThat;

import dev.sentinel.engine.infra.DatabaseClock;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** The context starts, the schema is present, and time comes from where it should. */
class EngineApplicationTests extends AbstractIntegrationTest {

    @Autowired
    private DatabaseClock databaseClock;

    @Test
    @DisplayName("every migration applies against a real Postgres")
    void migrationsApplyAgainstRealPostgres() {
        Integer applied = jdbcClient.sql("SELECT count(*) FROM flyway_schema_history WHERE success = true")
                .query(Integer.class)
                .single();

        assertThat(applied).isEqualTo(2);
    }

    @Test
    @DisplayName("the engine reads wall-clock time from the database, not from the JVM")
    void clockComesFromTheDatabase() {
        Instant fromDatabase = databaseClock.now();

        assertThat(fromDatabase).isCloseTo(Instant.now(), org.assertj.core.api.Assertions.within(
                Duration.ofMinutes(1)));
    }
}
