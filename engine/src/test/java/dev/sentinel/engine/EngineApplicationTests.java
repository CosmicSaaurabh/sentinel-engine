package dev.sentinel.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class EngineApplicationTests {

    @Autowired
    private DataSource dataSource;

    @Test
    void flywayBaselineAppliedAgainstRealPostgres() {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        Integer appliedMigrations = jdbcTemplate.queryForObject(
                "select count(*) from flyway_schema_history where success = true",
                Integer.class);

        assertEquals(1, appliedMigrations);
    }
}
