package dev.sentinel.engine.infra;

import java.time.Instant;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/**
 * The engine's only source of wall-clock time.
 *
 * <p>Every lease deadline, retry delay and scheduled trigger in this system is computed by Postgres
 * with {@code now()}, never by a JVM. Engine instances and workers run on different machines whose
 * clocks drift, and lease expiry is a comparison between "when this lease ends" and "what time is
 * it now". If those two came from different clocks, skew would show up as tasks reaped while their
 * worker is healthy, or zombies left running past their deadline.
 *
 * <p>This component exists so that code needing to reason about time has an honest way to ask, and
 * so that reaching for {@code Instant.now()} looks as wrong as it is. It costs a round trip, so it
 * belongs in tests and in slow paths, never inside the claim or heartbeat path, which get their
 * time inside the SQL statement itself.
 */
@Component
public class DatabaseClock {

    private final JdbcClient jdbcClient;

    public DatabaseClock(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    public Instant now() {
        return jdbcClient.sql("SELECT now()")
                .query(java.time.OffsetDateTime.class)
                .single()
                .toInstant();
    }
}
