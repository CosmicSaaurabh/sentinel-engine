package dev.sentinel.engine.repository.support;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/** Conversions between domain values and what the JDBC driver expects. */
public final class SqlValues {

    private SqlValues() {
    }

    /**
     * Binds an instant as a {@code timestamptz} parameter.
     *
     * <p>Nulls are passed through deliberately: every SQL statement that accepts an optional
     * instant wraps it in {@code coalesce(..., now())}, so a null means "use the database clock"
     * rather than "use the caller's clock".
     */
    public static OffsetDateTime timestamp(Instant instant) {
        return instant == null ? null : OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
