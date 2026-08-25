package dev.sentinel.engine.infra;

import dev.sentinel.engine.domain.TaskStatus;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/**
 * Queue depth, refreshed on a timer rather than computed on scrape.
 *
 * <h2>Why not a plain gauge over a query</h2>
 *
 * <p>Micrometer evaluates a gauge every time Prometheus scrapes it. A gauge that runs
 * {@code SELECT count(*) ... GROUP BY status} would therefore put a full aggregate over the
 * highest-churn table in the system on the scrape path, several times a minute, from every engine
 * instance at once. Worse, its cost grows with the table, so the monitoring gets more expensive
 * exactly as the system gets busier, and a scrape storm during an incident would add load precisely
 * when there is least to spare.
 *
 * <p>Refreshing into an {@link AtomicLong} on our own schedule decouples the two: the query runs at
 * a rate we choose, and a scrape reads a number out of memory. The cost is that the value can be a
 * few seconds stale, which for a queue-depth trend is not a cost at all.
 *
 * <h2>Label cardinality</h2>
 *
 * <p>Labelled by status only, which is six values forever. It is tempting to add task type, and for
 * a handful of types that would be fine, but nothing bounds how many task types a deployment
 * defines, and an unbounded label is how a Prometheus instance dies. Per-type detail belongs in
 * counters where the operator opts into the cardinality knowingly.
 */
@Component
public class QueueMetrics {

    private static final Logger log = LoggerFactory.getLogger(QueueMetrics.class);
    private static final long REFRESH_SECONDS = 10;

    private final JdbcClient jdbcClient;
    private final Map<TaskStatus, AtomicLong> depthByStatus = new EnumMap<>(TaskStatus.class);
    private final AtomicLong claimableNow = new AtomicLong();
    private final ScheduledExecutorService refresher;

    public QueueMetrics(JdbcClient jdbcClient, MeterRegistry meterRegistry) {
        this.jdbcClient = jdbcClient;
        this.refresher = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "sentinel-queue-metrics");
            thread.setDaemon(true);
            return thread;
        });

        for (TaskStatus status : TaskStatus.values()) {
            AtomicLong depth = new AtomicLong();
            depthByStatus.put(status, depth);
            Gauge.builder("sentinel.queue.depth", depth, AtomicLong::get)
                    .description("tasks in each state")
                    .tag("status", status.name())
                    .register(meterRegistry);
        }

        // Distinct from the PENDING depth, and the difference is the point. A task can be PENDING
        // but not yet claimable because it is waiting out a retry backoff, so a queue that looks
        // deep while nothing is claimable means the fleet is in a retry storm rather than behind.
        Gauge.builder("sentinel.queue.claimable", claimableNow, AtomicLong::get)
                .description("tasks claimable right now, which excludes those waiting out a backoff")
                .register(meterRegistry);

    }

    /**
     * Starts refreshing once the application is up.
     *
     * <p>Not in the constructor, for two reasons. The first is correctness: the first refresh
     * queries the database, and running before migrations and the connection pool are ready would
     * produce a burst of startup errors that mean nothing. The second is that publishing {@code
     * this} to another thread from inside a constructor lets that thread observe a partly built
     * object, which is a real hazard rather than a compiler nag.
     */
    @org.springframework.context.event.EventListener(
            org.springframework.boot.context.event.ApplicationReadyEvent.class)
    public void start() {
        refresher.scheduleWithFixedDelay(
                this::refreshQuietly, 0, REFRESH_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Refreshes, absorbing failures.
     *
     * <p>A scheduled task that throws is silently cancelled and never runs again, which here would
     * mean the queue-depth gauges freezing at their last value. Frozen metrics are worse than
     * missing ones: a flat line reads as a healthy steady state.
     */
    private void refreshQuietly() {
        try {
            refresh();
        } catch (RuntimeException e) {
            log.warn("could not refresh queue metrics; gauges will be stale until the next tick", e);
        }
    }

    /** Package-private so a test can drive it rather than wait ten seconds. */
    void refresh() {
        depthByStatus.values().forEach(depth -> depth.set(0));

        jdbcClient.sql("SELECT status, count(*) AS n FROM tasks GROUP BY status")
                .query((rs, rowNum) -> Map.entry(TaskStatus.valueOf(rs.getString("status")), rs.getLong("n")))
                .list()
                .forEach(entry -> depthByStatus.get(entry.getKey()).set(entry.getValue()));

        claimableNow.set(jdbcClient.sql("""
                SELECT count(*) FROM tasks
                 WHERE status = 'PENDING' AND next_attempt_at <= now() AND attempt < max_attempts
                """)
                .query(Long.class)
                .single());
    }

    long depthOf(TaskStatus status) {
        return depthByStatus.get(status).get();
    }

    long claimable() {
        return claimableNow.get();
    }

    @PreDestroy
    public void stop() {
        refresher.shutdownNow();
    }
}
