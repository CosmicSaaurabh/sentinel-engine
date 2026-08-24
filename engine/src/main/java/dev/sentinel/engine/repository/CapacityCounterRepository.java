package dev.sentinel.engine.repository;

import java.util.List;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * The sharded in-flight counter behind admission control.
 *
 * <p>Why sharded rather than one row or one {@code COUNT(*)}:
 *
 * <p>A single counter row would be updated on every claim and every completion, which at the
 * engine's write rate makes it a hot row that every transaction queues behind. That is the same
 * failure the task queue avoids with {@code SKIP LOCKED}, reappearing in a different place.
 *
 * <p>A live {@code COUNT(*)} over in-flight tasks needs no counter at all, which is genuinely
 * simpler, but its cost grows with the backlog. It would get slower exactly when the backlog is
 * largest, which is precisely the moment admission control has to stay fast. That is a
 * stress-amplifying feedback loop, so it loses despite being simpler.
 *
 * <p>Spreading the writes across N rows removes the hot row, and the read stays constant-time
 * because it sums a small fixed number of rows.
 *
 * <p>This counter is a heuristic, not an invariant. An engine that dies between claiming a task and
 * updating the counter leaves it slightly wrong, and the consequence is admitting a little too much
 * or too little work, never an incorrect task state. It is repaired by periodic reconciliation
 * rather than defended with locks.
 */
@Repository
public class CapacityCounterRepository {

    private final JdbcClient jdbcClient;

    public CapacityCounterRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    /**
     * Total tasks currently counted as in flight.
     *
     * <p>One statement, deliberately. Reading the shards with N separate queries and summing them
     * in Java would observe N different database snapshots and could produce a total that never
     * existed at any instant.
     */
    public long totalInFlight() {
        return jdbcClient.sql("SELECT GREATEST(0, coalesce(sum(in_flight), 0)) FROM capacity_counters")
                .query(Long.class)
                .single();
    }

    public int shardCount() {
        return jdbcClient.sql("SELECT count(*) FROM capacity_counters").query(Integer.class).single();
    }

    public List<Integer> shardIds() {
        return jdbcClient.sql("SELECT shard_id FROM capacity_counters ORDER BY shard_id")
                .query(Integer.class)
                .list();
    }

    /**
     * Adjusts one shard's counter.
     *
     * <p>A claim increments and a completion decrements, and there is no requirement that a
     * completion return to the shard its claim touched, because only the sum is ever read. That
     * freedom is exactly why an individual shard is allowed to go negative: routing a decrement to
     * an untouched shard is normal, not an error, and rejecting it would break the property that
     * makes the counter cheap. The clamp lives on {@link #totalInFlight()} instead, where drift can
     * be absorbed without discarding a completion that really happened.
     */
    public void addInFlight(int shardId, long delta) {
        jdbcClient.sql("""
                UPDATE capacity_counters
                   SET in_flight = in_flight + :delta, updated_at = now()
                 WHERE shard_id = :shardId
                """)
                .param("shardId", shardId)
                .param("delta", delta)
                .update();
    }

    /**
     * Picks a shard for a given key.
     *
     * <p>Hashing on the worker id rather than choosing at random keeps one worker's traffic on one
     * shard, which is friendlier to page caching, and it is deterministic, which makes tests
     * reproducible.
     */
    public int shardFor(String key, int shardCount) {
        return Math.floorMod(key.hashCode(), shardCount);
    }

    /**
     * Rewrites the counters from the authoritative task rows.
     *
     * <p>This is the drift repair the class comment refers to. It is scheduled maintenance, never
     * part of the hot path, and it is the reason the counter is allowed to be approximate in the
     * first place.
     */
    public long reconcileFromTasks() {
        long actual = jdbcClient.sql("SELECT count(*) FROM tasks WHERE status = 'RUNNING'")
                .query(Long.class)
                .single();
        List<Integer> shards = shardIds();
        long perShard = actual / shards.size();
        long remainder = actual % shards.size();

        for (int i = 0; i < shards.size(); i++) {
            long value = perShard + (i < remainder ? 1 : 0);
            jdbcClient.sql("UPDATE capacity_counters SET in_flight = :value, updated_at = now() WHERE shard_id = :shardId")
                    .param("value", value)
                    .param("shardId", shards.get(i))
                    .update();
        }
        return actual;
    }
}
