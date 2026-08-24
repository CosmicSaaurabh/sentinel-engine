package dev.sentinel.engine.service;

import static org.assertj.core.api.Assertions.assertThat;

import dev.sentinel.engine.AbstractIntegrationTest;
import dev.sentinel.engine.domain.ClaimedTask;
import dev.sentinel.engine.domain.NewWorkflow;
import dev.sentinel.engine.repository.WorkflowRepository;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

/**
 * The central correctness claim of this engine, proven rather than asserted in a comment: with many
 * workers polling one queue at the same time, no task is ever handed to two of them, and no task is
 * lost.
 *
 * <p>This has to be a real test against a real Postgres with real threads. The property being
 * tested is a interaction between row locks, {@code SKIP LOCKED}, and transaction visibility, none
 * of which a mock reproduces and none of which single-threaded tests can observe.
 *
 * <p>The pool is widened for this test only. Fifty threads on the production pool of twenty would
 * be measuring connection queueing rather than claim correctness, and pool behaviour has its own
 * test in {@link ClaimPoolExhaustionTest}.
 */
@TestPropertySource(properties = {
        "spring.datasource.hikari.maximum-pool-size=50",
        "spring.datasource.hikari.minimum-idle=50"
})
class ClaimConcurrencyTest extends AbstractIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ClaimConcurrencyTest.class);

    private static final int TASK_COUNT = 100_000;
    private static final int CLAIMERS = 50;
    private static final int BATCH_SIZE = 32;
    private static final String TASK_TYPE = "bench.claim";
    private static final int EMPTY_POLLS_BEFORE_STOPPING = 5;

    @Autowired
    private TaskClaimService claimService;

    @Autowired
    private WorkflowRepository workflows;

    @Test
    @DisplayName("50 concurrent claimers over 100k tasks: no duplicates, no losses")
    void concurrentClaimersNeverDuplicateOrLoseATask() throws Exception {
        seedTasks();

        Set<UUID> claimedIds = ConcurrentHashMap.newKeySet();
        AtomicInteger duplicates = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(CLAIMERS);

        ExecutorService claimers = Executors.newFixedThreadPool(CLAIMERS, runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("claimer-" + thread.threadId());
            thread.setDaemon(true);
            return thread;
        });

        long startedAt;
        try {
            for (int i = 0; i < CLAIMERS; i++) {
                String workerId = "worker-" + i;
                claimers.execute(() -> {
                    try {
                        startGate.await();
                        drainQueue(workerId, claimedIds, duplicates);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (RuntimeException e) {
                        failures.incrementAndGet();
                        log.error("claimer {} failed", workerId, e);
                    } finally {
                        finished.countDown();
                    }
                });
            }

            startedAt = System.nanoTime();
            startGate.countDown();
            assertThat(finished.await(3, TimeUnit.MINUTES))
                    .as("claimers should drain 100k tasks well inside three minutes")
                    .isTrue();
        } finally {
            claimers.shutdownNow();
            assertThat(claimers.awaitTermination(30, TimeUnit.SECONDS))
                    .as("no claimer thread may outlive the test")
                    .isTrue();
        }

        long elapsedNanos = System.nanoTime() - startedAt;
        double claimsPerSecond = TASK_COUNT / (elapsedNanos / 1_000_000_000.0);
        log.info("claimed {} tasks with {} concurrent claimers in {} ms: {} claims/sec",
                TASK_COUNT, CLAIMERS, TimeUnit.NANOSECONDS.toMillis(elapsedNanos), Math.round(claimsPerSecond));

        assertThat(failures.get()).as("no claimer thread should have errored").isZero();
        assertThat(duplicates.get())
                .as("a task handed to two workers is the exact failure this engine exists to prevent")
                .isZero();
        assertThat(claimedIds).as("every task must be claimed exactly once").hasSize(TASK_COUNT);

        assertThat(countWithStatus("PENDING")).as("nothing may be left behind in the queue").isZero();
        assertThat(countWithStatus("RUNNING")).isEqualTo(TASK_COUNT);
    }

    /**
     * Claims until the queue looks empty several polls in a row.
     *
     * <p>Stopping on a single empty result would be wrong, and the reason is the point of this
     * whole mechanism: {@code SKIP LOCKED} legitimately returns nothing when every row a claimer
     * walked past was momentarily locked by someone else, even though the queue is far from empty.
     * A real worker treats an empty poll as "back off and ask again", never as "the queue is
     * drained".
     */
    private void drainQueue(String workerId, Set<UUID> claimedIds, AtomicInteger duplicates) {
        ClaimRequest request = new ClaimRequest(workerId, List.of(TASK_TYPE), BATCH_SIZE);
        int consecutiveEmptyPolls = 0;

        while (consecutiveEmptyPolls < EMPTY_POLLS_BEFORE_STOPPING && !Thread.currentThread().isInterrupted()) {
            List<ClaimedTask> claimed = claimService.claim(request);
            if (claimed.isEmpty()) {
                consecutiveEmptyPolls++;
                continue;
            }
            consecutiveEmptyPolls = 0;
            for (ClaimedTask task : claimed) {
                if (!claimedIds.add(task.id())) {
                    duplicates.incrementAndGet();
                }
            }
        }
    }

    private void seedTasks() {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("claim-benchmark")).id();
        jdbcClient.sql("""
                INSERT INTO tasks (workflow_id, name, task_type, status)
                SELECT :workflowId, 'task-' || g, :taskType, 'PENDING'
                  FROM generate_series(1, :count) g
                """)
                .param("workflowId", workflowId)
                .param("taskType", TASK_TYPE)
                .param("count", TASK_COUNT)
                .update();
        jdbcClient.sql("ANALYZE tasks").update();
    }

    private long countWithStatus(String status) {
        return jdbcClient.sql("SELECT count(*) FROM tasks WHERE status = :status")
                .param("status", status)
                .query(Long.class)
                .single();
    }
}
