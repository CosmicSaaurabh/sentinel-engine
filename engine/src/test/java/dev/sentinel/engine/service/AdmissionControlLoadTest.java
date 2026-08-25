package dev.sentinel.engine.service;

import static org.assertj.core.api.Assertions.assertThat;

import dev.sentinel.engine.AbstractIntegrationTest;
import dev.sentinel.engine.domain.ClaimedTask;
import dev.sentinel.engine.domain.NewTask;
import dev.sentinel.engine.domain.NewWorkflow;
import dev.sentinel.engine.error.AdmissionRejectedException;
import dev.sentinel.engine.repository.CapacityCounterRepository;
import dev.sentinel.engine.repository.TaskRepository;
import dev.sentinel.engine.repository.WorkflowRepository;
import dev.sentinel.engine.support.Workflows;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

/**
 * Admission control under concurrency, which is the only condition it actually has to survive.
 *
 * <p>A sharded counter exists to spread writes. Whether it spreads them correctly is not visible
 * from a single-threaded test: the whole question is whether the aggregate stays right while many
 * transactions increment and decrement different shards at once.
 */
@TestPropertySource(properties = {
        "sentinel.admission.total-slots=20",
        "spring.datasource.hikari.maximum-pool-size=30",
        "spring.datasource.hikari.minimum-idle=30"
})
class AdmissionControlLoadTest extends AbstractIntegrationTest {

    private static final int WORKERS = 20;
    private static final int ROUNDS_PER_WORKER = 25;

    @Autowired
    private WorkflowSubmissionService submissionService;

    @Autowired
    private TaskClaimService claimService;

    @Autowired
    private TaskCompletionService completionService;

    @Autowired
    private CapacityCounterRepository capacity;

    @Autowired
    private WorkflowRepository workflows;

    @Autowired
    private TaskRepository tasks;

    private final ExecutorService fleet = Executors.newFixedThreadPool(WORKERS, runnable -> {
        Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        return thread;
    });

    @AfterEach
    void shutdownFleet() throws InterruptedException {
        fleet.shutdownNow();
        assertThat(fleet.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    @DisplayName("the counter sum stays accurate under many simultaneous claims and completions")
    void theCounterSurvivesConcurrentTraffic() throws Exception {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("counter-load")).id();
        tasks.insertAll(workflowId, java.util.stream.IntStream.range(0, WORKERS * ROUNDS_PER_WORKER)
                .mapToObj(i -> NewTask.runnable("task-" + i, Workflows.TASK_TYPE, "{}"))
                .toList());

        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(WORKERS);
        AtomicInteger completed = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();

        for (int i = 0; i < WORKERS; i++) {
            String workerId = "worker-" + i;
            fleet.execute(() -> {
                try {
                    startGate.await();
                    for (int round = 0; round < ROUNDS_PER_WORKER; round++) {
                        List<ClaimedTask> claimed =
                                claimService.claim(new ClaimRequest(workerId, List.of(Workflows.TASK_TYPE), 1));
                        for (ClaimedTask task : claimed) {
                            completionService.complete(task.id(), workerId, task.fencingToken(), "{}");
                            completed.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (RuntimeException e) {
                    errors.incrementAndGet();
                } finally {
                    finished.countDown();
                }
            });
        }

        startGate.countDown();
        assertThat(finished.await(2, TimeUnit.MINUTES)).isTrue();

        assertThat(errors.get()).isZero();
        assertThat(completed.get()).isPositive();

        long stillRunning = jdbcClient.sql("SELECT count(*) FROM tasks WHERE status = 'RUNNING'")
                .query(Long.class)
                .single();
        assertThat(capacity.totalInFlight())
                .as("every claim incremented a shard and every completion decremented one, "
                        + "possibly a different one, and only the total has to agree")
                .isEqualTo(stillRunning);
    }

    @Test
    @DisplayName("submissions above capacity are refused cleanly, leaving nothing half-written")
    void submissionsAboveCapacityAreRefusedCleanly() throws Exception {
        // Fill every slot, so every submission below is refused.
        capacity.addInFlight(0, 20);

        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(WORKERS);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        AtomicInteger unexpected = new AtomicInteger();

        for (int i = 0; i < WORKERS; i++) {
            fleet.execute(() -> {
                try {
                    startGate.await();
                    submissionService.submit(Workflows.diamond());
                    accepted.incrementAndGet();
                } catch (AdmissionRejectedException e) {
                    rejected.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (RuntimeException e) {
                    unexpected.incrementAndGet();
                } finally {
                    finished.countDown();
                }
            });
        }

        startGate.countDown();
        assertThat(finished.await(1, TimeUnit.MINUTES)).isTrue();

        assertThat(unexpected.get()).isZero();
        assertThat(rejected.get()).isEqualTo(WORKERS);
        assertThat(accepted.get()).isZero();

        assertThat(jdbcClient.sql("SELECT count(*) FROM workflows").query(Long.class).single())
                .as("a refused submission must leave no trace; an accepted-but-stalled workflow is "
                        + "the exact outcome refusing outright exists to prevent")
                .isZero();
        assertThat(jdbcClient.sql("SELECT count(*) FROM tasks").query(Long.class).single()).isZero();
    }

    @Test
    @DisplayName("capacity frees up as work completes, and submissions resume")
    void capacityRecoversAsWorkCompletes() {
        capacity.addInFlight(0, 20);
        assertThat(org.assertj.core.api.Assertions.catchThrowableOfType(
                AdmissionRejectedException.class, () -> submissionService.submit(Workflows.singleTask())))
                .isNotNull();

        capacity.addInFlight(3, -20);

        assertThat(submissionService.submit(Workflows.singleTask()).workflow())
                .as("saturation is a passing condition, not a poisoned engine")
                .isNotNull();
    }
}
