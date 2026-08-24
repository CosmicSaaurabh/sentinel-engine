package dev.sentinel.engine.service;

import static org.assertj.core.api.Assertions.assertThat;

import dev.sentinel.engine.AbstractIntegrationTest;
import dev.sentinel.engine.domain.ClaimedTask;
import dev.sentinel.engine.domain.Task;
import dev.sentinel.engine.domain.TaskStatus;
import dev.sentinel.engine.domain.Workflow;
import dev.sentinel.engine.support.Workflows;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Two parents of the same child completing at the same instant.
 *
 * <p>This is the race the dependency counter is most exposed to, and it has two distinct ways to go
 * wrong.
 *
 * <p>The first is a lost update. Both transactions read the child's blocker count as two, both
 * write one, and the child is left permanently blocked with one phantom blocker that will never be
 * removed. Nothing errors, and there is no repair process, because nothing downstream can
 * distinguish a stuck counter from a task that is legitimately still waiting. The decrement being a
 * single {@code UPDATE} that reads and writes in one statement, under a row lock, is what prevents
 * this.
 *
 * <p>The second is a deadlock. In a wider fan-out, two transactions touching an overlapping set of
 * children in different orders can each hold what the other needs. Postgres detects that and kills
 * one transaction, turning an ordinary completion into a spurious failure. Locking children in
 * ascending id order gives every transaction the same acquisition order, which makes the cycle
 * impossible to form rather than merely unlikely.
 *
 * <p>Both failures are timing-dependent, so this runs many rounds with the two completions released
 * from a barrier rather than trusting one attempt to hit the window.
 */
class ConcurrentCompletionTest extends AbstractIntegrationTest {

    private static final int ROUNDS = 25;

    @Autowired
    private WorkflowSubmissionService submissionService;

    @Autowired
    private TaskClaimService claimService;

    @Autowired
    private TaskCompletionService completionService;

    @Autowired
    private dev.sentinel.engine.repository.TaskRepository tasks;

    private final ExecutorService completers = Executors.newFixedThreadPool(2, runnable -> {
        Thread thread = new Thread(runnable);
        thread.setName("completer-" + thread.threadId());
        thread.setDaemon(true);
        return thread;
    });

    @AfterEach
    void shutdownCompleters() throws InterruptedException {
        completers.shutdownNow();
        assertThat(completers.awaitTermination(10, TimeUnit.SECONDS))
                .as("no test-owned thread may outlive the test")
                .isTrue();
    }

    @Test
    @DisplayName("both parents completing at once unblocks the shared child exactly once")
    void simultaneousParentsDoNotRaceOnTheirSharedChild() throws Exception {
        AtomicInteger failures = new AtomicInteger();

        for (int round = 0; round < ROUNDS; round++) {
            Workflow workflow = submissionService.submit(Workflows.diamond());
            completeAll(claim(10));

            List<ClaimedTask> branches = claim(10);
            assertThat(branches).hasSize(2);

            CyclicBarrier bothReady = new CyclicBarrier(2);
            List<Future<?>> completions = new java.util.ArrayList<>();
            for (ClaimedTask branch : branches) {
                completions.add(completers.submit(() -> {
                    try {
                        bothReady.await(10, TimeUnit.SECONDS);
                        completionService.complete(branch.id(), "worker-1", branch.fencingToken(), "{}");
                    } catch (Exception e) {
                        failures.incrementAndGet();
                        throw new IllegalStateException("completion failed in round", e);
                    }
                }));
            }

            for (Future<?> completion : completions) {
                completion.get(30, TimeUnit.SECONDS);
            }

            Task join = taskNamed(workflow, "join");
            assertThat(join.pendingDependencies())
                    .as("a lost decrement would leave a phantom blocker that nothing ever removes")
                    .isZero();
            assertThat(join.status())
                    .as("the child must become runnable exactly once, not twice and not never")
                    .isEqualTo(TaskStatus.PENDING);

            resetDatabase();
        }

        assertThat(failures.get())
                .as("a deadlock would surface here as a killed transaction, not as a wrong result")
                .isZero();
    }

    private List<ClaimedTask> claim(int batchSize) {
        return claimService.claim(new ClaimRequest("worker-1", List.of(Workflows.TASK_TYPE), batchSize));
    }

    private void completeAll(List<ClaimedTask> claimed) {
        claimed.forEach(task -> completionService.complete(task.id(), "worker-1", task.fencingToken(), "{}"));
    }

    private Task taskNamed(Workflow workflow, String name) {
        return tasks.findByWorkflowId(workflow.id()).stream()
                .filter(task -> task.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no task named " + name));
    }
}
