package dev.sentinel.engine.service;

import static org.assertj.core.api.Assertions.assertThat;

import dev.sentinel.engine.AbstractIntegrationTest;
import dev.sentinel.engine.domain.ClaimedTask;
import dev.sentinel.engine.domain.NewTask;
import dev.sentinel.engine.domain.NewWorkflow;
import dev.sentinel.engine.domain.OutboxEvent;
import dev.sentinel.engine.domain.OutboxEventType;
import dev.sentinel.engine.infra.ClaimProperties;
import dev.sentinel.engine.repository.CapacityCounterRepository;
import dev.sentinel.engine.repository.OutboxRepository;
import dev.sentinel.engine.repository.TaskRepository;
import dev.sentinel.engine.repository.WorkflowRepository;
import dev.sentinel.engine.support.DagFixtures;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** What the claim service does around the claim statement, and inside which transaction. */
class TaskClaimServiceTest extends AbstractIntegrationTest {

    @Autowired
    private TaskClaimService claimService;

    @Autowired
    private WorkflowRepository workflows;

    @Autowired
    private TaskRepository tasks;

    @Autowired
    private OutboxRepository outbox;

    @Autowired
    private CapacityCounterRepository capacity;

    @Autowired
    private ClaimProperties properties;

    @Test
    @DisplayName("a claim returns the task with a fencing token and a lease")
    void claimReturnsFencedTasks() {
        var dag = DagFixtures.singleTask(workflows, tasks, "only");

        List<ClaimedTask> claimed = claimService.claim(request("worker-1", 10));

        assertThat(claimed).hasSize(1);
        assertThat(claimed.getFirst().id()).isEqualTo(dag.taskId("only"));
        assertThat(claimed.getFirst().fencingToken()).isEqualTo(1L);
        assertThat(claimed.getFirst().leaseExpiresAt()).isNotNull();
    }

    @Test
    @DisplayName("a worker asking for more than the engine allows is clamped, not rejected")
    void oversizedRequestsAreClamped() {
        seedTasks(properties.maxBatchSize() + 10);

        List<ClaimedTask> claimed = claimService.claim(request("greedy-worker", 10_000));

        assertThat(claimed)
                .as("a worker cannot service more leases than it has threads, so the engine caps it")
                .hasSize(properties.maxBatchSize());
    }

    @Test
    @DisplayName("an empty poll is an ordinary outcome and touches nothing")
    void emptyPollIsOrdinary() {
        assertThat(claimService.claim(request("idle-worker", 10))).isEmpty();
        assertThat(capacity.totalInFlight()).isZero();
        assertThat(outbox.claimUnrelayed(10)).isEmpty();
    }

    @Test
    @DisplayName("a worker that declares no task types gets an empty result without querying")
    void noDeclaredTypesReturnsEmpty() {
        DagFixtures.singleTask(workflows, tasks, "only");

        assertThat(claimService.claim(new ClaimRequest("typeless", List.of(), 10))).isEmpty();
    }

    @Test
    @DisplayName("claiming records a history event per task, atomically with the claim")
    void claimingWritesHistory() {
        var dag = DagFixtures.singleTask(workflows, tasks, "only");

        claimService.claim(request("worker-1", 10));

        List<OutboxEvent> events = outbox.findByWorkflowId(dag.workflowId());
        assertThat(events).hasSize(1);
        assertThat(events.getFirst().eventType()).isEqualTo(OutboxEventType.TASK_CLAIMED);
        assertThat(events.getFirst().taskId()).isEqualTo(dag.taskId("only"));
        assertThat(events.getFirst().payload()).contains("worker-1");
    }

    @Test
    @DisplayName("claiming raises the in-flight counter that admission control reads")
    void claimingRaisesTheInFlightCounter() {
        seedTasks(5);

        claimService.claim(request("worker-1", 5));

        assertThat(capacity.totalInFlight()).isEqualTo(5);
    }

    @Test
    @DisplayName("the first claim records when its workflow actually started")
    void firstClaimRecordsWorkflowStart() {
        var dag = DagFixtures.singleTask(workflows, tasks, "only");
        assertThat(workflows.findById(dag.workflowId()).orElseThrow().startedAt()).isNull();

        claimService.claim(request("worker-1", 10));

        assertThat(workflows.findById(dag.workflowId()).orElseThrow().startedAt()).isNotNull();
    }

    @Test
    @DisplayName("two workers polling the same queue never receive the same task")
    void twoWorkersNeverShareATask() {
        seedTasks(20);

        List<ClaimedTask> first = claimService.claim(request("worker-1", 10));
        List<ClaimedTask> second = claimService.claim(request("worker-2", 10));

        assertThat(first).hasSize(10);
        assertThat(second).hasSize(10);
        assertThat(first.stream().map(ClaimedTask::id))
                .doesNotContainAnyElementsOf(second.stream().map(ClaimedTask::id).toList());
    }

    @Test
    @DisplayName("the claim leaves nothing half-written when it finds nothing")
    void nothingIsWrittenWhenNothingIsClaimed() {
        var dag = DagFixtures.singleTask(workflows, tasks, "only");
        claimService.claim(request("worker-1", 10));
        long inFlightAfterFirst = capacity.totalInFlight();

        claimService.claim(request("worker-2", 10));

        assertThat(capacity.totalInFlight()).isEqualTo(inFlightAfterFirst);
        assertThat(outbox.findByWorkflowId(dag.workflowId())).hasSize(1);
    }

    private ClaimRequest request(String workerId, int batchSize) {
        return new ClaimRequest(workerId, List.of(DagFixtures.TASK_TYPE), batchSize);
    }

    private UUID seedTasks(int count) {
        UUID workflowId = workflows.insert(NewWorkflow.immediate("seed")).id();
        tasks.insertAll(workflowId, IntStream.range(0, count)
                .mapToObj(i -> NewTask.runnable("task-" + i, DagFixtures.TASK_TYPE, "{}"))
                .toList());
        return workflowId;
    }
}
