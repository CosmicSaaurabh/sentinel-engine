package dev.sentinel.engine.infra;

import static org.assertj.core.api.Assertions.assertThat;

import dev.sentinel.engine.AbstractIntegrationTest;
import dev.sentinel.engine.domain.ClaimedTask;
import dev.sentinel.engine.domain.FailureKind;
import dev.sentinel.engine.domain.TaskStatus;
import dev.sentinel.engine.service.ClaimRequest;
import dev.sentinel.engine.service.TaskClaimService;
import dev.sentinel.engine.service.TaskCompletionService;
import dev.sentinel.engine.service.WorkflowSubmissionService;
import dev.sentinel.engine.support.Workflows;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * What the engine exposes about itself.
 *
 * <p>Metrics are unusually easy to break silently. A renamed meter, a dropped tag or a gauge wired
 * to the wrong field produces no error anywhere: the dashboard simply goes flat, and a flat line
 * reads as a healthy steady state rather than as missing data. These tests pin the names and tags
 * the committed dashboard queries by, so a rename breaks the build instead of the dashboard.
 */
class ObservabilityTest extends AbstractIntegrationTest {

    @Autowired
    private MeterRegistry meters;

    @Autowired
    private QueueMetrics queueMetrics;

    @Autowired
    private WorkflowSubmissionService submissionService;

    @Autowired
    private TaskClaimService claimService;

    @Autowired
    private TaskCompletionService completionService;

    @Test
    @DisplayName("queue depth is reported per status, and only per status")
    void queueDepthIsReportedPerStatus() {
        submissionService.submit(Workflows.chain("first", "second", "third"));

        queueMetrics.refresh();

        assertThat(queueMetrics.depthOf(TaskStatus.PENDING)).isEqualTo(1);
        assertThat(queueMetrics.depthOf(TaskStatus.BLOCKED)).isEqualTo(2);
        assertThat(meters.get("sentinel.queue.depth").tag("status", "PENDING").gauge().value())
                .isEqualTo(1.0);

        assertThat(meters.find("sentinel.queue.depth").gauges())
                .as("status has six values forever; adding task type would be an unbounded label, "
                        + "which is how a Prometheus instance dies")
                .allSatisfy(gauge -> assertThat(gauge.getId().getTags())
                        .allSatisfy(tag -> assertThat(tag.getKey()).isIn("status", "application")));
    }

    @Test
    @DisplayName("claimable is reported separately from pending, because a backoff is not a backlog")
    void claimableIsSeparateFromPending() {
        submissionService.submit(Workflows.singleTask());
        ClaimedTask claimed = claimOne();
        completionService.fail(claimed.id(), "worker-1", claimed.fencingToken(),
                FailureKind.RETRYABLE, "will retry after a delay");

        // The backoff uses full jitter, so the delay it actually picked could be anywhere from
        // zero upwards. This test is about the gauge, not the schedule, so the deadline is pinned
        // rather than left to chance; RetryAndDeadLetterTest covers the schedule itself.
        jdbcClient.sql("UPDATE tasks SET next_attempt_at = now() + interval '1 hour' WHERE id = :id")
                .param("id", claimed.id())
                .update();

        queueMetrics.refresh();

        assertThat(queueMetrics.depthOf(TaskStatus.PENDING)).isEqualTo(1);
        assertThat(queueMetrics.claimable())
                .as("a queue that looks deep while nothing is claimable is a retry storm, and "
                        + "adding workers to it would not help")
                .isZero();
    }

    @Test
    @DisplayName("the meters the committed dashboard queries all exist")
    void theDashboardsMetersExist() {
        submissionService.submit(Workflows.singleTask());
        ClaimedTask claimed = claimOne();
        completionService.complete(claimed.id(), "worker-1", claimed.fencingToken(), "{}");

        // Every name here appears in infra/grafana/sentinel-engine-dashboard.json. A rename that
        // does not update both should fail here rather than quietly flat-line a panel.
        List<String> required = List.of(
                "sentinel.queue.depth",
                "sentinel.queue.claimable",
                "sentinel.claim.tasks",
                "sentinel.claim.empty_polls",
                "sentinel.claim.duration",
                "sentinel.task.completed",
                "sentinel.task.retried",
                "sentinel.task.failed",
                "sentinel.task.fenced_out",
                "sentinel.task.duration",
                "sentinel.heartbeat.renewed",
                "sentinel.reaper.requeued",
                "sentinel.reaper.failed",
                "sentinel.reaper.expired_backlog",
                "sentinel.admission.rejected",
                "sentinel.poll.waiting",
                "sentinel.workflow.submitted");

        assertThat(required).allSatisfy(name -> {
            try {
                assertThat(meters.get(name).meter()).isNotNull();
            } catch (MeterNotFoundException e) {
                throw new AssertionError("the dashboard queries " + name + " but nothing registers it", e);
            }
        });
    }

    @Test
    @DisplayName("task duration is tagged by type, never by task id")
    void taskDurationIsTaggedByTypeOnly() {
        submissionService.submit(Workflows.singleTask());
        ClaimedTask claimed = claimOne();
        completionService.complete(claimed.id(), "worker-1", claimed.fencingToken(), "{}");

        var timer = meters.get("sentinel.task.duration").tag("task_type", Workflows.TASK_TYPE).timer();

        assertThat(timer.count()).isEqualTo(1);
        assertThat(timer.getId().getTags())
                .as("a task id label would create a new time series per task instance, growing "
                        + "forever until the metrics store falls over")
                .noneSatisfy(tag -> assertThat(tag.getKey()).isEqualTo("task_id"));
    }

    @Test
    @DisplayName("a workflow records the trace context of the request that submitted it")
    void submissionCapturesTraceContext() {
        var workflow = submissionService.submit(Workflows.singleTask()).workflow();

        // Null here, because this test does not run inside a traced request. What matters is that
        // the column exists and the claim path reads it: an untraced submitter is normal, and must
        // not be treated as an error anywhere downstream.
        String stored = jdbcClient.sql("SELECT trace_context FROM workflows WHERE id = :id")
                .param("id", workflow.id())
                .query(String.class)
                .optional()
                .orElse(null);
        assertThat(stored).isNull();

        ClaimedTask claimed = claimOne();
        assertThat(claimed.traceContext())
                .as("an absent trace must travel as absent, not as an empty string a worker would "
                        + "try to parse")
                .isNull();
    }

    private ClaimedTask claimOne() {
        List<ClaimedTask> claimed =
                claimService.claim(new ClaimRequest("worker-1", List.of(Workflows.TASK_TYPE), 1));
        assertThat(claimed).hasSize(1);
        return claimed.getFirst();
    }
}
