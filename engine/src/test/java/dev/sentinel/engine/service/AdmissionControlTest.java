package dev.sentinel.engine.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import dev.sentinel.engine.AbstractIntegrationTest;
import dev.sentinel.engine.error.AdmissionRejectedException;
import dev.sentinel.engine.repository.CapacityCounterRepository;
import dev.sentinel.engine.support.Workflows;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

/**
 * Backpressure at the submission boundary, with the capacity ceiling lowered so saturation is
 * reachable in a test.
 */
@TestPropertySource(properties = {
        "sentinel.admission.total-slots=2",
        "sentinel.admission.retry-after=4s"
})
class AdmissionControlTest extends AbstractIntegrationTest {

    @Autowired
    private WorkflowSubmissionService submissionService;

    @Autowired
    private CapacityCounterRepository capacity;

    @Test
    @DisplayName("submissions are accepted while there is room")
    void submissionsAreAcceptedBelowCapacity() {
        capacity.addInFlight(0, 1);

        assertThat(submissionService.submit(Workflows.singleTask()).workflow()).isNotNull();
    }

    @Test
    @DisplayName("a saturated engine refuses outright rather than queueing the work")
    void saturatedEngineRefuses() {
        capacity.addInFlight(0, 2);

        AdmissionRejectedException rejected = catchThrowableOfType(
                AdmissionRejectedException.class, () -> submissionService.submit(Workflows.singleTask()).workflow());

        assertThat(rejected).isNotNull();
        assertThat(rejected.getMessage()).contains("2 of 2 task slots");
        assertThat(jdbcClient.sql("SELECT count(*) FROM workflows").query(Integer.class).single())
                .as("a refused submission leaves no trace, which is what makes it safe to refuse")
                .isZero();
    }

    @Test
    @DisplayName("the retry hint is jittered, so a refused fleet does not return in one wave")
    void retryHintIsJittered() {
        capacity.addInFlight(0, 5);

        var hints = java.util.stream.IntStream.range(0, 30)
                .mapToObj(i -> catchThrowableOfType(
                        AdmissionRejectedException.class, () -> submissionService.submit(Workflows.singleTask()).workflow()))
                .map(AdmissionRejectedException::retryAfter)
                .distinct()
                .toList();

        assertThat(hints)
                .as("a fixed hint turns a saturated engine into a metronome that gets hit by waves")
                .hasSizeGreaterThan(5);
        assertThat(hints).allSatisfy(hint -> assertThat(hint)
                .isBetween(Duration.ofSeconds(2), Duration.ofSeconds(6)));
    }

    @Test
    @DisplayName("capacity recovers as tasks finish, and submissions resume")
    void capacityRecovers() {
        capacity.addInFlight(0, 2);
        assertThat(catchThrowableOfType(
                AdmissionRejectedException.class, () -> submissionService.submit(Workflows.singleTask()).workflow()))
                .isNotNull();

        capacity.addInFlight(0, -2);

        assertThat(submissionService.submit(Workflows.singleTask()).workflow())
                .as("saturation must be a transient condition, not a poisoned engine")
                .isNotNull();
    }
}
