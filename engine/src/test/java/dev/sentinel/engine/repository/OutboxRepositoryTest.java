package dev.sentinel.engine.repository;

import static org.assertj.core.api.Assertions.assertThat;

import dev.sentinel.engine.AbstractIntegrationTest;
import dev.sentinel.engine.domain.NewWorkflow;
import dev.sentinel.engine.domain.OutboxEvent;
import dev.sentinel.engine.domain.OutboxEventType;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * The outbox. Nothing relays it yet, so these tests describe the contract the relay will rely on
 * rather than the relay itself.
 */
class OutboxRepositoryTest extends AbstractIntegrationTest {

    @Autowired
    private WorkflowRepository workflows;

    @Autowired
    private OutboxRepository outbox;

    @Test
    @DisplayName("an appended event is undelivered until a relay marks it")
    void appendedEventStartsUndelivered() {
        UUID workflowId = newWorkflow();

        UUID eventId = outbox.append(workflowId, null, OutboxEventType.WORKFLOW_SUBMITTED, "{\"tasks\":3}");

        List<OutboxEvent> pending = outbox.claimUnrelayed(10);
        assertThat(pending).hasSize(1);
        assertThat(pending.getFirst().id()).isEqualTo(eventId);
        assertThat(pending.getFirst().isRelayed()).isFalse();
        assertThat(pending.getFirst().payload()).contains("3");
    }

    @Test
    @DisplayName("marking an event delivered removes it from the relay backlog")
    void markingRemovesEventFromBacklog() {
        UUID workflowId = newWorkflow();
        UUID eventId = outbox.append(workflowId, null, OutboxEventType.WORKFLOW_SUBMITTED, "{}");

        assertThat(outbox.markRelayed(List.of(eventId))).isEqualTo(1);
        assertThat(outbox.claimUnrelayed(10)).isEmpty();
    }

    @Test
    @DisplayName("marking twice changes nothing, so a crashed relay can safely reship")
    void markingIsIdempotent() {
        UUID workflowId = newWorkflow();
        UUID eventId = outbox.append(workflowId, null, OutboxEventType.TASK_COMPLETED, "{}");

        assertThat(outbox.markRelayed(List.of(eventId))).isEqualTo(1);
        assertThat(outbox.markRelayed(List.of(eventId))).isZero();
    }

    @Test
    @DisplayName("the backlog comes back in creation order, which is the order history must read in")
    void backlogIsOrdered() {
        UUID workflowId = newWorkflow();
        UUID first = outbox.append(workflowId, null, OutboxEventType.WORKFLOW_SUBMITTED, "{}");
        UUID second = outbox.append(workflowId, null, OutboxEventType.TASK_SCHEDULED, "{}");
        UUID third = outbox.append(workflowId, null, OutboxEventType.TASK_COMPLETED, "{}");

        assertThat(outbox.claimUnrelayed(10).stream().map(OutboxEvent::id))
                .containsExactly(first, second, third);
    }

    @Test
    @DisplayName("a relay takes a bounded batch")
    void batchSizeIsHonoured() {
        UUID workflowId = newWorkflow();
        outbox.append(workflowId, null, OutboxEventType.TASK_SCHEDULED, "{}");
        outbox.append(workflowId, null, OutboxEventType.TASK_SCHEDULED, "{}");
        outbox.append(workflowId, null, OutboxEventType.TASK_SCHEDULED, "{}");

        assertThat(outbox.claimUnrelayed(2)).hasSize(2);
    }

    @Test
    @DisplayName("pruning removes delivered events past retention and leaves undelivered ones alone")
    void pruningKeepsUndeliveredEvents() {
        UUID workflowId = newWorkflow();
        UUID delivered = outbox.append(workflowId, null, OutboxEventType.TASK_COMPLETED, "{}");
        outbox.append(workflowId, null, OutboxEventType.TASK_FAILED, "{}");
        outbox.markRelayed(List.of(delivered));
        jdbcClient.sql("UPDATE outbox SET relayed_at = now() - interval '30 days' WHERE id = :id")
                .param("id", delivered)
                .update();

        assertThat(outbox.pruneRelayedOlderThan(Duration.ofDays(7), 100)).isEqualTo(1);
        assertThat(outbox.findByWorkflowId(workflowId)).hasSize(1);
    }

    @Test
    @DisplayName("recently delivered events survive pruning")
    void recentlyDeliveredEventsSurvive() {
        UUID workflowId = newWorkflow();
        UUID eventId = outbox.append(workflowId, null, OutboxEventType.TASK_COMPLETED, "{}");
        outbox.markRelayed(List.of(eventId));

        assertThat(outbox.pruneRelayedOlderThan(Duration.ofDays(7), 100)).isZero();
    }

    private UUID newWorkflow() {
        return workflows.insert(NewWorkflow.immediate("outbox-test")).id();
    }
}
