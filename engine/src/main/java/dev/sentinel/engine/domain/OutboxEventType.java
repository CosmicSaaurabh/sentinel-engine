package dev.sentinel.engine.domain;

/**
 * History event kinds written to the transactional outbox.
 *
 * <p>The outbox is written from day one even though nothing relays it yet, so that when the
 * Cassandra history store lands no state-writing code has to change: the events are already
 * being produced atomically with the state changes they describe.
 */
public enum OutboxEventType {

    WORKFLOW_SUBMITTED,
    WORKFLOW_COMPLETED,
    WORKFLOW_FAILED,
    TASK_SCHEDULED,
    TASK_CLAIMED,
    TASK_COMPLETED,
    TASK_FAILED,
    TASK_RETRY_SCHEDULED,
    TASK_LEASE_EXPIRED,
    TASK_CANCELLED
}
