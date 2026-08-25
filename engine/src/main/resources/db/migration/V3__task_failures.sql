-- Why every attempt failed, in order, per LLD-003.
--
-- LLD-001 rejected a general task_transitions table because it duplicated the outbox for every
-- state change. This one is narrower and earns its place on three counts:
--
--   * it records failures only, not every transition;
--   * its growth is bounded at max_attempts rows per task by construction, so a task failing in a
--     tight loop writes at most ten rows and then goes terminal;
--   * it answers "show me this task's failures in order" without parsing JSON payloads, which the
--     outbox cannot do comfortably.
--
-- The outbox is still written for every failure. It is pruned after relay, which is exactly why
-- the cause chain needs its own projection: otherwise it would disappear at the moment someone
-- wants to investigate an old failure.

CREATE TABLE task_failures (
    id            uuid        PRIMARY KEY DEFAULT uuidv7(),
    task_id       uuid        NOT NULL REFERENCES tasks (id) ON DELETE CASCADE,
    workflow_id   uuid        NOT NULL,
    attempt       int         NOT NULL,
    worker_id     text,
    failure_kind  text        NOT NULL,
    error_class   text,
    error_message text        NOT NULL DEFAULT '',
    failed_at     timestamptz NOT NULL DEFAULT now(),

    -- LEASE_EXPIRED is not something a worker can report. It exists so that "nobody ever told us
    -- what happened" is visibly different from "the worker told us it failed". Operationally those
    -- point at different problems: the first at worker or network health, the second at the task.
    CONSTRAINT task_failures_kind_check
        CHECK (failure_kind IN ('RETRYABLE', 'PERMANENT', 'LEASE_EXPIRED')),
    CONSTRAINT task_failures_attempt_positive
        CHECK (attempt >= 1),
    -- One row per attempt. A retried report of the same failure must not append a second row, and
    -- the database enforces that rather than every write path remembering to check.
    CONSTRAINT task_failures_attempt_uq
        UNIQUE (task_id, attempt)
);

CREATE INDEX task_failures_task_idx ON task_failures (task_id, attempt);
CREATE INDEX task_failures_workflow_idx ON task_failures (workflow_id, failed_at);

-- ---------------------------------------------------------------------------
-- dead_letter_tasks
-- ---------------------------------------------------------------------------
-- A view, not a table. Terminal failures are already tasks rows with status = 'FAILED'; moving
-- them elsewhere would mean a second write path on the failure path and a second place to look a
-- task up by id, for no gain. A view gives the queryable surface while keeping one source of truth.
CREATE VIEW dead_letter_tasks AS
SELECT t.id                                                            AS task_id,
       t.workflow_id,
       w.name                                                          AS workflow_name,
       t.name                                                          AS task_name,
       t.task_type,
       t.attempt,
       t.max_attempts,
       t.last_error,
       t.last_error_kind,
       t.updated_at                                                    AS failed_at,
       (SELECT count(*) FROM task_failures f WHERE f.task_id = t.id)   AS recorded_failures
  FROM tasks t
  JOIN workflows w ON w.id = t.workflow_id
 WHERE t.status = 'FAILED';
