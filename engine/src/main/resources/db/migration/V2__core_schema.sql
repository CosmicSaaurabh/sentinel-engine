-- Core schema for Sentinel Engine, per LLD-001.
--
-- Design notes that are load-bearing and easy to break by accident:
--
--  * uuidv7() (Postgres 18+) is used instead of gen_random_uuid() because it is
--    time-ordered. Random UUIDs scatter inserts across the whole primary-key
--    btree; on the tasks table, which takes the highest insert rate in the
--    system, that fragments the index permanently.
--
--  * 'BLOCKED' and 'PENDING' are separate task statuses on purpose. The claim
--    index is partial on status = 'PENDING', so PENDING must mean "claimable"
--    and nothing else, or a wide DAG fills the hottest index in the system with
--    rows no worker can take.
--
--  * lease_expires_at is deliberately NOT indexed anywhere. Heartbeats write
--    only that column, and an update that touches no indexed column can be a
--    HOT (heap-only tuple) update, which skips index maintenance entirely.
--    Indexing it would add index writes to the highest-frequency write path in
--    the system. See LLD-001 section 6.2 before adding an index here.
--
--  * workflow_id is carried on task_dependencies and outbox even though it is
--    relationally redundant. It is the Citus distribution key, and a
--    distributed table must contain its distribution column.
--
-- Never edit this file once applied. Roll forward with V3__*.sql.

-- ---------------------------------------------------------------------------
-- workflows
-- ---------------------------------------------------------------------------
CREATE TABLE workflows (
    id                  uuid        PRIMARY KEY DEFAULT uuidv7(),
    name                text        NOT NULL,
    status              text        NOT NULL DEFAULT 'RUNNING',
    submission_key      text,
    scheduled_at        timestamptz NOT NULL DEFAULT now(),
    started_at          timestamptz,
    finished_at         timestamptz,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT workflows_name_not_blank
        CHECK (length(btrim(name)) > 0),
    CONSTRAINT workflows_status_check
        CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED')),
    CONSTRAINT workflows_finished_only_when_terminal
        CHECK ((status = 'RUNNING') = (finished_at IS NULL))
);

-- Submission idempotency: resubmitting the same key returns the existing
-- workflow rather than starting a second execution of the same work.
CREATE UNIQUE INDEX workflows_submission_key_uq
    ON workflows (submission_key) WHERE submission_key IS NOT NULL;

-- ---------------------------------------------------------------------------
-- tasks -- this table is also the queue
-- ---------------------------------------------------------------------------
CREATE TABLE tasks (
    id                   uuid        PRIMARY KEY DEFAULT uuidv7(),
    workflow_id          uuid        NOT NULL REFERENCES workflows (id) ON DELETE CASCADE,
    name                 text        NOT NULL,
    task_type            text        NOT NULL,
    status               text        NOT NULL,
    input                jsonb       NOT NULL DEFAULT '{}'::jsonb,
    output               jsonb,
    last_error           text,
    last_error_kind      text,
    attempt              int         NOT NULL DEFAULT 0,
    max_attempts         int         NOT NULL DEFAULT 10,
    pending_dependencies int         NOT NULL DEFAULT 0,
    next_attempt_at      timestamptz NOT NULL DEFAULT now(),
    lease_expires_at     timestamptz,
    owner_worker_id      text,
    fencing_token        bigint      NOT NULL DEFAULT 0,
    created_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT tasks_status_check
        CHECK (status IN ('BLOCKED', 'PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED')),
    CONSTRAINT tasks_error_kind_check
        CHECK (last_error_kind IS NULL OR last_error_kind IN ('RETRYABLE', 'PERMANENT')),
    CONSTRAINT tasks_name_uq
        UNIQUE (workflow_id, name),
    CONSTRAINT tasks_name_not_blank
        CHECK (length(btrim(name)) > 0),
    -- Routing keys are used unquoted in array-building SQL, so their character
    -- set is constrained here rather than trusted from the wire.
    CONSTRAINT tasks_task_type_format
        CHECK (task_type ~ '^[A-Za-z0-9._-]{1,128}$'),
    CONSTRAINT tasks_max_attempts_positive
        CHECK (max_attempts >= 1),
    -- A claim increments attempt, so exceeding max_attempts means a retry
    -- guard was missed somewhere. Fail loudly at the database rather than
    -- letting a task retry forever.
    CONSTRAINT tasks_attempt_bounds
        CHECK (attempt >= 0 AND attempt <= max_attempts),
    CONSTRAINT tasks_pending_deps_nonneg
        CHECK (pending_dependencies >= 0),
    -- The invariant the fencing-token logic rests on: exactly the RUNNING rows
    -- have an owner and a lease. Enforced here rather than trusting every code
    -- path to remember to null both out on release.
    CONSTRAINT tasks_lease_consistency CHECK (
        (status = 'RUNNING' AND owner_worker_id IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR
        (status <> 'RUNNING' AND owner_worker_id IS NULL AND lease_expires_at IS NULL)
    )
);

-- The claim index. Its predicate matches the claim query's predicate exactly,
-- so it holds only rows that are genuinely claimable. Its size tracks queue
-- depth rather than table size: 50M completed tasks and 400 pending ones give
-- a 400-entry index. id is last so the ordering is total and ties are stable.
CREATE INDEX tasks_claimable_idx
    ON tasks (task_type, next_attempt_at, id)
    WHERE status = 'PENDING';

-- The reaper index. Note what it does not contain: lease_expires_at. See the
-- header comment. The reaper scans this (bounded by in-flight concurrency) and
-- filters the timestamp from the heap.
CREATE INDEX tasks_running_idx
    ON tasks (id)
    WHERE status = 'RUNNING';

-- Dependency flip and per-workflow status aggregation.
CREATE INDEX tasks_workflow_status_idx
    ON tasks (workflow_id, status);

-- ---------------------------------------------------------------------------
-- task_dependencies -- the DAG edges
-- ---------------------------------------------------------------------------
CREATE TABLE task_dependencies (
    workflow_id        uuid NOT NULL REFERENCES workflows (id) ON DELETE CASCADE,
    task_id            uuid NOT NULL REFERENCES tasks (id) ON DELETE CASCADE,
    depends_on_task_id uuid NOT NULL REFERENCES tasks (id) ON DELETE CASCADE,

    PRIMARY KEY (workflow_id, task_id, depends_on_task_id),
    CONSTRAINT task_dependencies_no_self_edge CHECK (task_id <> depends_on_task_id)
);

-- "Which tasks unblock when this one completes?" -- the hot flip lookup.
CREATE INDEX task_dependencies_parent_idx
    ON task_dependencies (workflow_id, depends_on_task_id);

-- ---------------------------------------------------------------------------
-- outbox -- transactional outbox, written now, relayed once Cassandra lands
-- ---------------------------------------------------------------------------
CREATE TABLE outbox (
    id          uuid        PRIMARY KEY DEFAULT uuidv7(),
    workflow_id uuid        NOT NULL,
    task_id     uuid,
    event_type  text        NOT NULL,
    payload     jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at  timestamptz NOT NULL DEFAULT now(),
    relayed_at  timestamptz
);

-- Undelivered events only, so the index tracks relay backlog rather than the
-- total volume of history ever produced.
CREATE INDEX outbox_undelivered_idx
    ON outbox (created_at, id)
    WHERE relayed_at IS NULL;

-- ---------------------------------------------------------------------------
-- workers -- liveness and capacity. NOT the correctness mechanism.
-- ---------------------------------------------------------------------------
-- Task ownership is proven by tasks.fencing_token, never by this table. A stale
-- row here costs observability accuracy, never correctness.
CREATE TABLE workers (
    id                text        PRIMARY KEY,
    task_types        text[]      NOT NULL DEFAULT '{}',
    max_concurrency   int         NOT NULL DEFAULT 1,
    registered_at     timestamptz NOT NULL DEFAULT now(),
    last_heartbeat_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT workers_concurrency_positive CHECK (max_concurrency >= 1)
);

CREATE INDEX workers_heartbeat_idx ON workers (last_heartbeat_at);

-- ---------------------------------------------------------------------------
-- capacity_counters -- sharded admission control
-- ---------------------------------------------------------------------------
-- Admission control must answer "how many slots are free" in constant time.
-- A single counter row is a hot-row bottleneck at claim rate; a live COUNT(*)
-- over pending tasks gets slower exactly when the backlog is largest, which is
-- when admission control most needs to be fast. Hence N shard rows summed in
-- one statement, against one snapshot.
--
-- in_flight is deliberately allowed to go negative on an individual shard. A
-- claim increments a shard chosen by hash and a completion decrements one, with
-- no requirement that a completion return to the shard its claim touched --
-- that freedom is the whole point of sharding the counter. It follows that any
-- single shard can drift below zero while the sum across shards stays right,
-- so a per-row CHECK (in_flight >= 0) would reject perfectly correct writes.
-- The clamp belongs on the aggregate read instead.
CREATE TABLE capacity_counters (
    shard_id   int         PRIMARY KEY,
    in_flight  bigint      NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- 16 is a starting estimate to be benchmarked, not a fixed truth. Changing it
-- is a new migration that inserts or removes shard rows; the read path sums
-- whatever rows exist.
INSERT INTO capacity_counters (shard_id)
SELECT generate_series(0, 15);
