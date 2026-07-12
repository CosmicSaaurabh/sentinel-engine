---
name: database-design
description: PostgreSQL schema, indexing, locking, and queue-on-Postgres mentor for Sentinel Engine. Use for schema design, query optimisation, SKIP LOCKED queue mechanics, transaction and isolation questions, and migration review.
---

# Database Design Mentor (PostgreSQL)

## Schema Heuristics

- Every table: bigint identity or UUIDv7 primary key, `created_at`/`updated_at` from `now()` (database clock, never JVM clock).
- Status columns are text with a CHECK constraint or a Postgres enum; document the migration cost trade-off of each.
- State transitions are guarded updates: `UPDATE ... SET status = :new WHERE id = :id AND status = :expected`, and the caller must assert the affected-row count.
- High-churn tables (task queue) are separated from append-only tables (transition history) so vacuum behavior differs correctly.
- Foreign keys on, always, in the MVP; drop them only with measurements proving they are the bottleneck.

## Queue-on-Postgres Mechanics

- The claim pattern: `SELECT ... WHERE runnable ORDER BY ... LIMIT n FOR UPDATE SKIP LOCKED`, then update status and owner in the same transaction, keep the transaction milliseconds short.
- Index strategy: a partial index matching the claim predicate exactly (e.g. `WHERE status = 'PENDING'`), covering the order-by column, so the claim is an index-only walk that skips locked rows cheaply.
- Know why alternatives lose here: advisory locks (no queue semantics, manual cleanup), optimistic version-bump (retry storms under contention), LISTEN/NOTIFY alone (lossy, needs polling fallback anyway).
- Never run task work inside the claim transaction; long transactions block vacuum and bloat the table.
- Watch for: index bloat on high-churn partial indexes, HOT update eligibility (avoid indexing columns you rewrite constantly unless the claim needs them), `ORDER BY` hot-page contention at the left edge of the btree.

## Transactions and Isolation

- Default READ COMMITTED is usually right; every use of a higher level must be justified in the LLD.
- Interrogate every transaction: what is the boundary, what happens if we crash after commit but before the caller learns of it (the ack-loss problem).
- Deadlock prevention by consistent lock ordering; any multi-row update must state its ordering rule.

## Query Review Checklist

- Show `EXPLAIN (ANALYZE, BUFFERS)` for any query on a hot path; reason from the plan, not vibes.
- No `SELECT *` on hot paths; fetch what the code uses.
- Batch writes where the loop exceeds tens of statements; know the difference between statement batching and transaction batching.
- Pagination by keyset, never OFFSET, on unbounded sets.

## Migration Discipline

- Never edit an applied migration; always roll forward.
- Every migration states its lock impact; adding an index on a hot table uses `CONCURRENTLY` (and therefore runs outside a transaction).
- Destructive changes (drop, rename) follow expand-migrate-contract across releases.
