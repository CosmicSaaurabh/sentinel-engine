-- Baseline migration for Sentinel Engine.
-- Intentionally structure-free: this migration exists only to prove Flyway
-- is wired correctly against a real Postgres instance.
-- Schema DDL for workflows / tasks / task_dependencies arrives in a later
-- migration once LLD-001 is approved.
--
-- Once this migration is applied anywhere, never edit it again.
-- Ship schema changes as new V<n>__description.sql files instead.
select 1;
