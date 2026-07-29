# Tech Debt & Follow-Up Register

Running list of known gaps, deferred decisions, and PR review findings that are not blocking but need to be revisited.
Each entry records what the issue is, why it matters, how urgent it is, and where it came from.
Entries move to the Resolved section, with the PR that closed them, once addressed.
Entries are never deleted, so the history of what was consciously deferred stays visible.

## How to use this file

Add an entry whenever a PR review surfaces something worth fixing later but not now.
Priority is High, Medium, or Low, judged by blast radius and how soon the codebase will start depending on the gap being closed.
Effect describes the concrete failure or drift this causes if left alone, not just a general statement that something is bad practice.

---

## Open

### TD-001: Maven plugin versions not fully pinned in root `pom.xml`

- Priority: Medium
- Source: T0.1 PR review, 2026-07-29
- Files: `pom.xml`

`maven-compiler-plugin`, `maven-surefire-plugin`, and `spring-boot-maven-plugin` are explicitly pinned in `pluginManagement`, but `maven-clean-plugin`, `maven-resources-plugin`, and `maven-jar-plugin` are left to whatever the local Maven distribution's default bindings resolve to.
Effect: a different Maven installation, or a future Maven point release with different default plugin bindings, can silently change resource filtering or jar packaging behavior even though the wrapper pins Maven itself to `3.9.16`.
The reproducibility the wrapper is meant to guarantee is only partial until every plugin actually exercised by the build is pinned the same way.

### TD-002: `postgres:18.4` duplicated with no single source of truth

- Priority: Medium
- Source: T0.1 PR review, 2026-07-29
- Files: `infra/docker-compose.yml`, `engine/src/test/java/dev/sentinel/engine/TestcontainersConfiguration.java`, `docs/tasks/T0.1-scaffolding-runbook.md`

The Postgres image tag is a repeated literal in three places.
Effect: nothing fails loudly if one is bumped and another is missed, producing quiet drift between the Postgres a developer runs manually via `docker compose up` and the Postgres the test suite actually runs against.
Worth a shared property or constant once a natural place for one exists (for example, if a build-info or version-catalog mechanism is introduced later).

### TD-003: Local dev credentials are hardcoded with no stated boundary

- Priority: Low
- Source: T0.1 PR review, 2026-07-29
- Files: `infra/docker-compose.yml`, `engine/src/main/resources/application.yaml`

The `sentinel` / `sentinel` credential pair is committed in cleartext.
Acceptable for a local, non-exposed dev database bound to a non-standard port, but nothing today marks this as a deliberate, bounded decision rather than an oversight.
Effect: a future security review or secret scan has no way to tell "known local-only value" from "accidentally committed credential" without a comment stating the boundary explicitly.

### TD-004: Smoke test assertion conflates two different invariants

- Priority: Medium
- Source: T0.1 PR review, 2026-07-29
- Files: `engine/src/test/java/dev/sentinel/engine/EngineApplicationTests.java`

`assertEquals(1, appliedMigrations)` hardcodes "exactly one migration exists" instead of asserting the actual invariant this test is meant to protect, which is that Flyway is wired correctly and runs cleanly against real Postgres.
Effect: the test breaks the moment T1.3 adds `V2__...`, and whoever touches it next either has to rediscover why the number matters or loosens the assertion without understanding what it was protecting.
Revisit the assertion (for example, asserting zero failed migrations rather than an exact count) before T1.3 lands.

### TD-005: No explicit timeout below Hikari's pool-acquisition timeout

- Priority: High
- Source: T0.1 PR review, 2026-07-29
- Files: `engine/src/main/resources/application.yaml`

`connection-timeout: 2000` bounds how long a caller waits to borrow a connection from the pool, but says nothing about a connection that hangs mid-query after being checked out, for example during a network partition or a lock wait on the Postgres side.
Effect: a thread can block indefinitely once a connection is already in hand, with no explicit bound, directly contradicting the project's own "every timeout is explicit" rule.
This is inert today because no queries run yet, but it becomes load-bearing the moment T1.4's `SKIP LOCKED` claim query exists, since a hung claimer would also be holding row locks.
Close this before or alongside T1.4, not after a hang is observed in a concurrency test.

### TD-006: `management.endpoint.health.show-details: always` has no access restriction

- Priority: Low
- Source: T0.1 PR review, 2026-07-29
- Files: `engine/src/main/resources/application.yaml`

Full health detail, including datasource and disk information, is exposed to any caller who can reach `/actuator/health`, with no authentication or profile gating.
Effect: harmless on `localhost` today, but this file is the one most likely to be copy-forwarded into a future environment unchanged, at which point `always` quietly becomes an information-disclosure finding.
Revisit when the observability phase (T3.5) or any reachable deployment first comes into scope.

### TD-007: Ryuk-disabled workaround depends on shell state with no fail-loud check

- Priority: Low
- Source: T0.1 local verification, 2026-07-29
- Files: `~/.zshrc` (machine-local, not in repo), `docs/tasks/T0.1-scaffolding-runbook.md`

`TESTCONTAINERS_RYUK_DISABLED=true` lives in a shell profile, which only loads into new interactive shells, never into already-open terminal tabs or IDE-launched processes.
Effect: the Colima bind-mount failure this works around (see the runbook's "Local Docker environment: Colima" section) can reappear with a confusing stack trace any time a build runs from a shell or process that predates the profile edit, with nothing pointing back at the actual cause.
Not worth fully engineering around for a solo local setup, but if this keeps recurring, consider a Maven profile or a one-line check in the runbook's verification steps that fails fast with a clear message if the variable is unset and Colima is the active Docker context.

---

## Resolved

(none yet)
