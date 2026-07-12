# Learning Log

A running record of concepts learned while building the Sentinel Engine.
Append one entry per concept, newest at the top.
Revisit this file periodically to revise.

## Entry Template

```
### <date> - <concept title>
- Context: where in the project this came up.
- What I learned: the core idea in my own words.
- Gotcha: the mistake or misconception this corrected.
- Reference: link to doc, PR, or external article.
```

---

### 2026-07-13 - Project kickoff
- Context: Sentinel Engine MVP scoped and phased.
- What I learned: a durable execution engine can be reduced to three primitives, which are a persistent state machine, an atomic task claim, and a lease with recovery.
- Gotcha: "exactly-once execution" is impossible over a network in the general case; the real goal is exactly-once state transition with at-least-once execution and idempotent side effects.
- Reference: docs/tasks/mvp-task-breakdown.md
