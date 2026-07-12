---
name: modern-java-springboot
description: Modern Java 21 and Spring Boot 3 mentor for Sentinel Engine. Use when reviewing code idioms, Spring configuration, transaction boundaries, testing strategy, or API design against current best practices.
---

# Modern Java and Spring Boot Mentor

## Java 21 Idioms

- Records for DTOs, value objects, and configuration holders; classes only where identity or mutability is required.
- Sealed interfaces plus pattern matching for closed hierarchies (task results, state transition outcomes) instead of instanceof chains or enums with behavior switches.
- `Optional` for return types only; never for fields or parameters.
- Immutable collections (`List.of`, `Map.copyOf`) by default; defensive copies at trust boundaries.
- Virtual threads for blocking IO paths where appropriate; know the pinning caveats.
- Text blocks for SQL; SQL lives in one visible place, not concatenated fragments.

## Spring Boot Discipline

- Constructor injection only; components are final-field, easily unit-testable classes.
- `@Transactional` boundaries live in the service layer, and the reviewer always asks: what exactly is inside this transaction, and is any network call trapped in it.
- Self-invocation does not pass through proxies; `@Transactional` on a method called from the same class silently does nothing. Check for this in every review.
- `@ConfigurationProperties` records over `@Value` scatter; validated at startup with `@Validated`.
- Beans that own resources (pools, schedulers, gRPC servers) implement lifecycle hooks (`SmartLifecycle` or `DisposableBean`) with deliberate ordering.
- No business logic in controllers or gRPC service adapters; they translate and delegate.

## Testing Strategy

- Unit tests for domain logic with no Spring context; if a unit test needs `@SpringBootTest`, the design is coupled wrong.
- `@SpringBootTest` plus Testcontainers for integration paths: migrations, repositories, gRPC round-trips.
- One test pyramid rule: fast tests run always, container tests run in CI and before PR, and both stay green per project rules.
- Test names state behavior (`claimSkipsRowsLockedByOtherWorkers`), not method names.
- Assertions on behavior and state, not on interactions, unless the interaction is the contract.

## API and Error Design

- gRPC status codes are the contract: `FAILED_PRECONDITION` for illegal transitions, `NOT_FOUND`, `ALREADY_EXISTS` for idempotent duplicates; mapping documented in the LLD.
- Every external error surface includes a machine-readable reason and correlation id.
- Validation at the boundary, invariants in the domain; nothing invalid gets past the adapter layer.
