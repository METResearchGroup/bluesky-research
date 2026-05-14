# Repository Rules

This file is the source of truth for refactors in this repository.

## Architecture

- Keep orchestration, entrypoints, and handlers thin. They should validate inputs, wire dependencies, call services, and report outcomes. Put business logic in service modules.
- Separate I/O from domain logic. Filesystem, network, model loading, DuckDB, boto3, and Prefect interactions belong in adapters or boundary layers, not mixed into core transformations.
- Prefer dependency injection over hidden globals. Pass collaborators explicitly or wrap them in small typed containers.
- Avoid cross-layer imports that couple low-level helpers to orchestration or storage details.

## Imports and module boundaries

- Prefer absolute imports within repository packages.
- Keep module responsibilities narrow. If a file owns multiple workflows, split it by concern before adding more behavior.
- Production code under `services/` should be distinct from one-off analysis, demo, or experiment code.

## Types and data contracts

- Avoid `Any` in production paths unless a third-party library leaves no practical alternative. Narrow to `TypedDict`, `Protocol`, dataclasses, enums, or concrete collection types.
- Make data contracts explicit at boundaries. Validate external payloads once, then operate on typed internal representations.
- Avoid shape-changing `dict` mutation across call stacks when a typed value object would make invariants clearer.

## Errors and observability

- Do not catch broad `Exception` unless the boundary is explicitly translating unexpected failures into a stable error contract.
- When exceptions are caught, log structured context and preserve the original exception type when possible.
- Prefer returning typed results or raising domain-specific exceptions over sentinel values that hide failure modes.

## State and testability

- Avoid hidden process-wide caches and mutable module-level state in production code. If caching is required, encapsulate it behind a typed interface with explicit lifecycle.
- Write pure functions where possible so unit tests can cover behavior without filesystem, network, or model dependencies.
- Refactors should preserve or improve test coverage around parsing, scoring, serialization, batching, and failure handling boundaries.
