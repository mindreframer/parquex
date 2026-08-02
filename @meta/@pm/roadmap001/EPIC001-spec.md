# EPIC001 Spec: Native Foundation and Reproducible Quality Gate

## Purpose

Establish Parquex's Elixir/Rust boundary, public module structure, native runtime rules, isolated test foundations, and one reproducible quality gate before any storage or Parquet behavior is introduced.

## Reference Inputs

- Roadmap: `@meta/@pm/ROADMAP001.md`
- Existing Elixir project scaffold
- Rustler resource, scheduling, panic-containment, and NIF-loading conventions

## Scope

In scope:

- initial `Parquex`, location, schema, batch, stream, options, and error module boundaries
- documentation that streaming is the primary public interface
- a native Rust crate, pinned compatible dependencies, minimal loadable NIF, and boundary smoke call
- native resource ownership, cancellation-token, scheduling, panic-containment, and error-translation rules
- executable `bin/qa_check.sh` covering deterministic Elixir and Rust checks
- fixture, temporary-directory, resource-leak, and RustFS-integration-tag test foundations
- concise architecture decisions for streaming, storage neutrality, columnar batches, immutable outputs, and deferred features

Out of scope:

- local or S3 object operations
- Parquet schema inspection, reading, or writing
- Arrow/Parquet, DataFusion, Delta Lake, Iceberg, or Hive behavior
- production reader, writer, storage, cursor, or transaction resources

## Native Foundation Contract

The Elixir API owns public validation and stable result shapes while Rust implements native work behind a narrow Rustler boundary. NIF calls return documented success or contextual error values; Rust panics and backend details never cross into the BEAM. Long-running I/O or decoding must use managed asynchronous work or appropriate dirty scheduling, and every operation design must include cancellation and deterministic resource release. The initial smoke boundary performs no storage or Parquet work.

Public boundaries preserve a backend-neutral location model, bounded columnar batches, pull-based streams, explicit options, and stable errors without prematurely implementing those later-epic behaviors. `bin/qa_check.sh` is the sole repository quality entry point and later epics extend rather than bypass it.

## Acceptance Criteria

- Initial public modules and their non-goals are documented, with streaming identified as the primary API.
- The native crate uses pinned, toolchain-compatible dependencies and loads through Rustler.
- A deterministic test successfully crosses the Elixir/Rust boundary.
- A translated native error reaches Elixir without a Rust panic or VM crash.
- Resource ownership, cancellation, scheduling, panic containment, and error translation rules are explicit.
- Isolated fixture, temporary-directory, resource-leak, and RustFS-integration-tag foundations exist.
- Executable `bin/qa_check.sh` passes from a clean checkout.
- No object-store, Parquet, DataFusion, Delta, Iceberg, or Hive behavior is introduced.

## Test Strategy

- Exercise native loading, one success result, and one translated error through the public Elixir boundary.
- Give filesystem tests unique temporary directories with deterministic cleanup.
- Provide leak assertions that can detect unreleased native resources without timing sleeps.
- Keep RustFS-dependent tests explicitly tagged and excluded until the project-owned integration environment is added in EPIC005.
- Run deterministic, non-interactive Elixir and Rust format, compile/check, lint, and test stages through `bin/qa_check.sh`.

## Quality Bar

- No panic can unwind across the NIF boundary and no long-running work occupies a normal BEAM scheduler.
- Public errors are stable and contextual without exposing native internals or secrets.
- Tests do not depend on execution order, network availability, random timing, or wall-clock sleeps.
- The architecture decisions preserve bounded streaming, backend neutrality, columnar data, and immutable publication.
- `bin/qa_check.sh` is authoritative and green before the epic commit.
