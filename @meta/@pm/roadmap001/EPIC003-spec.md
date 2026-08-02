# EPIC003 Spec: Streaming Parquet Reads

## Purpose

Read local Parquet files lazily through the backend-neutral object contract as bounded columnar batches, with projection, cancellation, and predictable memory use.

## Reference Inputs

- Roadmap: `@meta/@pm/ROADMAP001.md`
- EPIC002 backend-neutral object access, local range reads, and location descriptors
- Required memory and lifecycle invariants in ROADMAP001

## Scope

In scope:

- compatible pinned Arrow and Parquet crates
- footer, metadata, and required data reads through bounded object ranges
- schema inspection and documented Parquet-to-Elixir type mappings
- primitive, binary, temporal, decimal, list, struct, and null handling
- native reader resources with projection, row-group position, cancellation, batch limits, and bounded prefetch
- pull-based lazy Elixir batch streams
- columnar batches with explicit access and conversion functions
- local lifecycle, malformed-input, projection, and bounded-memory coverage

Out of scope:

- Parquet writing or compression configuration
- S3 network access
- predicates, row-group pruning, SQL, or DataFusion
- implicit whole-result conversion to Elixir row maps
- unbounded prefetch or full-file materialization

## Streaming Read Contract

Opening a stream may inspect bounded metadata but must not read or decode the complete file. Each downstream demand advances the native reader by at most the configured batch work, and projection prevents decoding unselected columns. Reader state tracks selected columns, current row group, cancellation, batch limits, and a bounded prefetch budget; it never turns file size or result cardinality into an allocation target.

Peak buffering is documented as a function of batch size, prefetch depth, bounded range reads, and bounded metadata overhead, not total file size. Native I/O and decoding run without occupying a normal BEAM scheduler. Halting, consumer failure, timeout, or owner exit cancels work and promptly releases reader, file, and native resources.

Batches remain columnar across the native boundary for as long as practical. Row-oriented access is explicit. Supported mappings and nullability behavior are stable and documented; unsupported logical types and malformed data return contextual structured errors without panic.

## Acceptance Criteria

- The first batch is available without reading or decoding the complete file.
- Schema inspection covers every documented mapping and rejects unsupported logical types explicitly.
- Projection avoids decoding unselected columns.
- Empty and multi-row-group files stream correctly in deterministic batch order.
- Nested and null data preserve the documented columnar representation.
- Early halt and consumer failure promptly cancel work and close native and local resources.
- Peak buffering stays within the documented batch/prefetch/range/metadata envelope for a file materially larger than that envelope.
- Malformed data and invalid options return stable structured errors rather than panics.

## Test Strategy

- Use fixed local Parquet fixtures for empty, multi-row-group, nested, null, temporal, decimal, binary, malformed, and unsupported-schema cases.
- Assert schema metadata, column projection, batch boundaries, and explicit conversions independently.
- Instrument object ranges to prove startup and projected reads do not fetch the complete file.
- Halt streams and fail consumers at deterministic batch boundaries, then assert prompt resource cleanup.
- Repeatedly open and close isolated readers to detect leaked resources.
- Stream a fixture materially larger than configured buffers and assert peak memory against a documented tolerance.

## Quality Bar

- No public read path hides full-file or full-result materialization.
- Memory and prefetch limits are explicit, positive, validated, and covered by regression tests.
- No long-running I/O or decoding occupies a normal BEAM scheduler.
- Public errors are contextual and stable without exposing native panic details.
- `bin/qa_check.sh` must pass before the epic commit.
