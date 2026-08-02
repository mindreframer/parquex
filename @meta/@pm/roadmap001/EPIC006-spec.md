# EPIC006 Spec: Append-Oriented Access, Offset Filtering, and Backend Interoperability

## Purpose

Support append-oriented local-first workflows with immutable outputs, minimal typed offset filtering, safe Parquet pruning, and one bounded stream contract across caller-ordered local and S3 inputs.

## Reference Inputs

- Roadmap: `@meta/@pm/ROADMAP001.md`
- EPIC002 through EPIC005 object, streaming Parquet, publication, and S3 contracts
- Parquet row-group statistics and column projection semantics

## Scope

In scope:

- uniquely named immutable Parquet outputs beneath an explicit location or prefix
- a small typed predicate surface for comparisons such as `offset > value`
- conservative row-group pruning from Parquet min/max/null statistics
- bounded row-level predicate evaluation for correctness
- projection combined with predicate-required columns
- caller-ordered mixed local/S3 input streams
- isolated per-location configuration and bounded source concurrency
- a bounded rewrite example that publishes one new output and preserves all inputs

Out of scope:

- byte appends or mutation of completed Parquet objects
- DataFusion, SQL, query planning, or a general expression engine
- Delta Lake, Iceberg, catalogs, manifests, transaction logs, or other table formats
- Hive partition discovery or partition pruning
- application storage blending, routing, caching, synchronization, or deletion policy
- compaction scheduling, background workers, orchestration, or lifecycle ownership

## Append and Offset-Scan Contract

Append helpers create a collision-resistant new object name under a caller-selected local or S3 location/prefix and use the existing create-only, complete-or-absent publication path. A rare naming collision returns a stable conflict and preserves the completed object; helpers never reopen, replace, or append bytes to it.

Predicates are typed column/literal comparisons with explicit validation, null behavior, and unsupported-type errors. Row-group statistics may skip a group only when they prove no row can match; missing, malformed, truncated, or otherwise inconclusive statistics require reading the group. Predicate-required columns are available for bounded batch evaluation even when omitted from the returned projection, and only matching rows enter output batches.

A mixed-input scan accepts a caller-ordered collection of independently configured local and S3 locations. It preserves source order, emits the same columnar batch representation for both backends, and bounds batches, prefetch, active sources, and storage requests. The rewrite example is only a finite streaming read into one newly published immutable output; it owns no schedule, manifest, input deletion, or storage-placement policy.

## Acceptance Criteria

- Append-oriented writes create new uniquely named objects; collisions return stable conflicts and never replace completed objects.
- `offset > value` returns correct rows with usable, missing, and inconclusive statistics.
- Statistics reduce row-group or range reads when safe and never omit a matching row.
- Projection and predicate evaluation preserve documented schemas, types, null semantics, and source order.
- Mixed local/S3 inputs use one batch representation and one stream/cancellation contract.
- Peak mixed-input memory is bounded by configured batch, prefetch, and source-concurrency limits rather than total input size.
- The rewrite example remains streaming, publishes one complete output, and does not delete inputs.
- No SQL, table format, storage blending policy, partition discovery, or compaction orchestration is introduced.

## Test Strategy

- Generate deterministic multi-row-group fixtures with boundary values, nulls, and controlled usable/missing/inconclusive statistics.
- Compare filtered output to a row-level reference model while separately asserting safe row-group/range-read reductions.
- Scan caller-ordered local/S3 fixtures through the project-owned RustFS environment with low batch, prefetch, and source-concurrency limits.
- Exercise early halt, source errors, schema/type mismatch, output-name collision handling, and cleanup.
- Run the bounded rewrite with input materially larger than configured buffers and verify source objects remain unchanged.

## Quality Bar

- Filtering correctness never depends on metadata pruning.
- No path implicitly materializes all rows, opens all sources, or selects a process-global backend.
- Memory and concurrency bounds are configurable, documented, and regression-tested.
- The rewrite remains an example of bounded immutable publication, not a compaction subsystem.
- `bin/qa_check.sh` must pass before the epic commit.
