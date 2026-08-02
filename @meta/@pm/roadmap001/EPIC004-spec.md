# EPIC004 Spec: Streaming Parquet Writes And Compression

## Purpose

Write new immutable local Parquet files from bounded input batches with backpressure, safe publication, and explicit interoperable compression.

## Reference Inputs

- Roadmap: `@meta/@pm/ROADMAP001.md`
- EPIC002 staged local writes and atomic publication
- EPIC003 schema and columnar batch contracts
- Required memory and lifecycle invariants in ROADMAP001

## Scope

In scope:

- explicit or validated write schemas and bounded batch enumeration
- schema mismatch and nullability validation
- incremental Parquet encoding with configurable row-group and page limits
- staged local output and create-only publication after successful footer completion and close
- `:uncompressed`, `:snappy`, `:zstd`, `:gzip`, and `:lz4_raw` where supported by the pinned Parquet crate
- documented and tested default compression and codec availability
- producer backpressure, bounded native buffering, cancellation, and upstream termination
- large round trips, metadata verification, codec interoperability, and failure cleanup

Out of scope:

- S3 multipart publication
- mutation or byte append of completed Parquet files
- schema inference requiring full-input materialization
- unbounded producer queues or complete-output buffering
- table formats, manifests, compaction scheduling, or distributed transactions

## Write And Compression Contract

A write accepts a validated schema and an enumerable of bounded compatible columnar batches. Batches are consumed incrementally under downstream/native capacity; Parquex does not collect the enumerable or complete output. The writer closes row groups and pages according to validated limits and bounds buffering by input batch size, row-group/page settings, encoder buffers, and bounded staging overhead—not total output size.

Local output is written to a unique temporary object. Create-only publication occurs only after all batches are encoded, the Parquet footer is completed, the writer closes successfully, and configured flush/sync policy succeeds. If the destination already exists, publication returns a stable conflict, removes the owned temporary object, and leaves existing bytes unchanged. Failure, halt, cancellation, or owner exit stops upstream consumption and removes temporary output. A completed destination is a new immutable file and is never extended or replaced as an append mechanism.

Compression accepts `:uncompressed`, `:snappy`, `:zstd`, `:gzip`, and `:lz4_raw` only when available in the pinned crate build. The selected codec and relevant settings are validated before publication and visible in Parquet metadata. The documented default is exercised like every advertised codec, and unavailable codecs fail explicitly rather than silently falling back.

## Acceptance Criteria

- Input enumerables larger than available memory are consumed incrementally rather than materialized by Parquex.
- Valid batches produce a complete Parquet file with the requested schema and deterministic row order.
- Schema mismatch and nullability failures stop upstream consumption and publish nothing.
- Configured row-group, page, and compression settings are observable in Parquet metadata.
- Every advertised codec, including the documented default, round-trips and is readable by an independent compatible reader.
- Empty input follows one documented, interoperable schema-preserving policy.
- Failed, halted, or cancelled writes leave no published partial file or temporary artifact.
- Existing destinations return a stable conflict and remain byte-for-byte unchanged.
- Peak buffering stays within the documented batch/row-group/page/staging envelope for output materially larger than that envelope.

## Test Strategy

- Stream deterministic batches into isolated local destinations and round-trip through Parquex.
- Inspect metadata for row-group count, schema, and selected compression codec.
- Read every advertised codec with an independent compatible Parquet reader.
- Inject schema, nullability, encoder, producer, close, destination-collision, and publication failures at deterministic boundaries.
- Verify empty input and multiple row-group outputs explicitly.
- Halt and cancel while production is active, then assert upstream termination and temporary-file cleanup.
- Write data materially larger than configured buffers and assert peak memory against a documented tolerance.

## Quality Bar

- No write path collects the input enumerable or complete encoded object.
- Backpressure and all row-group, page, and buffering limits are explicit and validated.
- Codec support never silently changes the caller's requested compression.
- Publication is create-only and complete-or-absent; it never replaces or mutates an existing completed file.
- `bin/qa_check.sh` must pass before the epic commit.
