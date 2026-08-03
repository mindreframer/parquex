# EPIC005 Spec: Bounded Time-Partitioned Dataset Writer

## Purpose

Consume finite or continuous event batches and publish compressed immutable Parquet parts beneath canonical time prefixes without allowing cardinality or disorder to create unbounded memory.

## Reference Inputs

- `@meta/@pm/ROADMAP002.md`
- EPIC002 store lifecycle, EPIC003 Parquet writer and EPIC004 partition contract
- Dux partitioned Parquet output ergonomics

## Scope

In scope:

- direct finite dataset write
- explicit dataset writer open/write/close/cancel lifecycle
- timestamp extraction and row routing in Rust
- bounded open-partition registry
- row and estimated/uncompressed byte rotation
- collision-resistant create-only part names
- local and S3/RustFS publication and per-file reports
- late and out-of-order data

Out of scope:

- sorting/shuffling all input
- atomic commit of an entire dataset
- replacing or compacting prior parts
- background flush scheduling
- application checkpoint/manifests

## Writer Contract

Each input row is routed from its event timestamp. Rows retain their relative input order within a generated file. The writer keeps at most `max_open_partitions`; deterministic least-recently-used eviction closes and publishes a part, and later rows for that partition open a new part.

`max_rows_per_file` and `max_bytes_per_file` are rotation targets checked at bounded batch boundaries. A part key includes a collision-resistant identifier and carries enough report metadata to identify its partition and observed time bounds. Publication is create-only.

Closing publishes all active parts and returns a report. Cancelling or owner exit aborts active work. Parts successfully published before a later failure remain immutable and are reported in the structured partial-failure details; no unpublished partial object remains.

## Acceptance Criteria

- Every granularity produces canonical paths locally and on RustFS.
- Finite and continuous APIs produce independently readable files.
- Open partition resources never exceed the configured bound.
- Rotation creates multiple valid parts without row loss or duplication.
- Late/out-of-order rows safely create additional parts.
- Producer failure, cancellation and owner exit clean active uploads.
- Peak live memory is bounded by documented batch, partition, Parquet and multipart limits.

## Test Strategy

- Deterministic multi-partition fixtures for every granularity.
- Highly disordered input with a very small open-partition limit.
- Row/byte rotation, empty input, nullable timestamps and invalid timestamp tests.
- Local/RustFS parity, injected conflict/storage failure and resource snapshots.

## Quality Bar

Dataset writing must remain an explicit foreground operation with bounded resources and honest partial-success reporting.
