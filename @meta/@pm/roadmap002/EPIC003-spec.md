# EPIC003 Spec: Ergonomic Single-Object Parquet

## Purpose

Provide convenient finite and bounded streaming Parquet operations over a reusable store and key, with compression and schema handling performed primarily in Rust.

## Reference Inputs

- `@meta/@pm/ROADMAP002.md`
- EPIC001 public contract and EPIC002 stores
- Existing Parquet reader/writer, schema, batch and predicate implementations
- Real `event_log` schema used by the S3mux experiment

## Scope

In scope:

- `Parquex.write/4`, `open_writer/4`, `stream/3`, `read/3` and `schema/3` store/key forms
- row-map and column-map finite input
- finite-input schema inference with deterministic type rules
- explicit-schema bounded writers and readers
- Zstandard, Snappy and uncompressed output
- column projection and small typed comparisons
- local and RustFS integration

Out of scope:

- dataframe transformations
- arbitrary expression languages
- dataset partition routing/discovery
- schema merging/evolution
- globally ordered multi-object reads

## Data Contract

Finite input inference examines the supplied rows, produces one deterministic schema, rejects ambiguous all-null/empty inputs unless a schema is supplied, and validates every row before publication. Streaming writers require an explicit schema so late values cannot change physical types.

`read` materializes all selected rows and is documented for bounded results. `stream` returns bounded columnar batches and releases native state on halt. Row-map conversion is available per batch or as part of the finite helper, never hidden across an unbounded result.

## Acceptance Criteria

- The real event shape round-trips with Zstandard through local and RustFS stores.
- Finite row maps infer stable integer, float, boolean, string, binary and UTC datetime/timestamp types.
- Empty/all-null/mixed-incompatible inputs return stable validation errors.
- Explicit schemas support empty output and nullable columns.
- Projection and timestamp/version comparisons are correct with and without statistics.
- Repeated open/consume/close and early halt return reader/writer resources to baseline.
- Greedy helpers are visibly documented as materializing.

## Test Strategy

- Table-driven schema inference and validation tests.
- Codec round-trips through independent readers.
- Shared local/RustFS integration scenarios.
- Repetition, cancellation, producer failure and malformed-file tests.

## Quality Bar

The simple path should accept ordinary Elixir rows, while the production path remains explicit, columnar, bounded and predictable.
