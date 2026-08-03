# ROADMAP002 — Parquex: Time-Partitioned Parquet Datasets on Object Storage

- **Status:** Completed at `0.2.0` (2026-08-03)
- **Target release:** `0.2.0`
- **Primary interface:** Elixir
- **Native implementation:** Rust via Rustler
- **Supersedes:** `ROADMAP001` public direction while retaining its tested native foundation

## 1. Product Goal

Make Parquex the focused Elixir library for ergonomic, bounded-memory reading and writing of compressed Parquet objects and time-partitioned Parquet datasets on local and S3-compatible object storage.

The common path must read naturally:

```elixir
{:ok, store} = Parquex.Store.open(:s3, bucket: "events", region: "eu-central-1")

dataset =
  Parquex.Dataset.new(store, "event_log/",
    schema: event_schema,
    partition_by: {:time, :timestamp, :hour},
    timestamp_unit: :millisecond,
    compression: :zstd
  )

{:ok, result} = Parquex.Dataset.write(dataset, events)

stream =
  Parquex.Dataset.stream(dataset,
    from: ~U[2026-08-03 10:00:00Z],
    until: ~U[2026-08-03 13:00:00Z],
    columns: [:spaceid, :version, :name, :args]
  )
```

Parquex owns the Rust object-store client, Parquet encoding and decoding, compression, bounded streaming, multipart upload, time partition routing and time-range partition pruning. Applications retain event sequencing, file-retention policy, snapshots, materialization, sharding and business logic.

## 2. Corrected Public Boundary

The primary model is:

```text
Store -> object key -> Parquet object
Store -> dataset prefix + time partition specification -> Parquet dataset
```

Public concepts:

- `Parquex.Store`: one reusable local or S3-compatible object-store handle.
- `Parquex.Dataset`: a prefix, schema, time-partition contract and Parquet defaults.
- `Parquex.Writer`: a bounded writer for one Parquet object.
- `Parquex.Dataset.Writer`: a bounded writer that routes batches into immutable partition files.
- `Parquex.Stream`: a bounded reader over one object or the selected files in one dataset.
- `Parquex.Batch` and `Parquex.Schema`: advanced, explicit representations retained for predictable streaming.

Greedy helpers are allowed for finite inputs and results, but their materialization is explicit in their names and documentation. Streaming remains the safe default for unbounded or large data.

## 3. Time Partition Contract

Roadmap 002 supports event-time partitioning at these granularities:

- `:minute`
- `:hour`
- `:day`
- `:week`
- `:month`

Canonical Hive-compatible prefixes are:

```text
year=2026/month=8/day=3/hour=12/minute=30/
year=2026/month=8/day=3/hour=12/
year=2026/month=8/day=3/
iso_year=2026/week=32/
year=2026/month=8/
```

Rules:

1. Partition by the configured timestamp column in each row, never by upload time.
2. Version `0.2.0` partitions in UTC only.
3. Read intervals are half-open: `[from, until)`.
4. A dataset has one fixed partition specification.
5. Late data creates a new immutable part beneath the older partition; completed files are never modified.
6. Partition boundaries and file-rotation boundaries are independent; one partition may contain many files.
7. ISO week partitioning uses ISO week-year, not calendar year.
8. Partition columns remain available in Parquet rows so boundary filtering is exact.

## 4. In Scope

- Reusable local and S3-compatible stores backed by Rust `object_store`.
- Key-based object metadata, listing, bounded reads, streaming writes, conditional create and delete.
- Ergonomic single-object Parquet read/write helpers.
- Explicit-schema streaming and finite-input schema inference.
- Zstandard, Snappy and uncompressed Parquet.
- Time-partition path calculation and parsing in Rust.
- Bounded routing into minute/hour/day/week/month partition writers.
- Row- and byte-based immutable file rotation.
- Time-range partition pruning and exact boundary-row filtering.
- Column projection and one small typed comparison surface needed for timestamps and offsets.
- Cancellation, owner-exit cleanup, stable redacted errors and bounded telemetry.
- Precompiled NIFs for the established seven-target matrix.

## 5. Explicitly Deferred

- SQL, DataFusion, joins, aggregations and dataframe transformations.
- CSV, Excel, database ingestion and cross-source query execution.
- Event sequencing, event-store concurrency and virtual-shard ownership.
- Snapshot scheduling, projection/materialization logic and compaction services.
- Space-specific routing or partitioning policy.
- Arbitrary partition expressions, caller callbacks per row and custom time zones.
- Table formats, catalogs, transaction logs, atomic whole-dataset replacement and schema evolution.
- Automatic sorting or a globally ordered multi-file result.
- Arbitrary glob query semantics; dataset reads use a known prefix and partition contract.

## 6. Required Invariants

- Object and dataset size must not determine peak live native memory.
- Peak writer memory is bounded by input batch size, row-group/page limits, multipart limits and `max_open_partitions`.
- Peak reader memory is bounded by batch, range, prefetch and selected-file concurrency limits.
- An unordered stream that touches many time partitions cannot retain an unbounded number of writers; inactive writers are closed and later data creates another part.
- Completed objects are immutable and create-only. Failures never expose partial files and abort owned multipart uploads.
- Halting a stream, cancelling a writer or losing its owner promptly releases native resources.
- Repeated scans and writes must plateau in live native memory and resource counts.
- Dataset reads open no files from partitions that cannot overlap the requested interval.
- Exact row filtering removes out-of-range rows from boundary partitions.

## 7. Quality and Commit Policy

`bin/qa_check.sh` is the authoritative epic gate. Every epic must:

1. Complete all phases, deliverables and acceptance criteria in its spec and plan.
2. Run `bin/qa_check.sh` and fix every failure.
3. Review the diff for unrelated changes, generated artifacts, secrets and unsafe defaults.
4. Update the epic plan progress and quality-gate checklist.
5. Commit only after the gate is green, using `roadmap002 - epic N - xxx` with an informative body.

The existing untracked or unrelated user files must remain untouched.

---

## Epic 1 — Corrected Public Contract

**Objective:** Freeze a store- and dataset-centric `0.2.0` API before changing native behavior.

Deliverables:

- Public `Store` and `Dataset` contracts with executable examples.
- Clear greedy versus bounded operations.
- Compatibility/deprecation strategy for the `0.1.x` location-first API.
- An explicit inventory of retained internals and deferred features.

Acceptance:

- Common examples do not require constructing locations or native-shaped field structs.
- Types, result shapes, ownership, ordering and memory behavior are documented and tested.
- No dataset writer or reader behavior is claimed before its later epic.

## Epic 2 — Reusable Store-Centric Object Handling

**Objective:** Expose reusable Rust-backed store handles and key-based object operations for local and S3-compatible storage.

Deliverables:

- `Parquex.Store.open/2` for local and S3.
- Key-based `head`, `read`, `read_range`, `put`, `list` and `delete`.
- Conditional create, bounded multipart streaming and redacted configuration.
- Compatibility adapters from store/key pairs to the proven native object layer.

Acceptance:

- One store is reused across many keys without rebuilding client configuration.
- Local and RustFS pass the same object contract.
- Existing destinations, cancellation and failures preserve immutable-publication guarantees.

## Epic 3 — Ergonomic Single-Object Parquet

**Objective:** Make one compressed Parquet object easy to write and read greedily or as a bounded stream.

Deliverables:

- Store/key forms of `write`, `open_writer`, `stream`, `read` and `schema`.
- Row-map convenience input and finite-input schema inference.
- Explicit-schema columnar streaming for production-sized input.
- Compression, projection and simple typed predicate options.

Acceptance:

- The real event-log shape round-trips through local and RustFS storage with Zstandard.
- Greedy helpers are explicitly documented as materializing.
- Repeated bounded reads and writes release all native resources.

## Epic 4 — Time Partition Specification

**Objective:** Implement the canonical UTC minute/hour/day/week/month partition model independently of dataset I/O.

Deliverables:

- Validated partition specification and timestamp-unit contract.
- Rust partition calculation and canonical prefix formatting/parsing.
- Interval-to-partition enumeration for half-open ranges.
- Boundary and property tests including ISO week-year transitions.

Acceptance:

- Supported timestamp values round-trip through partition calculation and parsing.
- Leap, month, year and ISO-week transitions are correct.
- Invalid granularity, timestamps, units and reversed ranges return stable errors.

## Epic 5 — Bounded Time-Partitioned Dataset Writer

**Objective:** Route finite or continuous batches into compressed immutable Parquet parts beneath canonical time prefixes.

Deliverables:

- Direct dataset write and explicit dataset writer lifecycle.
- Bounded partition-writer registry with deterministic eviction/closure.
- Row/byte rotation, collision-resistant names and per-file write reports.
- Direct local/S3 output with cleanup on producer, native, owner and storage failure.

Acceptance:

- Every supported granularity produces the expected Hive-style layout.
- Out-of-order input cannot exceed `max_open_partitions`.
- Late data safely creates additional parts in an existing partition.
- Peak memory is independent of total input size.

## Epic 6 — Bounded Time-Range Dataset Reader

**Objective:** Stream a requested half-open time range while opening only overlapping partition files.

Deliverables:

- Dataset `stream` and explicit greedy `read`.
- Prefix planning, object discovery, deterministic file traversal and exact timestamp filtering.
- Column projection, simple offset predicate composition, early halt and cancellation.
- Read statistics exposing planned/opened/skipped partitions and files without sensitive keys.

Acceptance:

- Unrelated partitions are not listed or opened beyond required ancestors.
- Boundary partitions emit no rows outside `[from, until)`.
- Multi-file streaming remains bounded and makes no global-order promise.

## Epic 7 — Hardening and `0.2.0` Release

**Objective:** Validate the corrected API against real workloads, migrate documentation, release `0.2.0` and publish verified precompiled binaries.

Deliverables:

- Repeated-read/write memory, cancellation, fault, late-data and out-of-order regression coverage.
- Complete Store, object, dataset, time partition and migration documentation.
- `0.2.0` versions and changelog.
- Seven published NIF artifacts and committed SHA-256 checksum manifest.
- Green GitHub logic QA and clean no-Rust consumer matrix.

Acceptance:

- `bin/qa_check.sh` passes from the final worktree.
- Live native resources return to baseline and OS RSS plateaus under repetition.
- GitHub release workflow succeeds for macOS ARM/Intel, Linux GNU/musl ARM/Intel and Windows Intel.
- The `v0.2.0` release contains exactly seven verified assets.
- The checksum commit activates consumers that compile with failing Cargo/Rust shims on all supported runner platforms.
- ROADMAP002 status is marked completed at `0.2.0` only after release and consumer CI are green.

## 8. Dependency Order

```text
Epic 1: public contract
   -> Epic 2: reusable stores
   -> Epic 3: one Parquet object
   -> Epic 4: time partition model
   -> Epic 5: dataset writer
   -> Epic 6: dataset reader
   -> Epic 7: hardening and 0.2.0 release
```

## 9. Definition of Success

Roadmap 002 is complete when an Elixir application can reuse one local or S3 store, stream event batches through Rust into bounded Zstandard Parquet parts grouped by minute/hour/day/week/month, and stream an exact time range back without touching unrelated partitions or allowing dataset size to determine peak memory. The `0.2.0` source, documentation, CI and seven precompiled artifacts must all be published and green.
