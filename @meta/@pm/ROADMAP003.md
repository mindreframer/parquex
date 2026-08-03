# ROADMAP003 — Simple Streaming Parquet for Object Storage

- **Status:** Active
- **Target release:** `0.3.0`
- **Primary interface:** Elixir
- **Native implementation:** Rust via Rustler

## 1. Product Goal

Parquex makes it straightforward to write Parquet files to local or S3-compatible object storage and stream them back with bounded memory.

```elixir
{:ok, store} =
  Parquex.Store.open(:s3,
    bucket: "events",
    region: "eu-central-1",
    endpoint: "https://s3.eu-central-1.wasabisys.com",
    path_style: true
  )

rows = [
  %{"id" => 1, "name" => "created"},
  %{"id" => 2, "name" => "updated"}
]

{:ok, _metadata} =
  Parquex.write(store, "events/part-001.parquet", rows, compression: :zstd)

{:ok, stream} = Parquex.stream(store, "events/part-001.parquet")
Enum.each(stream, &consume_batch/1)
```

The package owns Parquet encoding and decoding, compression, bounded buffering, object-store clients, multipart uploads, range reads, cancellation and optional time partitioning. Applications choose keys and decide whether writing the same key again is appropriate.

## 2. Public Model

```text
Store + key -> object bytes
Store + key -> Parquet file
Store + dataset prefix -> time-partitioned Parquet files
```

Public concepts:

- `Parquex.Store`: a reusable local or S3-compatible object-store connection.
- `Parquex.Schema`: an explicit Parquet schema for continuous input.
- `Parquex.Batch`: a bounded columnar batch.
- `Parquex.Writer`: an incremental writer for one Parquet file.
- `Parquex.Stream`: a pull-based reader for one Parquet file.
- `Parquex.Dataset`: an optional descriptor for UTC time-partitioned files.

There is one addressing model: a store and a relative key. There are no URI-shaped destinations, backend-specific object wrappers or multi-source reader types.

## 3. Storage Contract

1. A write targets exactly one key.
2. Writing an existing key replaces its value using the storage provider's normal semantics.
3. S3 writers stream directly into a multipart upload at the final key.
4. Multipart completion happens only after the Parquet footer is encoded.
5. Cancellation and failures abort active uploads when possible.
6. Incomplete multipart uploads are not readable as completed objects.
7. Concurrent writes to one key use last-completion-wins behavior.
8. Applications use unique keys when they want append-like or immutable data.
9. Dataset writers always generate collision-resistant part keys.

Parquex does not require conditional puts, conditional copies, fencing, transactions or provider capability probes for ordinary writes.

## 4. In Scope

- Reusable local and S3-compatible stores.
- Per-key head, list, bounded read, put and delete operations.
- Direct bounded multipart uploads to the final S3 key.
- Local replacement through completed temporary-file publication.
- Finite row-map Parquet helpers with schema inference.
- Explicit-schema continuous Parquet writers.
- Bounded Parquet streams, projection and simple typed filters.
- Zstandard, Snappy, Gzip, LZ4 and uncompressed Parquet.
- Optional UTC minute/hour/day/week/month datasets.
- Clear owner-exit and cancellation behavior.
- Precompiled NIFs for the established seven-target matrix.

## 5. Deliberately Small Surface

- No SQL or dataframe engine.
- No object-store transactions or write fencing.
- No append-to-existing-file operation.
- No automatic key allocation for single-object writes.
- No whole-file content checksum protocol.
- No globally sorted multi-file results.
- No schema registry, evolution, compaction or table format.
- No event-store sequencing, snapshots or materialization orchestration.

## 6. Reliability Model

- Total file size does not determine peak live Elixir or native memory.
- S3 buffering is bounded by multipart part size and in-flight part count.
- Reader buffering is bounded by batch, range and prefetch settings.
- A successful close publishes a complete Parquet file at the requested key.
- A failed or cancelled close does not intentionally complete the active upload.
- A provider or transport failure around completion can be ambiguous; callers may reconcile with `Store.head/2` or read the key.
- Reusing a key is an explicit replacement, not a conflict.
- Dataset part names make accidental replacement negligibly likely.

## 7. Quality and Commit Policy

`bin/qa_check.sh` is the authoritative epic gate. Every epic must:

1. Complete every phase and acceptance criterion in its spec and plan.
2. Run `bin/qa_check.sh` and fix all failures.
3. Review the diff for unrelated changes, generated artifacts, secrets and stale concepts.
4. Update the epic plan progress and quality-gate checklist.
5. Commit only after the gate is green with `roadmap003 - epic N - ...` and an informative body.

User-owned unrelated files remain untouched. RustFS remains running and is reused between gates.

---

## Epic 1 — One Store-and-Key API

**Objective:** Expose one obvious addressing model throughout the Elixir and native boundaries.

Deliverables:

- Store/key-only object and Parquet operations.
- One-object stream and writer types.
- Removal of redundant destination, object, append and multi-source surfaces.
- A reduced NIF export table containing only functions used by the public model.

Acceptance:

- Every public read or write example begins with a `Parquex.Store`.
- Public module docs contain no alternate addressing model.
- Removed functions and modules are absent rather than deprecated.
- Tests cover the remaining API directly.

## Epic 2 — Direct Provider-Friendly Writes

**Objective:** Make normal PUT and multipart behavior sufficient for local, AWS-style and compatible object stores.

Deliverables:

- S3 multipart upload directly at the requested final key.
- Existing-key replacement for byte and Parquet writes.
- Local completed-file replacement.
- Removal of conditional copy, staging-object publication and create-only configuration.
- Integration coverage for overwrite, cancellation and cleanup.

Acceptance:

- The same-key second write succeeds and the new bytes are readable.
- S3 publication performs no server-side staging copy.
- RustFS and a manual Wasabi probe pass byte and Parquet replacement.
- Cancellation leaves no completed object when no prior value existed.

## Epic 3 — Straightforward Parquet and Dataset Ergonomics

**Objective:** Keep the common finite and streaming workflows obvious and remove low-value knobs and overloads.

Deliverables:

- One inferred-schema `write` form and one explicit-schema `write` form.
- Simple `read`, `stream`, `schema` and `open_writer` forms.
- Object chunk writers without storage-specific flush/sync options.
- Dataset writers that use collision-resistant keys and the same direct publication path.
- Focused examples and contract tests for finite, continuous and time-partitioned data.

Acceptance:

- A new user can write and read row maps without constructing native-shaped values.
- Continuous input remains explicitly schema-driven and bounded.
- Writing a dataset needs only a store, prefix, schema and time column.
- Unknown or removed options fail clearly.

## Epic 4 — Plain-Language Product and Package

**Objective:** Present Parquex as a small approachable Parquet/object-storage library with no assumed context.

Deliverables:

- A short README led by runnable local and S3 examples.
- Focused guides for stores, Parquet files, datasets, telemetry and runtime support.
- Current architecture notes that describe only the shipped design.
- Package contents and module docs free of migration, compatibility and historical narratives.

Acceptance:

- README explains the product, common API and write semantics without release history.
- Published guides contain no migration or compatibility sections.
- `mix docs --warnings-as-errors` passes.
- Package audit includes only relevant source and documentation.

## Epic 5 — Hardening and `0.3.0` Release

**Objective:** Validate the simplified contract, publish `0.3.0` and prove precompiled consumers need no Rust toolchain.

Deliverables:

- Replacement, cancellation, repeated-read/write and resource-plateau coverage.
- Synchronized Elixir and Cargo version `0.3.0`.
- Seven published NIF archives and a committed SHA-256 manifest.
- Green logic QA and clean no-Rust consumer matrix.
- Completed roadmap marker after all remote gates succeed.

Acceptance:

- Final local QA and documentation gates pass.
- GitHub release workflow succeeds on macOS ARM/Intel, Linux GNU/musl ARM/Intel and Windows Intel.
- `v0.3.0` contains exactly seven verified assets.
- Clean consumers load published binaries without invoking Cargo or Rust.
- ROADMAP003 is marked completed only after release and final CI are green.

## 8. Dependency Order

```text
Epic 1: one API
   -> Epic 2: direct writes
   -> Epic 3: simple ergonomics
   -> Epic 4: plain communication
   -> Epic 5: hardening and release
```

## 9. Definition of Success

Roadmap 003 is complete when an Elixir application can open a local or S3-compatible store, stream compressed Parquet data directly to any chosen key, replace that key using ordinary provider behavior, and stream it back with bounded memory. The optional dataset API groups files by UTC time without changing the storage contract. Source, documentation, CI and seven precompiled `0.3.0` binaries must be published and green.
