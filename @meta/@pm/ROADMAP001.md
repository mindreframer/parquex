# ROADMAP001 — Parquex: Streaming Parquet over Local and S3 Object Storage

- **Status:** Completed for `0.1.0`; superseded by `ROADMAP002`
- **Scope:** Initial implementation
- **Primary interface:** Elixir
- **Native implementation:** Rust via Rustler

## 1. Goal

Build Parquex as a focused Elixir binding for reading and writing Parquet files through a common local/S3-compatible object-storage interface.

The primary API must be streaming and backpressured. File size and result cardinality must not determine peak memory usage. Applications must be able to configure batch sizes, buffering, request concurrency, and related limits.

Parquex should fit local-first, mostly append-only applications in which immutable Parquet objects may live on local disk or remote S3 storage. Storage blending remains the application's responsibility, but Parquex must expose the same operations and data representation for both backends so it does not obstruct that design.

## 2. In Scope

- Elixir API backed by Rust through Rustler.
- Local files and S3-compatible object storage.
- A backend-neutral location and object-access model.
- Parquet schema inspection and bounded-memory batch reads.
- Bounded-memory Parquet writes to new immutable files/objects.
- Configurable Parquet compression.
- Column projection and a small typed predicate surface useful for offset scans.
- Row-group pruning using Parquet metadata when safe.
- Cancellation, backpressure, cleanup, and explicit memory controls.
- Quality checks and a green QA gate after every epic.

## 3. Explicitly Deferred

- DataFusion and SQL. The architecture may allow a later adapter, but this roadmap does not add it.
- Delta Lake, Iceberg, transaction logs, snapshots, or table-format metadata.
- Hive partition discovery or partition pruning.
- Data frames, analytics helpers, notebooks, or other data-science APIs.
- An application-level local/remote storage router, cache, or synchronization engine.
- A background compaction service or compaction scheduler.
- In-place append or mutation of completed Parquet files. Append-only usage creates new immutable objects.
- Exactly-once delivery or distributed transaction semantics.

## 4. Architecture Principles

1. **Streaming is the default:** public read and write paths operate on bounded batches. Parquex must not hide full-file or full-result materialization.
2. **Pull-based reads:** downstream Elixir demand controls native reads and decoding. Early stream termination cancels work and releases resources.
3. **Bounded pipelines:** batch size, prefetch depth, object-store concurrency, range size, row-group size, multipart part size, and in-flight uploads are configurable and bounded.
4. **Storage neutrality:** Parquet code consumes a backend-neutral object interface. Local and S3 locations use the same public scan/write contracts.
5. **Per-operation storage selection:** no process-global backend assumption may prevent an application from alternating or combining local and remote locations.
6. **Immutable publication:** writers publish a completed new file/object. Failed or cancelled work must not expose a partial result.
7. **Columnar boundary:** native batches remain columnar for as long as practical. Converting an entire result into Elixir row maps is optional and explicit, never implicit.
8. **Safe pushdown only:** metadata statistics can skip reads only when they prove a row group cannot match. Missing or inconclusive statistics fall back to row-level evaluation.
9. **Stable public errors:** Rust panics and backend-specific details are translated into contextual, redacted Elixir errors.
10. **Future compatibility without future scope:** storage, batch, schema, and expression boundaries should permit a later DataFusion adapter, but no DataFusion abstraction is implemented now.

## 5. Required Memory and Lifecycle Invariants

- A read must not download or decode an entire object merely because it is remote.
- Peak read buffering is a documented function of batch size, prefetch depth, concurrent range requests, and bounded metadata overhead—not total object size.
- Peak write buffering is a documented function of input batch size, row-group/page settings, multipart part size, and bounded in-flight parts—not total output size.
- Storage range requests and multipart uploads use bounded concurrency.
- An Elixir consumer that halts, exits, or times out causes native cancellation and prompt cleanup.
- No long-running I/O or decoding work occupies a normal BEAM scheduler.
- Failed local writes remove temporary files; failed S3 writes abort multipart uploads where applicable.
- Tests include data materially larger than configured buffers and assert that memory remains within a documented tolerance.

## 6. Quality Policy and Epic Gate

`bin/qa_check.sh` is the single project quality entry point. It must eventually cover:

- Elixir formatting, compilation with warnings treated as errors, static analysis where configured, and ExUnit tests.
- Rust formatting, `cargo check`, Clippy with warnings denied, and Rust tests.
- Documentation checks and focused local/S3 integration tests.
- Streaming cancellation, resource cleanup, and bounded-memory regression tests.

An epic is complete only when all of its phases, deliverables, and acceptance criteria are complete. At the end of every epic:

1. Run `bin/qa_check.sh` from the repository root.
2. Fix every failure; do not waive or skip checks merely to close the epic.
3. Confirm the epic's acceptance criteria.
4. Review `git diff` for generated files, secrets, credentials, and unrelated changes.
5. Commit only when the QA check is green and the epic is complete, using the suggested commit message or an equally clear equivalent.

Epics are ordered. Work may be explored ahead of time, but each epic gate should produce a clearly reviewable commit before the next epic is considered complete.

---

## Epic 1 — Native Foundation and Reproducible Quality Gate

**Objective:** Establish the Elixir/Rust boundary, project structure, and the quality process that every later epic must pass.

### Phases

1. **Define public boundaries:** replace the generated hello-world concept with initial `Parquex`, location, schema, batch, stream, options, and error module boundaries; document that streaming is the primary API.
2. **Bootstrap Rustler:** create the native Rust crate, pin toolchain-compatible dependencies, load a minimal NIF, and add a boundary smoke test.
3. **Establish native runtime rules:** define resource ownership, panic containment, cancellation tokens, async/dirty scheduling policy, and error translation before adding storage work.
4. **Create the QA command:** add executable `bin/qa_check.sh` with deterministic Elixir and Rust format, compile/check, lint, and test stages.
5. **Add test foundations:** create fixture helpers, temporary-directory isolation, resource-leak test helpers, and tags for tests that require the RustFS S3 integration environment.
6. **Document architecture decisions:** record concise ADRs for pull-based streaming, backend-neutral storage, columnar batches, immutable outputs, and deferred SQL/table formats.
7. **Pass the epic gate:** run `bin/qa_check.sh`, verify all Epic 1 acceptance criteria, and commit with `chore: establish native foundation and quality gate`.

### Deliverables

- Loadable Rustler NIF and native crate structure.
- One authoritative `bin/qa_check.sh` command.
- Documented module boundaries, lifecycle rules, and non-goals.

### Acceptance Criteria

- A test crosses the Elixir/Rust boundary successfully.
- A translated native error reaches Elixir without a panic or VM crash.
- The QA script passes from a clean checkout.
- No object-store, Parquet, DataFusion, Delta, Iceberg, or Hive behavior is prematurely introduced.

---

## Epic 2 — Unified Object Access and Local Storage

**Objective:** Build only the object operations Parquet needs, with a complete local implementation and an interface suitable for S3.

### Phases

1. **Define location descriptors:** support validated local paths, `file://` URIs, and reserved `s3://` descriptors with per-location options and redacted inspection.
2. **Define the Rust object contract:** model metadata/head, bounded range reads, listing by explicit prefix, streaming/staged write, publish, delete, and cancellation without leaking backend types into Parquet code.
3. **Implement local reads:** add metadata and strict bounded range reads, including empty objects, end-of-file behavior, canonicalization, and configurable allowed-root policy.
4. **Implement local writes:** stream into uniquely named temporary files, support explicit flush/sync policy, atomically publish without replacing an existing destination, and clean up on error or cancellation.
5. **Expose a consistent Elixir API:** normalize one or many locations while preserving caller order and without selecting a global default backend.
6. **Harden and test:** cover Unicode paths, invalid ranges, missing files, permissions, symlinks/root escape, interrupted writes, deterministic listing, cleanup, and stable error categories.
7. **Pass the epic gate:** run `bin/qa_check.sh`, verify all Epic 2 acceptance criteria, and commit with `feat: add unified object access and local storage`.

### Deliverables

- Backend-neutral object-access contract.
- Tested local range-read and safely published write implementation.
- Public location descriptors usable later by local and S3 operations.

### Acceptance Criteria

- Local object reads can retrieve a requested byte range without reading the complete file.
- Cancelled or failed writes do not publish partial destination files.
- Publication rejects an existing destination with a stable conflict and leaves its bytes unchanged.
- Location lists can contain independently configured descriptors and preserve their supplied order.
- Errors and inspected options never disclose credentials or other marked secrets.

---

## Epic 3 — Streaming Parquet Reads

**Objective:** Read local Parquet files lazily as bounded columnar batches through the object-access contract.

### Phases

1. **Integrate Arrow/Parquet crates:** pin compatible Rust Arrow and Parquet versions and implement footer/metadata reads through the range-read contract rather than direct filesystem coupling.
2. **Define schema compatibility:** document supported primitive, binary, temporal, decimal, list, struct, and null mappings, with explicit behavior for unsupported logical types.
3. **Create the native reader resource:** hold reader state, selected columns, current row group, batch limits, cancellation state, and bounded prefetch configuration.
4. **Expose pull-based Elixir streaming:** implement lazy batch enumeration so opening a stream does not read the complete file and downstream demand requests the next batch.
5. **Preserve columnar data:** expose a batch abstraction and explicit access/conversion functions; avoid automatically expanding every result into a complete list of Elixir maps.
6. **Verify lifecycle and memory:** test empty and multi-row-group files, nested/null data, projection, malformed files, early halt, consumer failure, repeated open/close, and large-file memory bounds.
7. **Pass the epic gate:** run `bin/qa_check.sh`, verify all Epic 3 acceptance criteria, and commit with `feat: add bounded streaming Parquet reads`.

### Deliverables

- Lazy local Parquet batch stream.
- Metadata/schema inspection and column projection.
- Initial Parquet-to-Elixir type compatibility table.

### Acceptance Criteria

- The first batch is available without reading or decoding the complete file.
- Projection avoids decoding unselected columns.
- Early stream termination promptly closes native and local resources.
- Peak buffering remains within the documented batch/prefetch envelope for a test file much larger than that envelope.
- Malformed data and unsupported schemas return structured errors rather than panics.

---

## Epic 4 — Streaming Parquet Writes and Compression

**Objective:** Write new immutable Parquet files from bounded input batches, with explicit and interoperable compression settings.

### Phases

1. **Define the write contract:** require an explicit or validated schema, accept an enumerable of bounded batches, and specify schema mismatch and nullability errors.
2. **Implement the native writer resource:** incrementally encode batches and close row groups according to configurable row-group/page limits without collecting the full input.
3. **Add local publication:** connect the Parquet writer to staged local object writes and publish only after successful footer completion and close, rejecting an existing destination.
4. **Add compression:** support `:uncompressed`, `:snappy`, `:zstd`, `:gzip`, and `:lz4_raw` where supported by the pinned Parquet crate; document a tested default and codec availability.
5. **Add write backpressure and cancellation:** bound producer/native buffering, stop upstream consumption on failure, and remove temporary output when halted or cancelled.
6. **Verify interoperability:** add large streaming round trips, multiple row groups, every advertised codec, schema failures, empty input, interrupted writes, and independent-reader compatibility tests.
7. **Pass the epic gate:** run `bin/qa_check.sh`, verify all Epic 4 acceptance criteria, and commit with `feat: add streaming Parquet writes and compression`.

### Deliverables

- Bounded-memory local Parquet writer.
- Compression options and compatibility documentation.
- Safe complete-or-absent publication behavior.

### Acceptance Criteria

- Input enumerables larger than available memory are not materialized by Parquex.
- Every advertised codec round-trips and is readable by an independent compatible Parquet reader.
- Output row-group and compression settings are observable in Parquet metadata.
- Failed or cancelled writes leave no published partial file.
- Existing completed files are rejected as destination conflicts and remain byte-for-byte unchanged.

---

## Epic 5 — S3-Compatible Object Storage

**Objective:** Apply the same Parquet streaming behavior to S3-compatible stores using bounded range reads and multipart writes.

### Phases

1. **Implement S3 configuration:** add endpoint, region, bucket/prefix, path-style, TLS, timeout, retry, credential-provider, and bounded concurrency options using Rust's `object_store` ecosystem.
2. **Implement remote object reads:** support head, bounded byte ranges, explicit-prefix listing, cancellation, and retry classification without whole-object downloads.
3. **Stream Parquet from S3:** fetch footer/metadata ranges and required column data through the common object contract while honoring batch, prefetch, timeout, and concurrency limits.
4. **Implement bounded multipart writes:** stream Parquet output using configurable part size and in-flight part count; complete only after the Parquet writer closes successfully and preserve create-only destination semantics.
5. **Handle failures safely:** abort multipart uploads on cancellation/failure where possible, classify retriable errors, redact credentials, and report cleanup failures without hiding the primary error.
6. **Build repeatable RustFS integration coverage:** add a root `docker-compose.yml` with a project-owned service using a verified pinned RustFS image, wait on readiness, and test ranges, compressed files, multipart writes, retries, cancellation, and missing/forbidden objects.
7. **Pass the epic gate:** run `bin/qa_check.sh`, verify all Epic 5 acceptance criteria, and commit with `feat: stream Parquet through S3-compatible storage`.

### Deliverables

- S3-compatible object backend.
- Remote streaming Parquet reads and writes.
- Reproducible RustFS integration tests and a minimal project-owned `docker-compose.yml`.

### Acceptance Criteria

- Reading Parquet from S3 uses range requests and does not first download the entire object.
- Multipart memory is bounded by configured part size and in-flight part count.
- Remote cancellation closes requests and aborts incomplete uploads where supported.
- Standard credentials and explicit credentials are supported without appearing in logs, errors, telemetry, or inspected structs.
- Existing S3 destinations are rejected or publication fails as unsupported when create-only completion cannot be guaranteed; completed objects are never silently replaced.
- The pinned RustFS service in `docker-compose.yml` starts repeatably, exposes a readiness check, uses test-only credentials, and is managed by `bin/qa_check.sh` without depending on a sibling checkout.
- The same public Parquet operations work for local and S3 locations.

---

## Epic 6 — Append-Oriented Access, Offset Filtering, and Backend Interoperability

**Objective:** Support the essential local-first access pattern without creating a table format or query engine.

### Phases

1. **Define immutable append conventions:** provide helpers/options for writing uniquely named new Parquet objects beneath an explicit location or prefix; never append bytes to a completed Parquet file.
2. **Add lightweight typed predicates:** support a deliberately small expression set needed for comparisons such as `offset > value`, separate from SQL and without adopting DataFusion.
3. **Implement safe row-group pruning:** use Parquet min/max/null statistics and column projection to skip proven nonmatching row groups; fall back to normal reads when statistics are missing or inconclusive.
4. **Apply row-level correctness filtering:** evaluate remaining predicates as a bounded batch operation so pruning is an optimization and never the source of correctness.
5. **Support mixed backend inputs:** allow one stream operation to consume a caller-ordered collection of local and S3 Parquet locations using isolated per-location configuration and bounded open-source concurrency.
6. **Prove rewrite compatibility:** add an example/test that streams selected immutable inputs into a new Parquet output, demonstrating an application can implement compaction without Parquex owning scheduling, manifests, or deletion policy.
7. **Pass the epic gate:** run `bin/qa_check.sh`, verify all Epic 6 acceptance criteria, and commit with `feat: support append-oriented offset scans across stores`.

### Deliverables

- Immutable append-output convention.
- Projection plus minimal typed filtering and safe row-group pruning.
- Caller-ordered mixed local/S3 streaming.
- A bounded streaming rewrite example, not a compaction subsystem.

### Acceptance Criteria

- `offset > value` returns correct rows with usable, missing, and inconclusive statistics.
- Statistics can reduce row-group/range reads but cannot cause matching rows to be omitted.
- Mixed local/S3 input uses one batch representation and one stream contract.
- Mixed-input peak memory is bounded by configured batch, prefetch, and source-concurrency limits rather than total input size.
- The rewrite example never materializes all source rows and does not delete its inputs.
- No SQL, catalog, transaction log, partition discovery, or background compaction behavior is introduced.

---

## Epic 7 — Hardening, Observability, Documentation, and Initial Release

**Objective:** Make the focused local/S3 Parquet API reliable, measurable, understandable, and ready for an initial release.

### Phases

1. **Stabilize the public contract:** review names, defaults, schemas, batch representation, option validation, error categories, ordering, cancellation, and versioning expectations.
2. **Add safe observability:** emit telemetry for bytes/ranges, rows/batches, row groups read/skipped, retries, durations, cancellations, and current/peak buffering without row contents or secrets.
3. **Exercise fault paths:** inject malformed Parquet data, timeouts, connection resets, throttling, credential failures, multipart failures, consumer exits, and native worker failures.
4. **Enforce memory and scheduler budgets:** add repeatable tests with oversized local and S3 datasets, documented tolerances, bounded concurrency assertions, and BEAM responsiveness checks.
5. **Complete user documentation:** document local and S3 setup, reads, writes, compression, projection, offset filtering, mixed sources, cancellation, tuning, limitations, and troubleshooting.
6. **Prepare release artifacts:** finalize package metadata, generated docs, supported Elixir/OTP/Rust matrix, native build guidance, changelog, security notes, and deferred-feature statement.
7. **Pass the final epic gate:** run `bin/qa_check.sh` from a clean checkout, verify all Epic 7 acceptance criteria, and commit with `docs: harden Parquex for initial release`.

### Deliverables

- Stable and documented initial API.
- Fault, cancellation, memory, and scheduler regression coverage.
- Telemetry contract and release-ready package metadata.

### Acceptance Criteria

- All documented examples execute in automated tests.
- Local and S3 tests demonstrate the stated memory invariants with inputs much larger than configured buffers.
- Fault tests leave no leaked readers, file handles, worker tasks, temporary files, HTTP requests, or multipart uploads under Parquex's control.
- Telemetry, errors, and logs contain neither credentials nor row contents.
- Package compilation and documentation generation pass from a clean checkout.
- Documentation clearly states that DataFusion/SQL, table formats, storage blending policy, and compaction orchestration remain outside the initial release.

---

## 7. Epic Dependency Order

```text
Epic 1: foundation and QA
   ↓
Epic 2: object contract + local storage
   ↓
Epic 3: streaming Parquet reads
   ↓
Epic 4: streaming Parquet writes + compression
   ↓
Epic 5: S3-compatible storage
   ↓
Epic 6: offset-oriented access + local/S3 interoperability
   ↓
Epic 7: hardening and release
```

## 8. Initial Technical Direction

The exact versions must be pinned during implementation and validated together. The intended Rust stack is:

- `rustler` for the Elixir/native boundary.
- `arrow` and `parquet` for columnar representation and Parquet encoding/decoding.
- `object_store` for local/S3-compatible object access where its interfaces satisfy the required streaming behavior.
- `tokio` and `futures` for managed asynchronous I/O, bounded channels, and cancellation.
- RustFS as the canonical S3-compatible integration target. A minimal project-owned `docker-compose.yml` is the reproducible default; `../RUSTFS_TOOL/bin` may remain an optional developer convenience but is not a repository or QA dependency.

DataFusion should be evaluated only after this roadmap is complete. The initial storage, schema, batch, and expression contracts should remain narrow enough that a later SQL layer can consume them without making Parquex itself a data platform.

## 9. Definition of Initial Success

Parquex is initially successful when an Elixir application can use one consistent API to:

- lazily read compressed Parquet from local disk or S3-compatible storage;
- process results as bounded columnar batches;
- select columns and retrieve changes after an offset without a SQL engine;
- stream batches into a newly published compressed Parquet file/object;
- alternate or combine local and remote locations without a global backend assumption; and
- cancel any operation without hidden full materialization or leaked native/storage resources.
