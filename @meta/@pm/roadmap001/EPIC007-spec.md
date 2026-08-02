# EPIC007 Spec: Hardening, Observability, Documentation, and Initial Release

## Purpose

Harden the focused local/S3 streaming Parquet API into a stable, measurable, documented initial release with repeatable fault, memory, scheduler, packaging, and clean-checkout verification.

## Reference Inputs

- Roadmap: `@meta/@pm/ROADMAP001.md`
- EPIC001 through EPIC006 public contracts, implementations, tests, and architecture decisions
- Hex packaging, ExDoc, Rustler/native-build, and supported toolchain requirements

## Scope

In scope:

- final review of public names, defaults, schemas, batches, options, errors, ordering, cancellation, and versioning expectations
- telemetry for storage, Parquet, retry, duration, cancellation, and buffering measurements
- deterministic injection and cleanup verification for native, local, and S3 fault paths
- repeatable read/write memory, concurrency, and BEAM responsiveness regression tests
- complete local/S3 usage, RustFS `docker-compose.yml` testing, tuning, limitation, and troubleshooting documentation
- package metadata, generated documentation, compatibility matrix, native build guidance, changelog, and security notes
- clean-checkout package, documentation, and QA verification

Out of scope:

- new query, analytics, or data-frame APIs
- DataFusion or SQL integration
- Delta Lake, Iceberg, catalogs, transaction logs, or other table formats
- application-level storage blending, routing, caching, or synchronization policy
- compaction services, scheduling, orchestration, manifests, or input deletion
- guarantees beyond the established local/S3 object and immutable Parquet contracts

## Release and Observability Contract

The initial public contract has validated options, stable structured error categories, documented source ordering, one columnar batch representation, bounded pull-based reads and writes, and prompt cancellation/cleanup. Hardening may clarify or normalize that contract before release but does not broaden roadmap scope.

Telemetry reports operation identity, backend kind, bytes/ranges, rows/batches, row groups read/skipped, retries, durations, cancellation, and current/peak buffering at documented event boundaries. Events and metadata never include row contents, credentials, authorization material, signed URLs, or unredacted sensitive options; handlers must not control operation correctness.

Published memory and scheduler budgets are expressed as functions of configured batch, prefetch, range, source-concurrency, row-group/page, multipart-part, and in-flight limits, not total object size. `bin/qa_check.sh` is the sole release quality gate and validates the candidate from a clean checkout, including bounded startup, readiness, diagnostics, and teardown of the pinned project-owned RustFS service in `docker-compose.yml`.

## Acceptance Criteria

- Public API names, defaults, validation, result/error shapes, ordering, cancellation, and compatibility expectations are documented and tested.
- All documented examples execute in automated tests.
- Telemetry exposes the documented measurements while errors, logs, and events disclose neither credentials nor row contents.
- Injected malformed data, storage/network, multipart, consumer, and native-worker faults terminate predictably and leave no resources under Parquex's control.
- Oversized local and S3 fixtures satisfy documented memory/concurrency tolerances and preserve BEAM responsiveness.
- The documented project-owned RustFS `docker-compose.yml` workflow and `bin/qa_check.sh` integration run repeatably without the sibling `RUSTFS_TOOL` directory.
- Package compilation and documentation generation pass from a clean checkout on the supported matrix.
- Release documentation explicitly defers DataFusion/SQL, table formats, storage blending policy, and compaction orchestration.

## Test Strategy

- Execute documentation examples as doctests or deterministic integration tests with isolated local and project-owned RustFS storage.
- Inject malformed Parquet, timeout, reset, throttle, credential, multipart, consumer-exit, and worker-failure conditions at controlled boundaries.
- Measure current/peak buffering and responsiveness with datasets materially larger than configured limits; assert tolerances rather than machine-specific absolute timing.
- Capture telemetry to assert event shape, units, pairing, cancellation/failure metadata, cardinality, and redaction.
- Build docs and package, inspect package contents, and run `bin/qa_check.sh` with its managed RustFS lifecycle from a clean source checkout.

## Quality Bar

- No panic, indefinite wait, leaked reader/file/task/request/temporary file/multipart upload, or normal-scheduler blocking remains on tested public paths.
- Telemetry is bounded-cardinality, side-effect-free, accurately documented, and safe by default.
- Memory, scheduler, and RustFS lifecycle checks are deterministic enough for the normal quality gate.
- Documentation states capabilities and limits without promising a query engine, table format, storage policy, or compaction system.
- `bin/qa_check.sh` must pass before the release commit.
