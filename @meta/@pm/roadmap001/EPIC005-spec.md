# EPIC005 Spec: S3-Compatible Object Storage

## Purpose

Apply the same bounded, cancellable Parquet read and write contracts to S3-compatible storage through the backend-neutral object interface.

## Reference Inputs

- Roadmap: `@meta/@pm/ROADMAP001.md`
- EPIC002 location and backend-neutral object contracts
- EPIC003 bounded streaming Parquet reads
- EPIC004 bounded streaming writes, compression, and immutable publication
- Required memory and lifecycle invariants in ROADMAP001
- Existing optional native developer tool: `../RUSTFS_TOOL/bin/restart_rustfs.sh` and `../RUSTFS_TOOL/bin/stop_rustfs.sh`
- Official RustFS container, configuration, and readiness conventions

## Scope

In scope:

- S3 endpoint, region, bucket/prefix, path-style, TLS, timeout, retry, credential-provider, and bounded-concurrency options
- standard credential providers and explicit credentials with mandatory redaction
- head, bounded byte ranges, explicit-prefix listing, cancellation, and retry classification
- Parquet footer, metadata, and selected column reads over bounded ranges
- configurable multipart part size and bounded in-flight uploads
- multipart completion only after successful Parquet writer close
- failure/cancellation cleanup and multipart abort where supported
- create-only destination publication or an explicit unsupported result when the backend cannot guarantee it
- root `docker-compose.yml` with a project-owned minimal RustFS service, verified pinned image, readiness check, test-only credentials, and isolated storage
- RustFS lifecycle, range, compression, multipart, retry, cancellation, redaction, and cleanup integration coverage

Out of scope:

- application-level local/remote routing, cache, or synchronization
- implicit bucket-wide listing or partition discovery
- whole-object download before Parquet decoding
- unbounded range concurrency or multipart buffering
- exactly-once publication, distributed transactions, SQL, or table formats
- production RustFS deployment, RustFS administration, or a dependency on the sibling `RUSTFS_TOOL` directory

## S3 Streaming Contract

Each S3 location carries independent validated connection, credential, timeout, retry, and concurrency configuration; no process-global backend selection is required. The same public metadata, read, and write operations and batch representation used for local locations apply to S3 locations. Inspected descriptors, errors, logs, and telemetry redact explicit and provider-derived credentials.

Reads fetch Parquet footer, metadata, and required column data through bounded range requests. Batch size, prefetch depth, request range size, request concurrency, timeout, and retries remain bounded; no read first downloads the complete object. Peak read buffering is a documented function of those limits plus bounded metadata overhead, not object size.

Writes incrementally feed the Parquet encoder into multipart upload parts. Peak multipart buffering is bounded by configured part size and in-flight part count plus bounded encoder/input buffers, not output size. Completion starts only after successful footer completion and writer close. Publication is create-only: an existing object returns a stable conflict and remains unchanged; if a configured S3-compatible backend cannot guarantee create-only completion, Parquex returns an explicit unsupported error rather than silently allowing replacement. Failure, cancellation, timeout, or owner exit closes requests and aborts incomplete multipart uploads where supported; cleanup failure is reported without replacing the primary error.

Retries apply only to classified retriable operations and remain bounded. Publication does not claim exactly-once semantics: ambiguous completion is returned contextually for caller reconciliation rather than hidden. Compression bytes and metadata produced by EPIC004 are preserved unchanged by S3 transport.

The canonical integration environment is the minimal RustFS service in the root `docker-compose.yml`, based on the official image pinned to a verified version or digest, never an unpinned `latest` tag. It binds test ports to localhost, uses documented test-only credentials, stores data in an isolated disposable volume, and exposes `/health/ready`. `bin/qa_check.sh` starts it, waits with a bounded deadline, runs isolated tests, captures diagnostics on failure, and tears it down with volumes through a trap. The native `../RUSTFS_TOOL` scripts may target the same test contract for manual development, but the project, CI, and QA gate do not depend on that sibling directory.

## Acceptance Criteria

- Valid S3 options support endpoint, region, bucket/prefix, path-style, TLS, timeout, retry, credential provider, and bounded concurrency configuration.
- Standard and explicit credentials never appear in inspected structs, errors, logs, or telemetry.
- Head, bounded ranges, and explicit-prefix listing work without whole-object downloads.
- Reading Parquet from S3 uses bounded ranges and preserves projection, batch, prefetch, cancellation, and compression behavior.
- Multipart writes bound memory by part size and in-flight part count plus documented encoder/input overhead.
- Multipart completion occurs only after the Parquet writer closes successfully.
- Existing destinations return a stable conflict and remain unchanged; unsupported create-only backends fail explicitly before Parquex can claim immutable publication.
- Cancellation and failure close active requests and abort incomplete multipart uploads where supported.
- Retriable and terminal failures are distinguishable, retries are bounded, and cleanup failures do not hide primary failures.
- The root `docker-compose.yml` starts a pinned RustFS service, reaches readiness within a bounded deadline, and tears down containers and volumes repeatably.
- `bin/qa_check.sh` owns RustFS startup, diagnostics, test execution, and cleanup without requiring `../RUSTFS_TOOL`.
- The same public Parquet operations and batch representation work for local and S3 locations.

## Test Strategy

- Run reproducible integration tests against the project-owned RustFS service in `docker-compose.yml`; optionally allow the native `../RUSTFS_TOOL` endpoint for manual focused runs.
- Instrument client requests to assert head/range behavior, explicit prefixes, bounded concurrency, retries, and absence of whole-object GETs.
- Stream compressed multi-row-group objects larger than read buffers and verify projection and memory bounds.
- Stream multipart outputs larger than part buffers; inspect parts/concurrency and round-trip every representative compressed object.
- Inject missing/forbidden responses, timeouts, throttling, transient failures, cancellation, writer failure, destination collision, completion failure, and abort failure.
- Scan inspected values and captured errors/logs/telemetry for credential material.
- Exercise equivalent local and S3 calls against the same deterministic schema and batches.
- Verify compose readiness deadlines, failure diagnostics, idempotent startup/teardown, and volume removal.

## Quality Bar

- No remote Parquet read requires a complete-object download.
- Range requests, retries, multipart parts, and all read/write concurrency are explicit and bounded.
- Remote cancellation promptly releases HTTP/native resources and attempts multipart abort.
- Credentials are redacted on every observable path.
- RustFS tests are deterministic, isolated, project-owned, and leave no objects, multipart uploads, containers, or test volumes under Parquex's control.
- The Docker image is pinned, test credentials are clearly non-production, and the integration suite does not depend on machine-specific absolute paths.
- `bin/qa_check.sh` must pass before the epic commit.
