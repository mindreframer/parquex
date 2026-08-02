# EPIC005 Plan: S3-Compatible Object Storage

## Progress

- [x] Phase 5.1: Implement validated, redacted, bounded S3 configuration.
- [x] Phase 5.2: Implement remote head, range, listing, cancellation, and retry behavior.
- [x] Phase 5.3: Stream Parquet reads through bounded S3 ranges.
- [x] Phase 5.4: Implement bounded create-only multipart Parquet writes and completion.
- [x] Phase 5.5: Abort failed work, classify retries/conflicts, and preserve primary errors.
- [x] Phase 5.6: Add root `docker-compose.yml` with pinned RustFS and isolated integration coverage.
- [x] Phase 5.7: Pass the epic gate and commit the completed S3 scope.

## Implementation Steps

1. Add per-location endpoint, region, bucket/prefix, path-style, TLS, timeout, retry, credential-provider, part/range, and bounded-concurrency validation with redacted inspection.
2. Implement head, strict byte ranges, explicit-prefix listing, cancellation, and bounded retry classification through the common object contract.
3. Route footer, metadata, and selected column reads through bounded S3 ranges while honoring batch, prefetch, timeout, and request-concurrency limits.
4. Feed incremental Parquet output into configurable multipart parts, cap in-flight uploads, complete only after footer/writer close, and require create-only completion or return unsupported.
5. Close active requests, abort incomplete uploads where supported, distinguish conflicts/retriable/terminal/ambiguous errors, preserve existing objects, and report cleanup failure without hiding the primary error.
6. Add root `docker-compose.yml` with a minimal service using an official RustFS image pinned by verified version/digest, localhost ports, test-only credentials, disposable storage, and `/health/ready`; integrate bounded startup, diagnostics, tests, and trap-based teardown into `bin/qa_check.sh`.
7. Document Compose and optional `../RUSTFS_TOOL/bin` usage, S3 configuration, credentials, retries, cancellation, memory envelopes, multipart/create-only limits, and backend parity; run and fix `bin/qa_check.sh`, confirm every Epic 5 criterion, review the final focused diff, and only then create the epic commit.

## Test Isolation Checklist

- [x] Every RustFS test uses a unique bucket or prefix and deterministic object keys.
- [x] RustFS endpoint and credentials come from documented test-only configuration and are always redacted.
- [x] Request assertions use client instrumentation rather than timing assumptions.
- [x] Retry and cancellation tests inject deterministic responses and synchronize at known boundaries.
- [x] Teardown verifies no test objects, incomplete multipart uploads, Compose containers, or disposable volumes remain under Parquex's control.
- [x] Compose startup/readiness uses bounded deadlines, emits logs on failure, and is idempotent.
- [x] Memory fixtures are materially larger than configured ranges, batches, parts, and in-flight buffers.

## Quality Gate

- [x] S3 configuration, validation, standard/explicit credential, and redaction tests pass.
- [x] Head, bounded range, explicit-prefix listing, retry, missing, and forbidden-object tests pass.
- [x] Compressed streaming read, projection, create-only multipart write, destination-conflict preservation, local/S3 parity, and memory-bound tests pass.
- [x] Cancellation, active-request cleanup, multipart abort, and cleanup-error reporting tests pass.
- [x] The pinned RustFS service in `docker-compose.yml` passes readiness, isolation, diagnostics, and teardown checks without depending on `../RUSTFS_TOOL`.
- [x] `bin/qa_check.sh` starts RustFS, runs the integration suite, cleans it up, and succeeds before the epic commit.
- [x] Git diff is focused on EPIC005 and contains no router, cache, filtering, SQL, or table-format behavior.
- [x] Commit title and informative body follow the commit rule.

## Commit Rule

After implementation, run only:

```sh
bin/qa_check.sh
```

Only if it succeeds, create one focused commit. Use the title format `roadmap001 - epic N - xxx`, replacing `N` with `5` and `xxx` with a concise S3-streaming description. The commit body must informatively summarize S3 configuration, bounded ranges/multipart behavior, cancellation and redaction, and tests executed by `bin/qa_check.sh`. Do not commit failing or partial work.
