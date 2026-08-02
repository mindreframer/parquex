# EPIC007 Plan: Hardening, Observability, Documentation, and Initial Release

## Progress

- [x] Phase 7.1: Stabilize the public contract.
- [x] Phase 7.2: Add safe observability.
- [x] Phase 7.3: Exercise fault paths.
- [x] Phase 7.4: Enforce memory and scheduler budgets.
- [x] Phase 7.5: Complete user documentation.
- [x] Phase 7.6: Prepare release artifacts.
- [x] Phase 7.7: Pass the final epic gate.

## Implementation Steps

1. Review and freeze public names, defaults, schema/batch representation, option validation, errors, source ordering, cancellation, and initial versioning expectations without expanding scope.
2. Add documented, bounded-cardinality telemetry for bytes/ranges, rows/batches, row groups, retries, durations, cancellations, and buffering with strict secret/row-content redaction.
3. Inject malformed Parquet, timeout, reset, throttle, credential, multipart, consumer-exit, and native-worker failures; assert stable errors and complete cleanup.
4. Add repeatable oversized local/S3 tests that enforce documented memory and concurrency tolerances and verify long native work does not degrade BEAM responsiveness.
5. Complete executable documentation for setup, the pinned RustFS `docker-compose.yml` workflow, optional `../RUSTFS_TOOL/bin` manual usage, reads/writes, compression, projection, offset filtering, mixed sources, cancellation, tuning, limitations, and troubleshooting.
6. Finalize package metadata and contents, generated docs, supported Elixir/OTP/Rust matrix, native build guidance, changelog, security notes, and deferred-feature statement.
7. Run and fix `bin/qa_check.sh` from a clean checkout, confirm every acceptance criterion, review the final focused release diff, and only then create the initial-release commit.

## Test Isolation Checklist

- [x] Every local and S3 scenario uses unique temporary paths/prefixes and deterministic fixtures.
- [x] Fault injection uses controlled hooks or RustFS/client responses, never external network instability or arbitrary sleeps.
- [x] Memory tests use explicit configuration, warm-up policy, measurement boundaries, and documented tolerances.
- [x] Scheduler tests use bounded deadlines and workload-independent responsiveness assertions.
- [x] Telemetry handlers are detached after each test and captured events contain no secrets or row data.
- [x] Every test reaps workers and verifies cleanup of files, requests, resources, and multipart uploads.
- [x] Clean-checkout/package checks do not depend on untracked files, cached build artifacts, the sibling `RUSTFS_TOOL`, or live services outside the QA-managed `docker-compose.yml` environment.

## Quality Gate

- [x] Public contract and telemetry reviews have no unresolved release blockers.
- [x] All documented examples and fault-injection scenarios pass deterministically.
- [x] Local/S3 memory, bounded-concurrency, cancellation, cleanup, and scheduler responsiveness tests pass.
- [x] Telemetry, errors, logs, fixtures, and package contents disclose no credentials or row contents.
- [x] Documentation and release notes accurately state supported behavior and deferred scope.
- [x] The pinned RustFS `docker-compose.yml` workflow, readiness diagnostics, repeat-run reuse, and explicit teardown command pass from a clean checkout.
- [x] Package compilation, package-content checks, and documentation generation pass from a clean checkout.
- [x] `bin/qa_check.sh` succeeds from the final clean checkout and the diff contains only EPIC007 work.
- [x] Commit title and informative body follow the commit rule.

## Commit Rule

After implementation, run only:

```sh
bin/qa_check.sh
```

Only if it succeeds, create one focused commit. Use the title format `roadmap001 - epic N - xxx`, replacing `N` with `7` and `xxx` with a concise hardening/release description. The commit body must informatively summarize contract stabilization, observability, fault and budget coverage, documentation/release artifacts, and tests executed by `bin/qa_check.sh`. Do not commit failing or partial work.
