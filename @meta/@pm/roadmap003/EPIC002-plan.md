# EPIC002 Plan: Direct Provider-Friendly Writes

## Progress

- [x] Phase 2.1: Replace remote staging/copy with direct multipart upload.
- [x] Phase 2.2: Implement normal local replacement.
- [x] Phase 2.3: Remove create-only configuration and errors.
- [x] Phase 2.4: Prove replacement, cancellation and cleanup.
- [x] Phase 2.5: Pass QA, review and commit.

## Implementation Steps

1. Open remote multipart uploads at the final destination path.
2. Complete and head that destination directly; abort only active uploads.
3. Remove staging sequence, stage deletion and publication-result machinery.
4. Change local publication to replace a completed destination.
5. Remove `create_only` option validation and conditional-copy configuration.
6. Rewrite object and Parquet conflict tests as replacement tests.
7. Run RustFS replacement and lifecycle scenarios.
8. Run a scoped Wasabi probe and delete every probe object.
9. Run the full QA gate and inspect the Rust diff for unused complexity.

## Quality Gate

- [x] Direct S3 writes use no copy or conditional operation.
- [x] Existing keys are replaced locally and remotely.
- [x] Cancellation and owner exit do not complete new objects.
- [x] Active multipart/resource counts return to baseline.
- [x] RustFS and Wasabi probes pass.
- [x] `bin/qa_check.sh` passes.

## Commit Rule

Commit as `roadmap003 - epic 2 - stream directly to object keys` with an informative body after all checks pass.
