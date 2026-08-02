# EPIC002 Plan: Unified Object Access and Local Storage

## Progress

- [x] Phase 2.1: Define validated local, `file://`, and reserved `s3://` location descriptors with redacted per-location options.
- [x] Phase 2.2: Define the backend-neutral Rust object contract for metadata, ranges, listing, staged writes, publication, deletion, and cancellation.
- [x] Phase 2.3: Implement local metadata and strict bounded range reads with canonicalization and allowed-root enforcement.
- [x] Phase 2.4: Implement unique staged local writes, flush/sync policy, create-only atomic publication, conflict handling, and cleanup.
- [x] Phase 2.5: Expose a consistent Elixir API for one or many caller-ordered locations without a global backend default.
- [x] Phase 2.6: Harden and test Unicode paths, ranges, failures, permissions, symlinks, listing, interruption, cleanup, and stable errors.
- [x] Phase 2.7: Pass the epic gate, verify Epic 2 acceptance criteria, and prepare the focused commit.

## Implementation Steps

1. Define location structs and validators for local paths, `file://` URIs, and non-operational reserved `s3://` descriptors with per-location redaction rules.
2. Introduce a backend-neutral Rust contract for metadata/head, bounded range reads, explicit-prefix listing, staged streaming writes, publish, delete, and cancellation.
3. Implement local metadata and exact bounded reads, including empty objects, EOF semantics, canonical paths, and configurable allowed roots.
4. Implement writes through unique temporary files with explicit flush/sync policy, atomic create-only publication, stable destination conflicts, existing-byte preservation, and deterministic cleanup on error or cancellation.
5. Expose Elixir normalization and object operations for one descriptor or an ordered descriptor collection without process-global backend state.
6. Add isolated tests for Unicode, invalid ranges, missing files, permissions, symlink/root escapes, deterministic listing, interrupted writes, cleanup, redaction, and stable errors.
7. Run and fix `bin/qa_check.sh`, confirm every Epic 2 criterion and scope boundary, review the final focused diff, and only then create the epic commit.

## Test Isolation Checklist

- [x] Every filesystem test owns a unique temporary root with deterministic cleanup.
- [x] Allowed-root tests configure only their isolated temporary root.
- [x] Cancellation and interrupted-write tests use explicit barriers or hooks, not sleeps.
- [x] Permission and symlink fixtures are restored or removed even when assertions fail.
- [x] Listing tests create controlled fixtures and assert deterministic order.
- [x] Tests require no live S3 service, network access, or credentials.
- [x] Cleanup assertions check both unpublished destinations and orphaned temporary files.

## Quality Gate

- [x] Location normalization, ordering, option isolation, and redacted inspection tests pass.
- [x] Local metadata and bounded-range tests prove complete files are not read unnecessarily.
- [x] Root-policy, symlink, Unicode, range, permission, and stable-error tests pass.
- [x] Staged-write publication, existing-destination conflict/preservation, cancellation, failure, and cleanup tests pass.
- [x] `bin/qa_check.sh` succeeds from the repository root before the epic commit.
- [x] Git diff contains no secrets, credentials, unrelated changes, S3 operations, or Parquet behavior.
- [x] Commit title and informative body follow the commit rule.

## Commit Rule

After implementation, run only the repository gate from the repository root:

```sh
bin/qa_check.sh
```

Only if it passes and all Epic 2 criteria are complete, create one focused commit. Use the title template `roadmap001 - epic N - xxx`, replacing `N` with `2` and `xxx` with a concise object-access and local-storage description. The commit body must informatively summarize the location and object contracts, local safety and publication decisions, and tests executed by `bin/qa_check.sh`. Do not commit failing, partial, or out-of-scope work.
