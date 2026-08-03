# EPIC005 Plan: Hardening and 0.3.0 Release

## Progress

- [x] Phase 5.1: Add final replacement, lifecycle and memory hardening.
- [x] Phase 5.2: Synchronize version and release metadata at `0.3.0`.
- [x] Phase 5.3: Pass final local QA/docs/package gates and commit.
- [x] Phase 5.4: Push and publish all seven precompiled NIFs.
- [x] Phase 5.5: Generate/commit checksums and pass clean-consumer CI.
- [x] Phase 5.6: Mark ROADMAP003 complete and pass final CI.

## Implementation Steps

1. Add repeated replacement and resource/RSS plateau coverage.
2. Re-run cancellation, owner-exit, dataset disorder and range-stream scenarios.
3. Set Mix/Cargo/changelog/artifact expectations to `0.3.0` and remove the old checksum manifest.
4. Run full QA, ExDoc, package and secret scans; commit only on green.
5. Push main and dispatch the seven-target release with publication enabled.
6. Cancel a doomed run immediately, fix failures, retest and redispatch until green.
7. Verify exactly seven assets and GitHub SHA-256 digests.
8. Generate, audit, commit and push the RustlerPrecompiled checksum manifest.
9. Monitor logic QA and every clean no-Rust consumer until green.
10. Mark the roadmap completed, commit/push and verify final CI, release, git and RustFS state.

## Quality Gate

- [x] Replacement and lifecycle stress tests pass.
- [x] Native resources return to baseline and RSS plateaus.
- [x] Mix, Cargo and release metadata agree on `0.3.0`.
- [x] Local QA, ExDoc and package audit pass.
- [x] Release workflow succeeds for all seven targets.
- [x] `v0.3.0` has exactly seven assets and matching checksums.
- [x] Every clean consumer and RustFS logic job is green.
- [x] ROADMAP003 is completed only after all preceding checks.

## Commit Rule

Use focused release-boundary commits:

1. `roadmap003 - epic 5 - harden and prepare 0.3.0`
2. `roadmap003 - epic 5 - add 0.3.0 precompiled checksums`
3. `roadmap003 - epic 5 - complete the 0.3.0 roadmap`

Every commit requires an informative body and the relevant green local or remote gate.
