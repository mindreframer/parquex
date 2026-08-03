# EPIC007 Plan: Hardening and 0.2.0 Release

## Progress

- [ ] Phase 7.1: Harden real-schema, memory and lifecycle behavior.
- [ ] Phase 7.2: Complete corrected API and migration documentation.
- [ ] Phase 7.3: Synchronize package/native version `0.2.0` and release metadata.
- [ ] Phase 7.4: Pass the final local QA gate and commit the epic.
- [ ] Phase 7.5: Publish and verify all seven precompiled NIFs.
- [ ] Phase 7.6: Generate/commit checksums and pass clean-consumer CI.
- [ ] Phase 7.7: Mark ROADMAP002 completed and pass final CI.

## Implementation Steps

1. Add repeated real-event reads/writes, live-resource baselines, RSS plateau checks and scheduler responsiveness coverage.
2. Harden cancellation, owner exit, multipart faults, late data, disorder and rotation.
3. Rewrite README/guides around Store, single-object Parquet, Dataset and UTC time ranges; add `0.1.x` migration guidance.
4. Update Elixir/Cargo versions, changelog, package metadata and artifact expectations to `0.2.0`.
5. Run `bin/qa_check.sh`, inspect package contents and commit EPIC007 only on green.
6. Push main, dispatch the precompiled release with publication enabled and monitor/fix every platform.
7. Verify exactly seven `v0.2.0` assets, generate the checksum manifest from published bytes, commit and push it.
8. Monitor logic QA and every no-Rust consumer; fix and repeat until green.
9. Mark ROADMAP002 completed at `0.2.0`, commit/push the marker and verify final CI plus clean git/RustFS state.

## Quality Gate

- [ ] Real event-shaped datasets pass every granularity and codec scenario.
- [ ] Live native resources return to baseline and repeated RSS plateaus.
- [ ] Docs/examples/package checks are accurate and green.
- [ ] Source/Cargo/changelog agree on `0.2.0`.
- [ ] `bin/qa_check.sh` passes before the epic commit.
- [ ] Release workflow succeeds for all seven targets.
- [ ] `v0.2.0` has exactly seven assets and checksums match.
- [ ] Every clean consumer and Docker/RustFS CI job is green.
- [ ] ROADMAP002 is marked completed only after all preceding checks.

## Commit Rule

Use focused commits as the release necessarily crosses publication boundaries:

1. `roadmap002 - epic 7 - harden and prepare 0.2.0`
2. `roadmap002 - epic 7 - add 0.2.0 precompiled checksums`
3. `roadmap002 - epic 7 - complete the 0.2.0 roadmap`

Every commit must have an informative body and may be created only after the relevant local or remote gate succeeds.
