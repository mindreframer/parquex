# EPIC005 Plan: Bounded Time-Partitioned Dataset Writer

## Progress

- [x] Phase 5.1: Implement finite dataset write orchestration.
- [x] Phase 5.2: Implement explicit dataset writer resources.
- [x] Phase 5.3: Add bounded partition routing and eviction.
- [x] Phase 5.4: Add file rotation, naming and reports.
- [x] Phase 5.5: Harden late/disordered/failure behavior.
- [x] Phase 5.6: Pass the epic gate and commit.

## Implementation Steps

1. Define Dataset writer options, defaults, validation and result/report structs.
2. Route timestamped rows into native partition-specific Parquet writers.
3. Enforce `max_open_partitions` with deterministic least-recently-used close/publish.
4. Rotate parts by row and bounded byte targets at batch boundaries.
5. Generate collision-resistant create-only keys beneath canonical prefixes.
6. Implement close, cancel, owner-exit and structured partial-failure cleanup.
7. Test every granularity, late/disordered input, rotation and local/RustFS parity.
8. Run `bin/qa_check.sh`, review the diff and commit.

## Quality Gate

- [x] Every granularity produces independently readable canonical parts.
- [x] Writer resources never exceed configured open partitions.
- [x] Rotation has no row loss/duplication.
- [x] Late data creates new immutable parts safely.
- [x] Failures clean active local/S3 staging and report published parts.
- [x] Memory is bounded independently of total input.
- [x] `bin/qa_check.sh` passes.

## Commit Rule

Commit only after the gate passes with title `roadmap002 - epic 5 - write partitioned datasets` and an informative body.
