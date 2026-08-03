# EPIC006 Plan: Bounded Time-Range Dataset Reader

## Progress

- [x] Phase 6.1: Implement partition planning and scoped discovery.
- [x] Phase 6.2: Implement bounded deterministic multi-file streaming.
- [x] Phase 6.3: Add exact timestamp and offset filtering.
- [x] Phase 6.4: Add projection, statistics and cancellation.
- [x] Phase 6.5: Prove partition pruning locally and on RustFS.
- [x] Phase 6.6: Pass the epic gate and commit.

## Implementation Steps

1. Validate `[from, until)` and enumerate canonical overlapping prefixes.
2. List only those prefixes, select Parquet parts and traverse deterministic key order.
3. Compose exact timestamp filtering with projection and optional simple offset comparison.
4. Stream one bounded batch at a time across files with prompt early-halt cancellation.
5. Add explicitly materializing `Dataset.read` over the same plan.
6. Expose bounded-cardinality counts for planned/opened/skipped partitions/files.
7. Instrument tests to prove unrelated objects are not opened and boundary rows are exact.
8. Run `bin/qa_check.sh`, review the diff and commit.

## Quality Gate

- [x] Unrelated partition files are not opened.
- [x] Boundary rows obey `[from, until)` exactly.
- [x] Empty/missing ranges are empty successes.
- [x] Projection/offset filtering remains correct.
- [x] Halt/failure releases listing/read resources.
- [x] Memory is bounded independently of selected dataset size.
- [x] `bin/qa_check.sh` passes.

## Commit Rule

Commit only after the gate passes with title `roadmap002 - epic 6 - stream dataset time ranges` and an informative body.
