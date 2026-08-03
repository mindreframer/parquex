# EPIC003 Plan: Straightforward Parquet and Dataset Ergonomics

## Progress

- [x] Phase 3.1: Simplify file function heads and writer settings.
- [x] Phase 3.2: Simplify raw object writer lifecycle.
- [x] Phase 3.3: Align datasets with generated-key direct publication.
- [x] Phase 3.4: Add focused ergonomic and boundedness tests.
- [x] Phase 3.5: Pass QA, review and commit.

## Implementation Steps

1. Make inferred and explicit schema writes separate, unambiguous forms.
2. Remove public flush/sync options while retaining safe internal defaults.
3. Reduce `Store.put` and `Store.open_writer` to the required arguments.
4. Remove stale immutable/conflict wording from code-facing docs and errors.
5. Trim dataset writer option forwarding and clarify generated part keys.
6. Add concise file and dataset contract tests.
7. Exercise finite rows, bounded batches, projection, filters and time ranges.
8. Run the full QA gate and inspect public docs/types.

## Quality Gate

- [x] Common function forms are unique and documented.
- [x] Removed options fail clearly.
- [x] Dataset writes and reads remain bounded.
- [x] Generated dataset keys remain collision-resistant.
- [x] Scheduler and resource regression tests pass.
- [x] `bin/qa_check.sh` passes.

## Commit Rule

Commit as `roadmap003 - epic 3 - simplify Parquet ergonomics` with an informative body after all checks pass.
