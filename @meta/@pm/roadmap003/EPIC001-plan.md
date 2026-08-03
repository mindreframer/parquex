# EPIC001 Plan: One Store-and-Key API

## Progress

- [x] Phase 1.1: Reduce the Elixir public surface.
- [x] Phase 1.2: Reduce Reader, Writer and native entry points.
- [x] Phase 1.3: Convert focused tests to stores and keys.
- [x] Phase 1.4: Pass QA, review and commit.

## Implementation Steps

1. Delete redundant modules and remove their public function heads.
2. Simplify aliases, types, structs and metadata conversion around store/key destinations.
3. Delete unused local/S3 object and Parquet NIF entry points and resource wrappers.
4. Synchronize `Parquex.Native` and the raw release smoke function list.
5. Convert retained fixture, scheduler and telemetry tests to local stores.
6. Delete tests that exist only for removed modules.
7. Run the full QA gate and inspect the exported/module surface.

## Quality Gate

- [x] Only the Store/key addressing model remains public.
- [x] Reader and Writer have no alternate backend paths.
- [x] Native exports and smoke stubs agree.
- [x] Removed tests are replaced where they covered retained behavior.
- [x] `bin/qa_check.sh` passes.
- [x] Diff contains no user-owned unrelated files.

## Commit Rule

Commit as `roadmap003 - epic 1 - use one store and key API` with an informative body after all checks pass.
