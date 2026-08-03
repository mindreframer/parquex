# EPIC001 Plan: One Store-and-Key API

## Progress

- [ ] Phase 1.1: Reduce the Elixir public surface.
- [ ] Phase 1.2: Reduce Reader, Writer and native entry points.
- [ ] Phase 1.3: Convert focused tests to stores and keys.
- [ ] Phase 1.4: Pass QA, review and commit.

## Implementation Steps

1. Delete redundant modules and remove their public function heads.
2. Simplify aliases, types, structs and metadata conversion around store/key destinations.
3. Delete unused local/S3 object and Parquet NIF entry points and resource wrappers.
4. Synchronize `Parquex.Native` and the raw release smoke function list.
5. Convert retained fixture, scheduler and telemetry tests to local stores.
6. Delete tests that exist only for removed modules.
7. Run the full QA gate and inspect the exported/module surface.

## Quality Gate

- [ ] Only the Store/key addressing model remains public.
- [ ] Reader and Writer have no alternate backend paths.
- [ ] Native exports and smoke stubs agree.
- [ ] Removed tests are replaced where they covered retained behavior.
- [ ] `bin/qa_check.sh` passes.
- [ ] Diff contains no user-owned unrelated files.

## Commit Rule

Commit as `roadmap003 - epic 1 - use one store and key API` with an informative body after all checks pass.
