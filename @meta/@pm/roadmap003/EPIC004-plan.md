# EPIC004 Plan: Plain-Language Product and Package

## Progress

- [x] Phase 4.1: Rewrite README around first use.
- [x] Phase 4.2: Replace the guide set with current focused documents.
- [x] Phase 4.3: Simplify module docs, changelog and package metadata.
- [x] Phase 4.4: Verify docs and unpacked package.
- [x] Phase 4.5: Pass QA, review and commit.

## Implementation Steps

1. Rewrite README from a blank current-product outline.
2. Consolidate object and Parquet material into focused guides.
3. Delete migration, compatibility, append and conditional-publication documents.
4. Rewrite architecture index/notes that refer to removed design decisions.
5. Rewrite module docs and package description in plain language.
6. Reduce changelog to the current product release entry.
7. Update ExDoc extras and package file selection.
8. Run forbidden-term searches, doctests, ExDoc and unpacked-package audit.
9. Run the full QA gate.

## Quality Gate

- [x] README assumes no context and leads with runnable examples.
- [x] Published docs contain no historical/migration narrative.
- [x] Removed APIs and publication semantics are absent.
- [x] `mix docs --warnings-as-errors` passes.
- [x] Unpacked package contents are focused and complete.
- [x] `bin/qa_check.sh` passes.

## Commit Rule

Commit as `roadmap003 - epic 4 - explain the simple product` with an informative body after all checks pass.
