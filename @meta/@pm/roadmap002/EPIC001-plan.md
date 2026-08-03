# EPIC001 Plan: Corrected Public Contract

## Progress

- [x] Phase 1.1: Inventory and classify the `0.1.x` public surface.
- [x] Phase 1.2: Add Store and Dataset descriptors and validation.
- [x] Phase 1.3: Define corrected object and Parquet signatures.
- [x] Phase 1.4: Document compatibility, ownership and memory behavior.
- [x] Phase 1.5: Add executable contract tests.
- [x] Phase 1.6: Pass the epic gate and commit.

## Implementation Steps

1. Record which existing modules remain primary, advanced, compatibility-only or internal.
2. Implement pure `Store`/`Dataset` types only where behavior can be supported honestly at this epic.
3. Normalize relative keys/prefixes, schema and time-partition options with stable errors.
4. Add top-level/store-key function heads or documented staged signatures without breaking existing callers.
5. Rewrite module and README examples around the corrected model, clearly marking later-epic behavior.
6. Test constructors, inspection, validation, types and compatibility.
7. Run `bin/qa_check.sh`, review the diff and commit.

## Quality Gate

- [x] Corrected examples avoid direct Location construction.
- [x] Finite and streaming behavior is unambiguous.
- [x] Inspection is redacted and keys/prefixes normalize safely.
- [x] Existing behavior remains green.
- [x] `bin/qa_check.sh` passes.
- [x] Diff contains only EPIC001 work plus plan completion updates.

## Commit Rule

Commit only after the gate passes with title `roadmap002 - epic 1 - correct the public contract` and an informative body.
