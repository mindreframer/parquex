# EPIC003 Plan: Ergonomic Single-Object Parquet

## Progress

- [ ] Phase 3.1: Add store/key Parquet operations.
- [ ] Phase 3.2: Add finite row/column convenience conversion.
- [ ] Phase 3.3: Add deterministic schema inference.
- [ ] Phase 3.4: Preserve bounded explicit streaming.
- [ ] Phase 3.5: Validate real-schema local/RustFS round-trips.
- [ ] Phase 3.6: Pass the epic gate and commit.

## Implementation Steps

1. Adapt schema, scan and writer creation to reusable stores and keys.
2. Define row-map/column-map normalization and deterministic finite schema inference.
3. Add explicitly materializing read/write convenience helpers.
4. Retain explicit-schema batch streaming, codecs, projection and small typed predicates.
5. Test the real event shape with Zstandard locally and through RustFS.
6. Add repeated lifecycle, early halt, malformed data and producer failure coverage.
7. Run `bin/qa_check.sh`, review the diff and commit.

## Quality Gate

- [ ] Real event-shaped rows round-trip with Zstandard.
- [ ] Inference rejects ambiguous/incompatible data deterministically.
- [ ] Explicit schemas support empty and nullable datasets.
- [ ] Greedy helpers are clearly documented.
- [ ] Native resources return to baseline after repetition and halt.
- [ ] `bin/qa_check.sh` passes.

## Commit Rule

Commit only after the gate passes with title `roadmap002 - epic 3 - simplify Parquet object IO` and an informative body.
